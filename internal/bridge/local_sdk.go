package bridge

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	localSDKMagic        = "\x9e\xba\xac\xe9"
	localSDKHeaderSize   = 32
	localSDKTrailerSize  = 32
	localSDKPreview      = 0x2011
	localSDKPreviewResp  = 0x2012
	localSDKStreamSetup  = 0x3105
	localSDKStreamResp   = 0x3106
	localSDKIV           = "01234567\x00\x00\x00\x00\x00\x00\x00\x00"
	localSDKReceiverPort = 10101
	localSDKSetupTimeout = 10 * time.Second
)

type LocalDeviceSession struct {
	Serial        string
	Host          string
	CommandPort   int
	StreamPort    int
	CASIP         string
	CASPort       int
	OperationCode string
	Key           string
	IsEncrypt     bool
}

type localSDKSource struct {
	session   LocalDeviceSession
	channel   int
	quality   string
	command   net.Conn
	stream    net.Conn
	closeOnce sync.Once
}

func NewLocalSDKSource(session LocalDeviceSession, channel int, quality string) streamSource {
	return &localSDKSource{session: session, channel: channel, quality: quality}
}

func (s *localSDKSource) Start(ctx context.Context, _ string) error {
	if s.session.Host == "" || s.session.CommandPort <= 0 || s.session.StreamPort <= 0 {
		return fmt.Errorf("local source endpoint is incomplete")
	}
	if len(s.session.Key) == 0 || len(s.session.OperationCode) == 0 {
		return fmt.Errorf("local source CAS session is incomplete")
	}
	setupCtx, cancel := context.WithTimeout(ctx, localSDKSetupTimeout)
	defer cancel()
	dialer := &net.Dialer{Timeout: localSDKSetupTimeout}
	command, err := dialer.DialContext(setupCtx, "tcp", net.JoinHostPort(s.session.Host, strconv.Itoa(s.session.CommandPort)))
	if err != nil {
		return fmt.Errorf("local command connection: %w", err)
	}
	s.command = command
	preview, err := s.sendCommand(setupCtx, s.command, localSDKPreview, s.previewXML())
	if err != nil {
		s.Close()
		return err
	}
	if preview.command != localSDKPreviewResp {
		s.Close()
		return fmt.Errorf("local preview response command=0x%x", preview.command)
	}
	sessionID, result := localXMLFields(preview.body)
	if result != "" && result != "0" {
		s.Close()
		err := fmt.Errorf("local preview result=%s", result)
		if result == "1052677" {
			return fmt.Errorf("%w: %v", errLocalResourceUnavailable, err)
		}
		return err
	}
	if sessionID == "" {
		s.Close()
		return fmt.Errorf("local preview response has no session")
	}
	stream, err := dialer.DialContext(setupCtx, "tcp", net.JoinHostPort(s.session.Host, strconv.Itoa(s.session.StreamPort)))
	if err != nil {
		s.Close()
		return fmt.Errorf("local stream connection: %w", err)
	}
	s.stream = stream
	setup, err := s.sendCommand(setupCtx, s.stream, localSDKStreamSetup, localStreamSetupXML(sessionID))
	if err != nil {
		s.Close()
		return err
	}
	if setup.command != localSDKStreamResp {
		s.Close()
		return fmt.Errorf("local stream setup response command=0x%x", setup.command)
	}
	if _, result := localXMLFields(setup.body); result != "" && result != "0" {
		s.Close()
		return fmt.Errorf("local stream setup result=%s", result)
	}
	return nil
}

func (s *localSDKSource) Read(ctx context.Context) (SourceMessage, error) {
	if s.stream == nil {
		return SourceMessage{}, fmt.Errorf("local stream is not started")
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = s.stream.SetReadDeadline(deadline)
	} else {
		_ = s.stream.SetReadDeadline(time.Now().Add(localReadDeadline))
	}
	payload, err := readLocalRTPPayload(s.stream)
	if err != nil {
		if ctx.Err() != nil {
			return SourceMessage{}, ctx.Err()
		}
		return SourceMessage{}, fmt.Errorf("local stream read: %w", err)
	}
	return SourceMessage{Binary: true, Data: payload}, nil
}

func (s *localSDKSource) Close() {
	s.closeOnce.Do(func() {
		if s.command != nil {
			_ = s.command.Close()
		}
		if s.stream != nil {
			_ = s.stream.Close()
		}
	})
}

type localSDKFrame struct {
	command uint16
	body    []byte
}

func (s *localSDKSource) sendCommand(ctx context.Context, conn net.Conn, command uint16, body string) (localSDKFrame, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	}
	frame, err := buildLocalSDKFrame(command, []byte(body), s.session.Key)
	if err != nil {
		return localSDKFrame{}, err
	}
	if _, err := conn.Write(frame); err != nil {
		return localSDKFrame{}, fmt.Errorf("local command write: %w", err)
	}
	return readLocalSDKFrame(conn)
}

func buildLocalSDKFrame(command uint16, body []byte, keyString string) ([]byte, error) {
	key, err := decodeLocalAESKey(keyString)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	padded := pkcs7Pad(body, block.BlockSize())
	blockMode := cipher.NewCBCEncrypter(block, []byte(localSDKIV))
	blockMode.CryptBlocks(padded, padded)
	header := make([]byte, localSDKHeaderSize)
	copy(header[:4], []byte(localSDKMagic))
	binary.BigEndian.PutUint32(header[4:8], 0x01000000)
	binary.BigEndian.PutUint32(header[8:12], 0)
	binary.BigEndian.PutUint32(header[12:16], 0)
	binary.BigEndian.PutUint16(header[18:20], command)
	binary.BigEndian.PutUint32(header[20:24], 0xffffffff)
	binary.BigEndian.PutUint32(header[24:28], uint32(len(padded)))
	trailer := md5.Sum(padded)
	return append(append(header, padded...), []byte(hex.EncodeToString(trailer[:]))...), nil
}

func readLocalSDKFrame(reader io.Reader) (localSDKFrame, error) {
	header := make([]byte, localSDKHeaderSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return localSDKFrame{}, err
	}
	if string(header[:4]) != localSDKMagic {
		return localSDKFrame{}, fmt.Errorf("local SDK response magic invalid")
	}
	command := binary.BigEndian.Uint16(header[18:20])
	bodyLength := binary.BigEndian.Uint32(header[24:28])
	if bodyLength > 1<<20 {
		return localSDKFrame{}, fmt.Errorf("local SDK response body too large")
	}
	body := make([]byte, int(bodyLength))
	if _, err := io.ReadFull(reader, body); err != nil {
		return localSDKFrame{}, err
	}
	trailer := make([]byte, localSDKTrailerSize)
	if _, err := io.ReadFull(reader, trailer); err != nil {
		return localSDKFrame{}, err
	}
	return localSDKFrame{command: command, body: body}, nil
}

func (s *localSDKSource) previewXML() string {
	streamType := "MAIN"
	streamTypeID := 1
	if s.quality == "sd" {
		streamType = "SUB"
		streamTypeID = 2
	}
	isEncrypt := "FALSE"
	if s.session.IsEncrypt {
		isEncrypt = "TRUE"
	}
	return fmt.Sprintf("<?xml version=\"1.0\" encoding=\"utf-8\"?><Request><OperationCode>%s</OperationCode><Channel>%d</Channel><ReceiverInfo Address=\"\" Port=\"%d\" ServerType=\"1\" StreamType=\"%s\" NewStreamType=\"%d\" TransProto=\"TCP\" /><IsEncrypt>%s</IsEncrypt><ReceiverInfoEx SessionID=\"\" Port=\"%d\" /><Authentication Ticket=\"\" BizCode=\"biz=1\" Interval=\"180\" /></Request>", xmlEscape(s.session.OperationCode), s.channel, localSDKReceiverPort, streamType, streamTypeID, isEncrypt, localSDKReceiverPort)
}

func localStreamSetupXML(session string) string {
	return fmt.Sprintf("<?xml version=\"1.0\" encoding=\"utf-8\"?><Request><Session>%s</Session><Rate>1</Rate><Mode>-1</Mode></Request>", xmlEscape(session))
}

func localXMLFields(body []byte) (session, result string) {
	text := string(body)
	for _, tag := range []string{"Session", "Result"} {
		start := strings.Index(text, "<"+tag+">")
		if start < 0 {
			continue
		}
		start += len(tag) + 2
		end := strings.Index(text[start:], "</"+tag+">")
		if end < 0 {
			continue
		}
		value := text[start : start+end]
		if tag == "Session" {
			session = value
		} else {
			result = value
		}
	}
	return session, result
}

func xmlEscape(value string) string {
	value = strings.ReplaceAll(value, "&", "&amp;")
	value = strings.ReplaceAll(value, "<", "&lt;")
	value = strings.ReplaceAll(value, ">", "&gt;")
	value = strings.ReplaceAll(value, "\"", "&quot;")
	return strings.ReplaceAll(value, "'", "&apos;")
}

func decodeLocalAESKey(value string) ([]byte, error) {
	if decoded, err := hex.DecodeString(value); err == nil && len(decoded) == 16 {
		return decoded, nil
	}
	if len(value) == 16 {
		return []byte(value), nil
	}
	return nil, fmt.Errorf("local CAS key must be 16 bytes")
}

func pkcs7Pad(value []byte, size int) []byte {
	padding := size - len(value)%size
	return append(append([]byte(nil), value...), bytes.Repeat([]byte{byte(padding)}, padding)...)
}
