package bridge

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	ezOpenAppKey       = "1975ba5d42c64ec693df8c260005f166"
	localFeatureCode   = "78313dadecd92bd11623638d57aa5139"
	localAppID         = "com.catlinks.app"
	localAppName       = "CATLINK"
	localSDKVersion    = "v5.30.2.20260804"
	localCASClientType = 13
	localCASServerName = "eucas.ezvizlife.com"
	localCASCommand    = 0x2001
	localCASMagic      = "\x9e\xba\xac\xe9"
	localCASHeaderSize = 32
	c07LocalCmdPort    = 9010
	c07LocalStreamPort = 9020
)

type ezDevicePlayInfo struct {
	DeviceSerial    string `json:"deviceSerial"`
	LocalIP         string `json:"localIp"`
	LocalCmdPort    int    `json:"localCmdPort"`
	LocalStreamPort int    `json:"localStreamPort"`
	CASIP           string `json:"casIp"`
	CASPort         int    `json:"casPort"`
	IsEncrypt       int    `json:"isEncrypt"`
}

type ezDevicePlayResponse struct {
	Result struct {
		Code string           `json:"code"`
		Msg  string           `json:"msg"`
		Data ezDevicePlayInfo `json:"data"`
	} `json:"result"`
}

type localCASResponse struct {
	Result  string `xml:"Result"`
	Session struct {
		Key           string `xml:"Key,attr"`
		OperationCode string `xml:"OperationCode,attr"`
		EncryptType   int    `xml:"EncryptType,attr"`
	} `xml:"Session"`
}

func (m *AccountTokenManager) ResolveLocalDevice(ctx context.Context, device DeviceConfig, resolved ResolvedDevice) (ResolvedDevice, error) {
	localDevice, _, err := m.resolveLocalDeviceWithGeneration(ctx, device, resolved)
	return localDevice, err
}

func (m *AccountTokenManager) ResolveLocalDeviceWithGeneration(ctx context.Context, device DeviceConfig, resolved ResolvedDevice) (ResolvedDevice, uint64, error) {
	return m.resolveLocalDeviceWithGeneration(ctx, device, resolved)
}

func (m *AccountTokenManager) resolveLocalDeviceWithGeneration(ctx context.Context, device DeviceConfig, resolved ResolvedDevice) (ResolvedDevice, uint64, error) {
	m.mu.Lock()
	if resolved.LocalIP != "" && resolved.LocalCmdPort > 0 && resolved.LocalStreamPort > 0 && resolved.LocalOperationCode != "" && resolved.LocalKey != "" {
		generation := m.generation
		m.mu.Unlock()
		return resolved, generation, nil
	}
	if err := m.ensureSessionLocked(ctx); err != nil {
		generation := m.generation
		m.mu.Unlock()
		return ResolvedDevice{}, generation, err
	}
	cameraToken, err := m.cameraTokenLocked(ctx)
	if err != nil {
		generation := m.generation
		m.mu.Unlock()
		return ResolvedDevice{}, generation, err
	}
	client := m.client
	generation := m.generation
	m.mu.Unlock()
	info, err := fetchEZDevicePlayInfo(ctx, client, cameraToken, resolved.EZOpenSerial)
	if err != nil {
		return ResolvedDevice{}, generation, err
	}
	info, err = applyLocalPortFallback(info, resolved.DeviceType)
	if err != nil {
		return ResolvedDevice{}, generation, err
	}
	if info.LocalIP == "" {
		info.LocalIP, err = fetchEZLocalAddress(ctx, client, cameraToken, resolved.EZOpenSerial)
		if err != nil {
			return ResolvedDevice{}, generation, err
		}
	}
	operationCode, key, err := fetchLocalCASOperation(ctx, info.CASIP, info.CASPort, cameraToken, resolved.EZOpenSerial)
	if err != nil {
		return ResolvedDevice{}, generation, err
	}
	resolved.LocalIP = info.LocalIP
	resolved.LocalCmdPort = info.LocalCmdPort
	resolved.LocalStreamPort = info.LocalStreamPort
	resolved.CASIP = info.CASIP
	resolved.CASPort = info.CASPort
	resolved.LocalOperationCode = operationCode
	resolved.LocalKey = key
	resolved.LocalIsEncrypt = info.IsEncrypt != 0
	_ = device
	return resolved, generation, nil
}

func applyLocalPortFallback(info ezDevicePlayInfo, deviceType string) (ezDevicePlayInfo, error) {
	if info.LocalCmdPort > 0 && info.LocalStreamPort > 0 {
		return info, nil
	}
	if info.LocalCmdPort != 0 || info.LocalStreamPort != 0 {
		return ezDevicePlayInfo{}, fmt.Errorf("local device detail has invalid LAN stream ports")
	}
	if deviceType != "VISUAL_C07" {
		return ezDevicePlayInfo{}, fmt.Errorf("local device detail has no LAN stream ports")
	}
	info.LocalCmdPort = c07LocalCmdPort
	info.LocalStreamPort = c07LocalStreamPort
	return info, nil
}

func fetchEZDevicePlayInfo(ctx context.Context, client *http.Client, cameraToken, serial string) (ezDevicePlayInfo, error) {
	form := url.Values{}
	form.Set("accessToken", cameraToken)
	form.Set("appKey", ezOpenAppKey)
	form.Set("appName", localAppName)
	form.Set("appID", localAppID)
	form.Set("clientType", "13")
	form.Set("featureCode", localFeatureCode)
	form.Set("osVersion", "1.0")
	form.Set("sdkVersion", localSDKVersion)
	form.Set("netType", "WIFI")
	form.Set("deviceSerial", serial)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, defaultOpenDomain+"/api/device/sdk/detail", strings.NewReader(form.Encode()))
	if err != nil {
		return ezDevicePlayInfo{}, err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := client.Do(request)
	if err != nil {
		return ezDevicePlayInfo{}, fmt.Errorf("local device detail request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 256<<10))
	if err != nil {
		return ezDevicePlayInfo{}, err
	}
	var payload ezDevicePlayResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return ezDevicePlayInfo{}, fmt.Errorf("local device detail response invalid")
	}
	if payload.Result.Code != "0" && payload.Result.Code != "200" {
		return ezDevicePlayInfo{}, fmt.Errorf("local device detail code %s: %s", payload.Result.Code, payload.Result.Msg)
	}
	if payload.Result.Data.CASIP == "" || payload.Result.Data.CASPort <= 0 {
		return ezDevicePlayInfo{}, fmt.Errorf("local device detail has no CAS endpoint")
	}
	return payload.Result.Data, nil
}

func fetchEZLocalAddress(ctx context.Context, client *http.Client, cameraToken, serial string) (string, error) {
	form := url.Values{}
	form.Set("accessToken", cameraToken)
	form.Set("deviceSerial", serial)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, defaultOpenDomain+"/api/lapp/device/info", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("local address request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 128<<10))
	if err != nil {
		return "", err
	}
	var payload struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			LocalAddress string `json:"localAddress"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("local address response invalid")
	}
	if payload.Code != "0" && payload.Code != "200" {
		return "", fmt.Errorf("local address code %s: %s", payload.Code, payload.Msg)
	}
	if payload.Data.LocalAddress == "" {
		return "", fmt.Errorf("local address response has no localAddress")
	}
	return payload.Data.LocalAddress, nil
}

func fetchLocalCASOperation(ctx context.Context, host string, port int, clientID, serial string) (string, string, error) {
	body := []byte(fmt.Sprintf("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<Request>\n\t<ClientID>%s</ClientID>\n\t<Sign>%s</Sign>\n\t<DevSerial>%s</DevSerial>\n\t<ClientType>%d</ClientType>\n</Request>\n", xmlEscape(clientID), localFeatureCode, xmlEscape(serial), localCASClientType))
	trailer := make([]byte, 16)
	if _, err := rand.Read(trailer); err != nil {
		return "", "", err
	}
	body = []byte(fmt.Sprintf("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<Request>\n\t<ClientID>%s</ClientID>\n\t<Sign>%s</Sign>\n\t<DevSerial>%s</DevSerial>\n\t<ClientType>%d</ClientType>\n</Request>\n", xmlEscape(clientID), localFeatureCode, xmlEscape(serial), localCASClientType))
	request := buildLocalCASFrame(body, trailer)
	var response []byte
	var err error
	if ctx == nil {
		ctx = context.Background()
	}
	response, err = exchangeLocalCAS(ctx, host, port, request, true, false)
	if err != nil {
		return "", "", fmt.Errorf("CAS exchange: %w", err)
	}
	body, err = localCASBody(response)
	if err != nil {
		return "", "", fmt.Errorf("CAS response: %w", err)
	}
	var parsed localCASResponse
	if err := xml.Unmarshal(body, &parsed); err != nil {
		return "", "", fmt.Errorf("CAS operation response invalid: %w", err)
	}
	if parsed.Session.OperationCode == "" || parsed.Session.Key == "" {
		return "", "", fmt.Errorf("CAS operation response has no session result=%s", parsed.Result)
	}
	return parsed.Session.OperationCode, parsed.Session.Key, nil
}

func buildLocalCASFrame(body, trailer []byte) []byte {
	header := make([]byte, localCASHeaderSize)
	copy(header[:4], []byte(localCASMagic))
	copy(header[4:8], []byte{1, 0, 0, 0})
	binary.BigEndian.PutUint32(header[8:12], 5)
	binary.BigEndian.PutUint32(header[16:20], localCASCommand)
	binary.BigEndian.PutUint32(header[24:28], uint32(len(body)))
	return append(append(header, body...), hex.EncodeToString(trailer)...)
}

func exchangeLocalCAS(ctx context.Context, host string, port int, payload []byte, useTLS, insecureTLS bool) ([]byte, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	var conn net.Conn
	var err error
	if useTLS {
		serverName := host
		if net.ParseIP(host) != nil {
			serverName = localCASServerName
		}
		address := net.JoinHostPort(host, strconv.Itoa(port))
		config := localCASTLSConfig(serverName, insecureTLS)
		conn, err = dialCASTLS(ctx, dialer, address, config)
		if err != nil && !insecureTLS && isExpiredCASTLSCertificate(err) {
			conn, err = dialCASTLS(ctx, dialer, address, localCASTLSConfig(serverName, true))
			if err == nil {
				state := conn.(*tls.Conn).ConnectionState()
				if verifyErr := verifyExpiredCASTLSChain(state, serverName); verifyErr != nil {
					_ = conn.Close()
					return nil, verifyErr
				}
			}
		}
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	}
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	}
	if _, err := conn.Write(payload); err != nil {
		return nil, err
	}
	header := make([]byte, localCASHeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	length := binary.BigEndian.Uint32(header[24:28])
	if length > 1<<20 {
		return nil, fmt.Errorf("CAS response body too large")
	}
	body := make([]byte, int(length))
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	tail := make([]byte, localCASHeaderSize)
	if _, err := io.ReadFull(conn, tail); err != nil {
		return nil, err
	}
	return append(append(header, body...), tail...), nil
}

func dialCASTLS(ctx context.Context, dialer *net.Dialer, address string, config *tls.Config) (net.Conn, error) {
	raw, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	conn := tls.Client(raw, config)
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return conn, nil
}

func localCASTLSConfig(serverName string, insecureSkipVerify bool) *tls.Config {
	return &tls.Config{
		ServerName:         serverName,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: insecureSkipVerify,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		},
	}
}

func isExpiredCASTLSCertificate(err error) bool {
	var invalid x509.CertificateInvalidError
	if errors.As(err, &invalid) {
		return invalid.Reason == x509.Expired
	}
	var verification *tls.CertificateVerificationError
	if errors.As(err, &verification) {
		return errors.As(verification.Err, &invalid) && invalid.Reason == x509.Expired
	}
	return false
}

func verifyExpiredCASTLSChain(state tls.ConnectionState, serverName string) error {
	if len(state.PeerCertificates) == 0 {
		return fmt.Errorf("CAS TLS expiry fallback received no peer certificate")
	}
	leaf := state.PeerCertificates[0]
	now := time.Now()
	if !leaf.NotAfter.Before(now) {
		return fmt.Errorf("CAS TLS expiry fallback received a non-expired certificate")
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return fmt.Errorf("CAS TLS expiry fallback cannot load system roots: %w", err)
	}
	intermediates := x509.NewCertPool()
	for _, certificate := range state.PeerCertificates[1:] {
		intermediates.AddCert(certificate)
	}
	verifyAt := leaf.NotAfter.Add(-time.Second)
	if verifyAt.Before(leaf.NotBefore) {
		return fmt.Errorf("CAS TLS expiry fallback certificate validity window is invalid")
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		DNSName:       serverName,
		Roots:         roots,
		Intermediates: intermediates,
		CurrentTime:   verifyAt,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		return fmt.Errorf("CAS TLS expiry fallback certificate chain invalid: %w", err)
	}
	return nil
}

func localCASBody(frame []byte) ([]byte, error) {
	if len(frame) < localCASHeaderSize || string(frame[:4]) != localCASMagic {
		return nil, fmt.Errorf("CAS response frame invalid")
	}
	length := int(binary.BigEndian.Uint32(frame[24:28]))
	if len(frame) < localCASHeaderSize+length {
		return nil, fmt.Errorf("CAS response body truncated")
	}
	return frame[localCASHeaderSize : localCASHeaderSize+length], nil
}
