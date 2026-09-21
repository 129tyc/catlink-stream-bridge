package bridge

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"
)

func TestLocalSDKFrameUsesBoundedEncryptedEnvelope(t *testing.T) {
	key := "0123456789abcdef"
	source := &localSDKSource{
		session: LocalDeviceSession{OperationCode: "1234567", Key: key, IsEncrypt: false},
		channel: 1,
		quality: "hd",
	}
	body := source.previewXML()
	frame, err := buildLocalSDKFrame(localSDKPreview, []byte(body), key)
	if err != nil {
		t.Fatalf("build local frame: %v", err)
	}
	if len(frame) > localSDKHeaderSize+1<<20+localSDKTrailerSize {
		t.Fatalf("frame is unexpectedly large: %d", len(frame))
	}
	if string(frame[:4]) != localSDKMagic {
		t.Fatalf("magic=%x", frame[:4])
	}
	if got := binary.BigEndian.Uint16(frame[18:20]); got != localSDKPreview {
		t.Fatalf("command=0x%x", got)
	}
	ciphertext := frame[localSDKHeaderSize : len(frame)-localSDKTrailerSize]
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	plain := append([]byte(nil), ciphertext...)
	cipher.NewCBCDecrypter(block, []byte(localSDKIV)).CryptBlocks(plain, plain)
	plain = plain[:len(plain)-int(plain[len(plain)-1])]
	if string(plain) != body || !strings.Contains(string(plain), "<OperationCode>1234567</OperationCode>") {
		t.Fatalf("decrypted body mismatch: %q", plain)
	}
	digest := md5.Sum(ciphertext)
	if !bytes.Equal(frame[len(frame)-localSDKTrailerSize:], []byte(hex.EncodeToString(digest[:]))) {
		t.Fatal("ciphertext digest mismatch")
	}
}
