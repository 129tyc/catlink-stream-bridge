package bridge

import (
	"testing"
	"unsafe"
)

func TestLocalCASStreamInfoLayout(t *testing.T) {
	if got := unsafe.Sizeof(localCASStreamInfo{}); got != 1464 {
		t.Fatalf("ST_STREAM_INFO size = %d, want 1464", got)
	}
	checks := map[string]uintptr{
		"DeviceSerial":      unsafe.Offsetof(localCASStreamInfo{}.DeviceSerial),
		"DeviceIP":          unsafe.Offsetof(localCASStreamInfo{}.DeviceIP),
		"DeviceCommandPort": unsafe.Offsetof(localCASStreamInfo{}.DeviceCommandPort),
		"DeviceStreamPort":  unsafe.Offsetof(localCASStreamInfo{}.DeviceStreamPort),
		"OperationCode":     unsafe.Offsetof(localCASStreamInfo{}.OperationCode),
		"Key":               unsafe.Offsetof(localCASStreamInfo{}.Key),
		"ServerIP":          unsafe.Offsetof(localCASStreamInfo{}.ServerIP),
		"DeviceShareCheck":  unsafe.Offsetof(localCASStreamInfo{}.DeviceShareCheck),
		"LID":               unsafe.Offsetof(localCASStreamInfo{}.LID),
		"Timestamp":         unsafe.Offsetof(localCASStreamInfo{}.Timestamp),
		"SuperSerial":       unsafe.Offsetof(localCASStreamInfo{}.SuperSerial),
		"FrameInterval":     unsafe.Offsetof(localCASStreamInfo{}.FrameInterval),
	}
	want := map[string]uintptr{
		"DeviceSerial":      12,
		"DeviceIP":          140,
		"DeviceCommandPort": 204,
		"DeviceStreamPort":  208,
		"OperationCode":     220,
		"Key":               348,
		"ServerIP":          416,
		"DeviceShareCheck":  632,
		"LID":               1180,
		"Timestamp":         1312,
		"SuperSerial":       1320,
		"FrameInterval":     1456,
	}
	for name, got := range checks {
		if got != want[name] {
			t.Errorf("%s offset = %d, want %d", name, got, want[name])
		}
	}
}

func TestNewLocalCASStreamInfo(t *testing.T) {
	session := LocalDeviceSession{
		Serial:        "device",
		Host:          "192.0.2.10",
		CommandPort:   9010,
		StreamPort:    9020,
		OperationCode: "op",
		Key:           "key",
		CASIP:         "192.0.2.20",
		CASPort:       6500,
		IsEncrypt:     true,
	}
	info := newLocalCASStreamInfo(session, 2, "sd")
	if string(info.DeviceIP[:len("192.0.2.10")]) != "192.0.2.10" {
		t.Fatalf("device IP not copied")
	}
	if info.Channel != 2 || info.StreamType != 2 || info.DeviceCommandPort != 9010 || info.DeviceStreamPort != 9020 {
		t.Fatalf("stream fields not copied: %+v", info)
	}
	if info.EncryptType != 1 || info.ServerPort != 6500 {
		t.Fatalf("CAS fields not copied")
	}
}
