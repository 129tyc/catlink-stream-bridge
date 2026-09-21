package bridge

import "unsafe"

// localCASStreamInfo mirrors the PDB-confirmed x64 ST_STREAM_INFO layout.
// It is kept internal until the TLS/CAS bootstrap is implemented in Go.
type localCASStreamInfo struct {
	ClientSession      *byte
	ClientSessionLen   uint32
	DeviceSerial       [128]byte
	DeviceIP           [64]byte
	DeviceCommandPort  int32
	DeviceStreamPort   int32
	Channel            int32
	StreamType         int32
	OperationCode      [64]byte
	PermanentKey       [64]byte
	Key                [64]byte
	EncryptType        int32
	ServerIP           [64]byte
	ServerPort         int32
	StunIP             [64]byte
	StunPort           int32
	HDSign             [64]byte
	SupportNAT34       uint8
	SupportPlaybackEnd uint8
	streamFlagsPadding [2]byte
	DeviceType         int32
	PreconnectionType  int32
	NetworkType        int32
	DeviceShareCheck   [548]byte
	LID                [128]byte
	Timestamp          int64
	SuperSerial        [128]byte
	LinkEncryptV2      int32
	RecordType         int32
	FrameInterval      uint32
}

func newLocalCASStreamInfo(session LocalDeviceSession, channel int, quality string) localCASStreamInfo {
	streamType := int32(1)
	if quality == "sd" {
		streamType = 2
	}
	var info localCASStreamInfo
	copyCString(info.DeviceSerial[:], session.Serial)
	copyCString(info.DeviceIP[:], session.Host)
	info.DeviceCommandPort = int32(session.CommandPort)
	info.DeviceStreamPort = int32(session.StreamPort)
	info.Channel = int32(channel)
	info.StreamType = streamType
	copyCString(info.OperationCode[:], session.OperationCode)
	copyCString(info.Key[:], session.Key)
	info.EncryptType = boolToInt32(session.IsEncrypt)
	copyCString(info.ServerIP[:], session.CASIP)
	info.ServerPort = int32(session.CASPort)
	return info
}

func boolToInt32(value bool) int32 {
	if value {
		return 1
	}
	return 0
}

func copyCString(dst []byte, value string) {
	if len(dst) == 0 {
		return
	}
	copy(dst[:len(dst)-1], value)
}

var _ = unsafe.Sizeof(localCASStreamInfo{})
