package bridge

import "testing"

func TestApplyLocalPortFallbackForC07(t *testing.T) {
	info, err := applyLocalPortFallback(ezDevicePlayInfo{CASIP: "cas", CASPort: 6500}, "VISUAL_C07")
	if err != nil {
		t.Fatalf("fallback error: %v", err)
	}
	if info.LocalCmdPort != c07LocalCmdPort || info.LocalStreamPort != c07LocalStreamPort {
		t.Fatalf("ports=%d/%d", info.LocalCmdPort, info.LocalStreamPort)
	}
}

func TestApplyLocalPortFallbackPreservesAdvertisedPorts(t *testing.T) {
	info, err := applyLocalPortFallback(ezDevicePlayInfo{LocalCmdPort: 19010, LocalStreamPort: 19020}, "VISUAL_C07")
	if err != nil {
		t.Fatalf("fallback error: %v", err)
	}
	if info.LocalCmdPort != 19010 || info.LocalStreamPort != 19020 {
		t.Fatalf("ports=%d/%d", info.LocalCmdPort, info.LocalStreamPort)
	}
}

func TestApplyLocalPortFallbackRejectsUnknownDeviceType(t *testing.T) {
	if _, err := applyLocalPortFallback(ezDevicePlayInfo{}, "UNKNOWN"); err == nil {
		t.Fatal("missing ports accepted for unknown device type")
	}
}

func TestApplyLocalPortFallbackRejectsPartialPorts(t *testing.T) {
	for _, info := range []ezDevicePlayInfo{
		{LocalCmdPort: 9010},
		{LocalStreamPort: 9020},
		{LocalCmdPort: -1, LocalStreamPort: -1},
	} {
		if _, err := applyLocalPortFallback(info, "VISUAL_C07"); err == nil {
			t.Fatalf("invalid ports accepted: %+v", info)
		}
	}
}
