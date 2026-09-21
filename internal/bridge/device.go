package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const deviceCatalogTTL = 5 * time.Minute

type DeviceCandidate struct {
	APIID         string
	DeviceName    string
	Model         string
	DeviceType    string
	VisibleSerial string
	EZOpenSerial  string
}

type deviceProfile struct {
	deviceType string
	detailPath string
}

var cameraDeviceProfiles = map[string]deviceProfile{
	"VISUAL_C07": {deviceType: "VISUAL_C07", detailPath: "token/cameraLitterbox/info"},
}

func fetchDeviceCatalog(ctx context.Context, client *http.Client, apiBase, loginToken string) ([]DeviceCandidate, error) {
	body, err := catlinkRequest(ctx, client, apiBase, loginToken, http.MethodGet, "token/device/union/list/sorted", map[string]string{
		"type": "NONE",
	})
	if err != nil {
		return nil, err
	}
	var envelope struct {
		ReturnCode json.RawMessage `json:"returnCode"`
		Data       json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("device catalog response invalid")
	}
	code, present, err := parseBusinessCode(envelope.ReturnCode)
	if err != nil {
		return nil, fmt.Errorf("device catalog returnCode invalid: %w", err)
	}
	if present && code != 0 {
		return nil, fmt.Errorf("device catalog business code %d", code)
	}
	if len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return nil, fmt.Errorf("device catalog has no data")
	}
	var data struct {
		Devices []json.RawMessage `json:"devices"`
	}
	if err := json.Unmarshal(envelope.Data, &data); err != nil || data.Devices == nil {
		return nil, fmt.Errorf("device catalog has no devices")
	}

	catalog := make([]DeviceCandidate, 0, len(data.Devices))
	for _, raw := range data.Devices {
		var item struct {
			ID              string `json:"id"`
			DeviceName      string `json:"deviceName"`
			Model           string `json:"model"`
			DeviceType      string `json:"deviceType"`
			OtherExtendInfo struct {
				DeviceSerial string `json:"deviceSerial"`
			} `json:"otherExtendInfo"`
		}
		if err := json.Unmarshal(raw, &item); err != nil || strings.TrimSpace(item.ID) == "" {
			continue
		}
		if _, ok := cameraDeviceProfiles[item.DeviceType]; !ok {
			continue
		}
		candidate := DeviceCandidate{
			APIID:         strings.TrimSpace(item.ID),
			DeviceName:    strings.TrimSpace(item.DeviceName),
			Model:         strings.TrimSpace(item.Model),
			DeviceType:    item.DeviceType,
			VisibleSerial: strings.TrimSpace(item.OtherExtendInfo.DeviceSerial),
		}
		catalog = append(catalog, candidate)
	}
	return catalog, nil
}

func (p deviceProfile) resolveEZOpenSerial(ctx context.Context, client *http.Client, apiBase, loginToken, apiID string) (string, error) {
	body, err := catlinkRequest(ctx, client, apiBase, loginToken, http.MethodGet, p.detailPath, map[string]string{
		"deviceId": apiID,
	})
	if err != nil {
		return "", err
	}
	var envelope struct {
		ReturnCode json.RawMessage `json:"returnCode"`
		Data       struct {
			DeviceInfo struct {
				DN string `json:"dn"`
			} `json:"deviceInfo"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return "", fmt.Errorf("device detail response invalid")
	}
	code, present, err := parseBusinessCode(envelope.ReturnCode)
	if err != nil {
		return "", fmt.Errorf("device detail returnCode invalid: %w", err)
	}
	if present && code != 0 {
		return "", fmt.Errorf("device detail business code %d", code)
	}
	serial := strings.TrimSpace(envelope.Data.DeviceInfo.DN)
	if serial == "" {
		return "", fmt.Errorf("device detail has no dn")
	}
	return serial, nil
}

func selectDevice(catalog []DeviceCandidate, selector *DeviceSelector) (DeviceCandidate, error) {
	if selector == nil {
		if len(catalog) == 1 {
			return catalog[0], nil
		}
		return DeviceCandidate{}, fmt.Errorf("camera device selection is ambiguous: %d matches", len(catalog))
	}
	name := strings.TrimSpace(selector.DeviceName)
	serial := strings.TrimSpace(selector.DeviceSerial)
	matches := make([]DeviceCandidate, 0, len(catalog))
	for _, candidate := range catalog {
		if candidate.DeviceName == name {
			if serial == "" || candidate.VisibleSerial == serial {
				matches = append(matches, candidate)
			}
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) == 0 {
		return DeviceCandidate{}, fmt.Errorf("camera device not found for name %q", name)
	}
	return DeviceCandidate{}, fmt.Errorf("camera device name %q is ambiguous: %d matches", name, len(matches))
}
