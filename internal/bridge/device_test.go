package bridge

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestResolveDeviceUsesAppVisibleSelectorAndC07DN(t *testing.T) {
	var providerCalls, listCalls, detailCalls atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/session":
			providerCalls.Add(1)
			_, _ = io.WriteString(response, `{"token":"login","apiBase":"`+server.URL+`/"}`)
		case "/token/device/union/list/sorted":
			listCalls.Add(1)
			if request.Method != http.MethodGet || request.URL.Query().Get("type") != "NONE" {
				t.Errorf("unexpected device-list request: %s %s", request.Method, request.URL.String())
			}
			_, _ = io.WriteString(response, `{"returnCode":0,"data":{"devices":[
  {"id":"api-c07","deviceName":"客厅猫砂盆","model":"大白 Pro","deviceType":"VISUAL_C07","otherExtendInfo":{"deviceSerial":"visible-c07"}},
  {"id":"api-broken-c07","deviceName":"卧室猫砂盆","model":"大白 Pro","deviceType":"VISUAL_C07","otherExtendInfo":{"deviceSerial":"visible-broken"}},
  {"id":"api-feeder","deviceName":"喂食器","model":"Visual Feeder","deviceType":"VISUAL_FEEDER"}
]}}`)
		case "/token/cameraLitterbox/info":
			detailCalls.Add(1)
			if request.Method != http.MethodGet || request.URL.Query().Get("deviceId") != "api-c07" {
				t.Errorf("unexpected C07 detail request: %s %s", request.Method, request.URL.String())
				response.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, _ = io.WriteString(response, `{"returnCode":0,"data":{"deviceInfo":{"dn":"ezopen-c07"}}}`)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	manager := NewAccountTokenManager(AccountConfig{
		ID: "account", SessionProviderURL: server.URL + "/v1/session", APIKey: "key",
	}, server.Client())
	selector := &DeviceSelector{DeviceName: "客厅猫砂盆"}
	for attempt := 0; attempt < 4; attempt++ {
		serial, err := manager.ResolveDevice(context.Background(), selector)
		if err != nil || serial != "ezopen-c07" {
			t.Fatalf("attempt %d serial=%q err=%v", attempt, serial, err)
		}
	}
	if providerCalls.Load() != 1 || listCalls.Load() != 1 || detailCalls.Load() != 1 {
		t.Fatalf("calls provider=%d list=%d detail=%d", providerCalls.Load(), listCalls.Load(), detailCalls.Load())
	}
}

type failingRoundTripper func(*http.Request) (*http.Response, error)

func (f failingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestCatlinkRequestSanitizesTransportError(t *testing.T) {
	client := &http.Client{Transport: failingRoundTripper(func(request *http.Request) (*http.Response, error) {
		return nil, errors.New("connection reset")
	})}

	_, err := catlinkRequest(context.Background(), client, "https://api.example.invalid/", "login-token", http.MethodGet, "token/device/union/list/sorted", map[string]string{
		"type":     "NONE",
		"deviceId": "api-device",
	})
	if err == nil {
		t.Fatal("expected transport error")
	}
	for _, secret := range []string{"login-token", "api-device", "sign="} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("transport error leaked %q: %v", secret, err)
		}
	}
}

func TestSelectDeviceRejectsAmbiguousAndMismatchedSelectors(t *testing.T) {
	catalog := []DeviceCandidate{
		{APIID: "one", DeviceName: "大白 Pro", VisibleSerial: "visible-one", EZOpenSerial: "ez-one"},
		{APIID: "two", DeviceName: "大白 Pro", VisibleSerial: "visible-two", EZOpenSerial: "ez-two"},
	}
	if _, err := selectDevice(catalog, &DeviceSelector{DeviceName: "大白 Pro"}); err == nil {
		t.Fatal("ambiguous name accepted")
	}
	candidate, err := selectDevice(catalog, &DeviceSelector{DeviceName: "大白 Pro", DeviceSerial: "visible-two"})
	if err != nil || candidate.APIID != "two" {
		t.Fatalf("tie-break candidate=%+v err=%v", candidate, err)
	}
	if _, err := selectDevice(catalog, &DeviceSelector{DeviceName: "大白 Pro", DeviceSerial: "missing"}); err == nil {
		t.Fatal("mismatched serial accepted")
	}
	if candidate, err := selectDevice(catalog[:1], nil); err != nil || candidate.APIID != "one" {
		t.Fatalf("unique auto-selection candidate=%+v err=%v", candidate, err)
	}
}
