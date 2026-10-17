package bridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type tokenRecoveryTransport struct {
	code          string
	providerCalls int
	exchangeCalls int
	detailTokens  []string
	cancel        context.CancelFunc
}

func (s *tokenRecoveryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var body string
	switch request.URL.Path {
	case "/v1/session":
		s.providerCalls++
		body = `{"token":"synthetic-login","apiBase":"https://session.example/"}`
	case "/token/device/camera/accessToken":
		s.exchangeCalls++
		body = fmt.Sprintf(`{"returnCode":0,"data":{"accessToken":"synthetic-camera-%d"}}`, s.exchangeCalls)
	case "/api/device/sdk/detail":
		if err := request.ParseForm(); err != nil {
			return nil, err
		}
		s.detailTokens = append(s.detailTokens, request.Form.Get("accessToken"))
		if s.cancel != nil && len(s.detailTokens) == 2 {
			s.cancel()
		}
		body = fmt.Sprintf(`{"result":{"code":%q,"msg":"accessToken过期或参数异常"}}`, s.code)
	default:
		return nil, fmt.Errorf("unexpected mock request path %s", request.URL.Path)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}

func newTokenRecoveryRunner(t *testing.T, transport *tokenRecoveryTransport, sessionFile bool) (*RouteRunner, *AccountTokenManager) {
	t.Helper()
	client := &http.Client{Transport: transport}
	account := AccountConfig{ID: "account", SessionProviderURL: "https://session.example/v1/session"}
	if sessionFile {
		path := filepath.Join(t.TempDir(), "session.json")
		if err := os.WriteFile(path, []byte(`{"token":"synthetic-login"}`), 0600); err != nil {
			t.Fatal(err)
		}
		account = AccountConfig{ID: "account", SessionFile: path, APIBase: "https://session.example/"}
	}
	manager := NewAccountTokenManager(account, client)
	runner := NewRouteRunner(
		RouteConfig{Name: "camera", Source: "local", Channel: 1, Quality: "hd", Path: "/camera"},
		DeviceConfig{Serial: "synthetic-serial", AccountID: "account"},
		manager, "", client, nil, NewHealth([]string{"camera"}),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	return runner, manager
}

func TestRouteRunnerRefreshesRejectedLocalCameraToken(t *testing.T) {
	for _, sessionFile := range []bool{false, true} {
		for _, code := range []string{"10002", "401", "500"} {
			t.Run(fmt.Sprintf("sessionFile=%v/code=%s", sessionFile, code), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				transport := &tokenRecoveryTransport{code: code, cancel: cancel}
				runner, _ := newTokenRecoveryRunner(t, transport, sessionFile)
				runner.Run(ctx)
				if len(transport.detailTokens) != 2 {
					t.Fatalf("detail attempts=%d, want 2", len(transport.detailTokens))
				}
				wantExchanges := 2
				wantSecondToken := "synthetic-camera-2"
				if code == "500" {
					wantExchanges = 1
					wantSecondToken = "synthetic-camera-1"
				}
				if transport.exchangeCalls != wantExchanges {
					t.Fatalf("token exchanges=%d, want %d", transport.exchangeCalls, wantExchanges)
				}
				if transport.detailTokens[0] != "synthetic-camera-1" || transport.detailTokens[1] != wantSecondToken {
					t.Fatalf("detail tokens=%v, want initial then %s", transport.detailTokens, wantSecondToken)
				}
				wantProviderCalls := wantExchanges
				if sessionFile {
					wantProviderCalls = 0
				}
				if transport.providerCalls != wantProviderCalls {
					t.Fatalf("session provider calls=%d, want %d", transport.providerCalls, wantProviderCalls)
				}
			})
		}
	}
}

func TestLocalAuthorizationFailurePreservesNewerAccountSession(t *testing.T) {
	transport := &tokenRecoveryTransport{code: "10002"}
	runner, manager := newTokenRecoveryRunner(t, transport, false)
	ctx := context.Background()
	firstErr := runner.runAttempt(ctx, nil)
	secondErr := runner.runAttempt(ctx, nil)
	for _, err := range []error{firstErr, secondErr} {
		var generationErr accountGenerationError
		if !isAuthFailure(err) || !errors.As(err, &generationErr) || generationErr.generation == 0 {
			t.Fatalf("local failure lost authorization or account generation: %v", err)
		}
	}
	runner.invalidateAuthFailure(firstErr)
	freshToken, err := manager.Token(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A delayed error from the other channel must not clear the new session.
	runner.invalidateAuthFailure(secondErr)
	currentToken, err := manager.Token(ctx)
	if err != nil || currentToken != freshToken || transport.exchangeCalls != 2 || transport.providerCalls != 2 {
		t.Fatalf("stale failure invalidated fresh session: exchanges=%d provider=%d err=%v", transport.exchangeCalls, transport.providerCalls, err)
	}
}
