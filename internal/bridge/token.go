package bridge

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	catlinkSignKey        = "00109190907746a7ad0e2139b6d09ce47551770157fe4ac5922f3a5454c82712"
	defaultCATLINKAPIBase = "https://app-sh.catlinks.cn/api/"
)

type AccountTokenManager struct {
	provider *SessionProvider
	client   *http.Client

	sessionFile    string
	sessionAPIBase string

	mu          sync.Mutex
	loginToken  string
	apiBase     string
	cameraToken string
	generation  uint64
	catalog     []DeviceCandidate
	catalogAt   time.Time
}

type accountGenerationError struct {
	generation uint64
	err        error
}

func (e accountGenerationError) Error() string { return e.err.Error() }
func (e accountGenerationError) Unwrap() error { return e.err }

func wrapAccountGenerationError(generation uint64, err error) error {
	if err == nil {
		return nil
	}
	return accountGenerationError{generation: generation, err: err}
}

type SessionProvider struct {
	URL    string
	APIKey string
	Client *http.Client
}

func NewAccountTokenManager(account AccountConfig, client *http.Client) *AccountTokenManager {
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	manager := &AccountTokenManager{client: client}
	if strings.TrimSpace(account.SessionFile) != "" {
		manager.sessionFile = strings.TrimSpace(account.SessionFile)
		manager.sessionAPIBase = normalizeAPIBase(account.APIBase)
		return manager
	}
	manager.provider = &SessionProvider{URL: account.SessionProviderURL, APIKey: account.APIKey, Client: client}
	return manager
}

func (m *AccountTokenManager) Token(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ensureSessionLocked(ctx); err != nil {
		return "", err
	}
	return m.cameraTokenLocked(ctx)
}

func (m *AccountTokenManager) ResolveDevice(ctx context.Context, selector *DeviceSelector) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ensureSessionLocked(ctx); err != nil {
		return "", err
	}
	device, err := m.resolveDeviceIdentityLocked(ctx, DeviceConfig{Selector: selector})
	if err != nil {
		return "", err
	}
	return device.EZOpenSerial, nil
}

func (m *AccountTokenManager) SessionForDevice(ctx context.Context, device DeviceConfig) (AccountSessionSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ensureSessionLocked(ctx); err != nil {
		return AccountSessionSnapshot{}, wrapAccountGenerationError(m.generation, err)
	}
	cameraToken, err := m.cameraTokenLocked(ctx)
	if err != nil {
		return AccountSessionSnapshot{}, wrapAccountGenerationError(m.generation, err)
	}
	resolved, err := m.resolveDeviceIdentityLocked(ctx, device)
	if err != nil {
		return AccountSessionSnapshot{}, wrapAccountGenerationError(m.generation, err)
	}
	return AccountSessionSnapshot{
		CameraToken: cameraToken,
		Device:      resolved,
		Generation:  m.generation,
	}, nil
}

func (m *AccountTokenManager) cameraTokenLocked(ctx context.Context) (string, error) {
	if m.cameraToken != "" {
		return m.cameraToken, nil
	}
	accessToken, err := exchangeCameraToken(ctx, m.client, m.apiBase, m.loginToken)
	if err != nil {
		if isCameraTokenAuthorizationError(err) {
			m.loginToken = ""
			m.apiBase = ""
			m.cameraToken = ""
			m.catalog = nil
			m.catalogAt = time.Time{}
			return "", fmt.Errorf("%w: %v", ErrTalkAuthorization, err)
		}
		return "", err
	}
	m.cameraToken = accessToken
	return accessToken, nil
}

func isCameraTokenAuthorizationError(err error) bool {
	if err == nil {
		return false
	}
	value := strings.ToLower(err.Error())
	return strings.Contains(value, "business code 401") ||
		strings.Contains(value, "business code 403") ||
		strings.Contains(value, "business code 1002") ||
		strings.Contains(value, "status 401") ||
		strings.Contains(value, "status 403") ||
		strings.Contains(value, "unauthorized") ||
		strings.Contains(value, "token expired") ||
		strings.Contains(value, "token invalid")
}

func (m *AccountTokenManager) resolveDeviceIdentityLocked(ctx context.Context, device DeviceConfig) (ResolvedDevice, error) {
	if strings.TrimSpace(device.Serial) != "" {
		if device.Selector != nil {
			return ResolvedDevice{}, fmt.Errorf("device cannot contain both serial and selector")
		}
		return ResolvedDevice{EZOpenSerial: strings.TrimSpace(device.Serial)}, nil
	}
	catalog, err := m.deviceCatalogLocked(ctx)
	if err != nil {
		return ResolvedDevice{}, err
	}
	candidate, err := selectDevice(catalog, device.Selector)
	if err != nil {
		return ResolvedDevice{}, err
	}
	profile, ok := cameraDeviceProfiles[candidate.DeviceType]
	if !ok {
		return ResolvedDevice{}, fmt.Errorf("selected CATLINK device has no camera profile")
	}
	if candidate.EZOpenSerial == "" {
		serial, err := profile.resolveEZOpenSerial(ctx, m.client, m.apiBase, m.loginToken, candidate.APIID)
		if err != nil {
			return ResolvedDevice{}, fmt.Errorf("device %q detail lookup: %w", candidate.DeviceName, err)
		}
		candidate.EZOpenSerial = serial
		for index := range m.catalog {
			if m.catalog[index].APIID == candidate.APIID {
				m.catalog[index].EZOpenSerial = serial
				break
			}
		}
	}
	return ResolvedDevice{
		APIID:         candidate.APIID,
		DeviceName:    candidate.DeviceName,
		Model:         candidate.Model,
		DeviceType:    candidate.DeviceType,
		VisibleSerial: candidate.VisibleSerial,
		EZOpenSerial:  candidate.EZOpenSerial,
	}, nil
}

func (m *AccountTokenManager) ensureSessionLocked(ctx context.Context) error {
	if m.sessionFile != "" {
		loginToken, apiBase, err := loadSessionFile(m.sessionFile, m.sessionAPIBase)
		if err != nil {
			return err
		}
		if m.loginToken == loginToken && m.apiBase == apiBase {
			return nil
		}
		m.loginToken = loginToken
		m.apiBase = apiBase
		m.cameraToken = ""
		m.catalog = nil
		m.catalogAt = time.Time{}
		m.generation++
		return nil
	}
	if m.loginToken != "" && m.apiBase != "" {
		return nil
	}
	if m.provider == nil {
		return errors.New("no session source configured")
	}
	loginToken, apiBase, err := m.provider.Get(ctx)
	if err != nil {
		return err
	}
	m.loginToken = loginToken
	m.apiBase = apiBase
	m.generation++
	return nil
}

func normalizeAPIBase(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultCATLINKAPIBase
	}
	return strings.TrimRight(value, "/") + "/"
}

func loadSessionFile(pattern, apiBaseOverride string) (string, string, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", "", fmt.Errorf("invalid session file pattern: %w", err)
	}
	if len(matches) == 0 {
		return "", "", fmt.Errorf("session file not found")
	}
	if len(matches) != 1 {
		return "", "", fmt.Errorf("session file matches multiple files")
	}
	contents, err := os.ReadFile(matches[0])
	if err != nil {
		return "", "", errors.New("read session file")
	}
	trimmedContents := strings.TrimSpace(string(contents))
	token := trimmedContents
	var payload struct {
		Token      string `json:"token"`
		LoginToken string `json:"loginToken"`
		Data       struct {
			Token string `json:"token"`
		} `json:"data"`
	}
	if strings.HasPrefix(trimmedContents, "{") || strings.HasPrefix(trimmedContents, "[") {
		if err := json.Unmarshal(contents, &payload); err != nil {
			return "", "", errors.New("session file contains invalid JSON")
		}
		switch {
		case strings.TrimSpace(payload.Token) != "":
			token = strings.TrimSpace(payload.Token)
		case strings.TrimSpace(payload.LoginToken) != "":
			token = strings.TrimSpace(payload.LoginToken)
		case strings.TrimSpace(payload.Data.Token) != "":
			token = strings.TrimSpace(payload.Data.Token)
		default:
			return "", "", errors.New("session file contains no token")
		}
	}
	if token == "" {
		return "", "", fmt.Errorf("session file contains no token")
	}
	return token, normalizeAPIBase(apiBaseOverride), nil
}

func (m *AccountTokenManager) deviceCatalogLocked(ctx context.Context) ([]DeviceCandidate, error) {
	if !m.catalogAt.IsZero() && time.Since(m.catalogAt) < deviceCatalogTTL {
		return append([]DeviceCandidate(nil), m.catalog...), nil
	}
	catalog, err := fetchDeviceCatalog(ctx, m.client, m.apiBase, m.loginToken)
	if err != nil {
		if len(m.catalog) > 0 && !isAuthFailure(err) {
			return append([]DeviceCandidate(nil), m.catalog...), nil
		}
		if isAuthFailure(err) {
			m.loginToken = ""
			m.apiBase = ""
			m.cameraToken = ""
			m.catalog = nil
			m.catalogAt = time.Time{}
		}
		return nil, err
	}
	m.catalog = catalog
	m.catalogAt = time.Now()
	return append([]DeviceCandidate(nil), catalog...), nil
}

func (m *AccountTokenManager) Invalidate() {
	m.mu.Lock()
	m.invalidateLocked()
	m.mu.Unlock()
}

func (m *AccountTokenManager) InvalidateGeneration(generation uint64) {
	m.mu.Lock()
	if generation == m.generation {
		m.invalidateLocked()
	}
	m.mu.Unlock()
}

func (m *AccountTokenManager) invalidateLocked() {
	m.loginToken = ""
	m.apiBase = ""
	m.cameraToken = ""
	m.catalog = nil
	m.catalogAt = time.Time{}
}

func (p *SessionProvider) Get(ctx context.Context) (string, string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL, nil)
	if err != nil {
		return "", "", err
	}
	request.Header.Set("X-CATLINK-BRIDGE-KEY", p.APIKey)
	request.Header.Set("Authorization", "Bearer "+p.APIKey)
	request.Header.Set("Accept", "application/json")
	response, err := p.Client.Do(request)
	if err != nil {
		return "", "", fmt.Errorf("session provider request: %w", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil {
		return "", "", err
	}
	if response.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("session provider status %d", response.StatusCode)
	}
	var payload struct {
		Token   string `json:"token"`
		APIBase string `json:"apiBase"`
	}
	if err := json.Unmarshal(responseBody, &payload); err != nil || payload.Token == "" || payload.APIBase == "" {
		return "", "", fmt.Errorf("session provider returned no session")
	}
	return strings.TrimSpace(payload.Token), strings.TrimRight(payload.APIBase, "/") + "/", nil
}

func exchangeCameraToken(ctx context.Context, client *http.Client, apiBase, loginToken string) (string, error) {
	body, err := catlinkRequest(ctx, client, apiBase, loginToken, http.MethodPost, "token/device/camera/accessToken", nil)
	if err != nil {
		return "", fmt.Errorf("camera token request: %w", err)
	}
	var payload struct {
		ReturnCode int `json:"returnCode"`
		Data       struct {
			AccessToken string `json:"accessToken"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("camera token exchange response invalid")
	}
	if payload.ReturnCode != 0 {
		return "", fmt.Errorf("camera token business code %d", payload.ReturnCode)
	}
	if payload.Data.AccessToken == "" {
		return "", fmt.Errorf("camera token exchange rejected")
	}
	return payload.Data.AccessToken, nil
}

func catlinkRequest(ctx context.Context, client *http.Client, apiBase, loginToken, method, path string, fields map[string]string) ([]byte, error) {
	params := url.Values{}
	for key, value := range fields {
		params.Set(key, value)
	}
	noncestr := strconv.FormatInt(time.Now().UnixMilli(), 10)
	params.Set("noncestr", noncestr)
	params.Set("token", loginToken)
	signFields := make(map[string]string, len(fields)+2)
	for key, value := range fields {
		signFields[key] = value
	}
	signFields["noncestr"] = noncestr
	signFields["token"] = loginToken
	params.Set("sign", signParams(signFields))
	requestURL := strings.TrimRight(apiBase, "/") + "/" + strings.TrimLeft(path, "/")
	var body io.Reader
	if method == http.MethodGet {
		requestURL += "?" + params.Encode()
	} else {
		body = strings.NewReader(params.Encode())
	}
	request, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Language", "en_US")
	request.Header.Set("User-Agent", "catlink-go-bridge")
	request.Header.Set("Token", loginToken)
	if method != http.MethodGet {
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, sanitizedCATLinkTransportError(err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("status %d", response.StatusCode)
	}
	return responseBody, nil
}

func sanitizedCATLinkTransportError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errors.New("CATLINK request transport failure")
}

func signParams(params map[string]string) string {
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys)+1)
	for _, key := range keys {
		parts = append(parts, key+"="+params[key])
	}
	parts = append(parts, "key="+catlinkSignKey)
	sum := md5.Sum([]byte(strings.Join(parts, "&")))
	return strings.ToUpper(hex.EncodeToString(sum[:]))
}
