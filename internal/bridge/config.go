package bridge

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
)

type Config struct {
	Listen       string          `json:"listen"`
	HealthListen string          `json:"healthListen"`
	OpenDomain   string          `json:"openDomain"`
	Accounts     []AccountConfig `json:"accounts"`
	Devices      []DeviceConfig  `json:"devices"`
	Routes       []RouteConfig   `json:"routes"`
}

type AccountConfig struct {
	ID                 string `json:"id"`
	SessionProviderURL string `json:"sessionProviderUrl,omitempty"`
	APIKey             string `json:"apiKey,omitempty"`
	SessionFile        string `json:"sessionFile,omitempty"`
	APIBase            string `json:"apiBase,omitempty"`
}

type DeviceConfig struct {
	ID        string          `json:"id"`
	AccountID string          `json:"accountId"`
	Serial    string          `json:"serial,omitempty"`
	Selector  *DeviceSelector `json:"selector,omitempty"`
}

type DeviceSelector struct {
	DeviceName   string `json:"deviceName"`
	DeviceSerial string `json:"deviceSerial,omitempty"`
}

type RouteConfig struct {
	Name     string `json:"name"`
	DeviceID string `json:"deviceId"`
	Channel  int    `json:"channel"`
	Quality  string `json:"quality"`
	Path     string `json:"path"`
	Source   string `json:"source,omitempty"`
}

func LoadConfig(r io.Reader) (Config, error) {
	var cfg Config
	decoder := json.NewDecoder(io.LimitReader(r, 1<<20))
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if cfg.Listen == "" {
		cfg.Listen = ":8554"
	}
	if cfg.HealthListen == "" {
		cfg.HealthListen = ":8080"
	}
	if cfg.OpenDomain == "" {
		cfg.OpenDomain = "https://open.ys7.com"
	}
	if err := validateConfig(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func validateConfig(cfg Config) error {
	accounts := make(map[string]AccountConfig, len(cfg.Accounts))
	for _, account := range cfg.Accounts {
		if account.ID == "" {
			return fmt.Errorf("account id is required")
		}
		providerConfigured := strings.TrimSpace(account.SessionProviderURL) != "" || strings.TrimSpace(account.APIKey) != ""
		fileConfigured := strings.TrimSpace(account.SessionFile) != ""
		if providerConfigured == fileConfigured {
			return fmt.Errorf("account %s must configure either sessionProviderUrl/apiKey or sessionFile", account.ID)
		}
		if providerConfigured && (strings.TrimSpace(account.SessionProviderURL) == "" || strings.TrimSpace(account.APIKey) == "") {
			return fmt.Errorf("account %s requires both sessionProviderUrl and apiKey", account.ID)
		}
		if providerConfigured {
			if _, err := url.ParseRequestURI(account.SessionProviderURL); err != nil {
				return fmt.Errorf("invalid session provider URL for account %s", account.ID)
			}
		}
		if strings.TrimSpace(account.APIBase) != "" {
			if !fileConfigured {
				return fmt.Errorf("apiBase is only supported with sessionFile for account %s", account.ID)
			}
			parsed, err := url.ParseRequestURI(account.APIBase)
			if err != nil || parsed.Scheme == "" || parsed.Host == "" {
				return fmt.Errorf("invalid apiBase for account %s", account.ID)
			}
		}
		if _, exists := accounts[account.ID]; exists {
			return fmt.Errorf("duplicate account %s", account.ID)
		}
		accounts[account.ID] = account
	}
	devices := make(map[string]DeviceConfig, len(cfg.Devices))
	for _, device := range cfg.Devices {
		if device.ID == "" || device.AccountID == "" {
			return fmt.Errorf("device id and accountId are required")
		}
		if device.Serial != "" && device.Selector != nil {
			return fmt.Errorf("device %s cannot contain both serial and selector", device.ID)
		}
		if device.Selector != nil {
			if strings.TrimSpace(device.Selector.DeviceName) == "" {
				return fmt.Errorf("device %s selector deviceName is required", device.ID)
			}
			if strings.TrimSpace(device.Selector.DeviceSerial) != "" && strings.TrimSpace(device.Selector.DeviceName) == "" {
				return fmt.Errorf("device %s selector deviceSerial requires deviceName", device.ID)
			}
		}
		if _, exists := accounts[device.AccountID]; !exists {
			return fmt.Errorf("device %s references unknown account %s", device.ID, device.AccountID)
		}
		if _, exists := devices[device.ID]; exists {
			return fmt.Errorf("duplicate device %s", device.ID)
		}
		devices[device.ID] = device
	}
	paths := make(map[string]struct{}, len(cfg.Routes))
	names := make(map[string]struct{}, len(cfg.Routes))
	for _, route := range cfg.Routes {
		if route.Name == "" || route.DeviceID == "" || route.Path == "" {
			return fmt.Errorf("route name, deviceId and path are required")
		}
		if _, exists := devices[route.DeviceID]; !exists {
			return fmt.Errorf("route %s references unknown device %s", route.Name, route.DeviceID)
		}
		if route.Channel != 1 && route.Channel != 2 {
			return fmt.Errorf("route %s channel must be 1 or 2", route.Name)
		}
		if route.Quality != "sd" && route.Quality != "hd" {
			return fmt.Errorf("route %s quality must be sd or hd", route.Name)
		}
		if route.Source != "" && route.Source != "cloud" && route.Source != "local" && route.Source != "auto" {
			return fmt.Errorf("route %s source must be cloud, local, or auto", route.Name)
		}
		if err := validatePath(route.Path); err != nil {
			return fmt.Errorf("route %s: %w", route.Name, err)
		}
		if _, exists := names[route.Name]; exists {
			return fmt.Errorf("duplicate route name %s", route.Name)
		}
		if _, exists := paths[route.Path]; exists {
			return fmt.Errorf("duplicate route path %s", route.Path)
		}
		names[route.Name] = struct{}{}
		paths[route.Path] = struct{}{}
	}
	if len(cfg.Routes) == 0 {
		return fmt.Errorf("at least one route is required")
	}
	return nil
}

func validatePath(path string) error {
	if len(path) > 256 || !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || strings.HasSuffix(path, "/") {
		return fmt.Errorf("invalid RTSP path")
	}
	for _, part := range strings.Split(path[1:], "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("invalid RTSP path")
		}
	}
	return nil
}
