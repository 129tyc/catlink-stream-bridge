package bridge

import (
	"encoding/json"
	"net/http"
	"sync"
)

type Health struct {
	mu     sync.RWMutex
	rtsp   bool
	routes map[string]bool
}

func NewHealth(routeNames []string) *Health {
	routes := make(map[string]bool, len(routeNames))
	for _, name := range routeNames {
		routes[name] = false
	}
	return &Health{routes: routes}
}

func (h *Health) SetRTSP(ready bool) {
	h.mu.Lock()
	h.rtsp = ready
	h.mu.Unlock()
}

func (h *Health) SetRoute(name string, ready bool) {
	h.mu.Lock()
	h.routes[name] = ready
	h.mu.Unlock()
}

func (h *Health) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	h.mu.RLock()
	rtsp := h.rtsp
	readyRoutes := 0
	for _, ready := range h.routes {
		if ready {
			readyRoutes++
		}
	}
	h.mu.RUnlock()

	if request.URL.Path == "/healthz" {
		writeHealth(response, http.StatusOK, map[string]any{"ok": true, "rtsp": rtsp})
		return
	}
	if request.URL.Path == "/readyz" {
		status := http.StatusOK
		if !rtsp || readyRoutes == 0 {
			status = http.StatusServiceUnavailable
		}
		writeHealth(response, status, map[string]any{"ready": status == http.StatusOK, "rtsp": rtsp, "routes": readyRoutes})
		return
	}
	http.NotFound(response, request)
}

func writeHealth(response http.ResponseWriter, status int, body map[string]any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(body)
}
