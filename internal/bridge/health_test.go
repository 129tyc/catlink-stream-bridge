package bridge

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReadinessTracksOutputServiceIndependentlyOfMedia(t *testing.T) {
	health := NewHealth([]string{"camera"})
	for _, step := range []struct {
		name   string
		rtsp   bool
		media  bool
		status int
		routes int
	}{
		{"starting", false, false, http.StatusServiceUnavailable, 0},
		{"listening without upstream", true, false, http.StatusOK, 0},
		{"upstream available", true, true, http.StatusOK, 1},
		{"upstream stopped", true, false, http.StatusOK, 0},
		{"output stopped with retained route", false, true, http.StatusServiceUnavailable, 1},
	} {
		t.Run(step.name, func(t *testing.T) {
			health.SetRTSP(step.rtsp)
			health.SetRoute("camera", step.media)
			response := httptest.NewRecorder()
			health.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if response.Code != step.status {
				t.Fatalf("status=%d, want %d", response.Code, step.status)
			}
			var body struct {
				Ready  bool `json:"ready"`
				RTSP   bool `json:"rtsp"`
				Routes int  `json:"routes"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Ready != step.rtsp || body.RTSP != step.rtsp || body.Routes != step.routes {
				t.Fatalf("unexpected readiness body: %+v", body)
			}
		})
	}
}
