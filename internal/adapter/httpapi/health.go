package httpapi

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	checkTimeout  = time.Second
	checkCacheTTL = 2 * time.Second
)

type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

// Health answers liveness and readiness. Readiness results are cached so probes cannot overload dependencies.
type Health struct {
	checks   []Check
	now      func() time.Time
	draining atomic.Bool

	mu      sync.Mutex
	checked time.Time
	last    map[string]string
}

func NewHealth(checks ...Check) *Health {
	return &Health{checks: checks, now: time.Now}
}

// Drain makes readiness fail so no new traffic is routed here while the server shuts down.
func (h *Health) Drain() { h.draining.Store(true) }

func (h *Health) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
}

func (h *Health) ready(w http.ResponseWriter, r *http.Request) {
	if h.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "DOWN", "reason": "shutting down"})
		return
	}
	results := h.results(r.Context())
	status, code := "UP", http.StatusOK
	for _, res := range results {
		if res != "up" {
			status, code = "DOWN", http.StatusServiceUnavailable
		}
	}
	writeJSON(w, code, map[string]any{"status": status, "checks": results})
}

func (h *Health) results(ctx context.Context) map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.last != nil && h.now().Sub(h.checked) < checkCacheTTL {
		return h.last
	}
	res := make(map[string]string, len(h.checks))
	for _, c := range h.checks {
		cctx, cancel := context.WithTimeout(ctx, checkTimeout)
		if err := c.Fn(cctx); err != nil {
			res[c.Name] = "down"
		} else {
			res[c.Name] = "up"
		}
		cancel()
	}
	h.last, h.checked = res, h.now()
	return res
}
