package httpapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/celio001/backend-challenge-go/internal/testutil/spantest"
	"github.com/celio001/backend-challenge-go/internal/usecase"
)

const incomingTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func request(e env, method, path, token, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Idempotency-Key", "provider-a:transaction-123")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return rec
}

func TestEveryBusinessRouteHasASpanNamedAfterItsPattern(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		path    string
		token   string
		pattern string
		status  string
	}{
		{name: "an id in the path does not become part of the name", method: http.MethodGet, path: "/wallets/" + walletID, token: "admin", pattern: "GET /wallets/{id}", status: "200"},
		{name: "ledger", method: http.MethodGet, path: "/wallets/" + walletID + "/ledger", token: "admin", pattern: "GET /wallets/{id}/ledger", status: "200"},
		{name: "reconciliation", method: http.MethodPost, path: "/wallets/" + walletID + "/reconciliation", token: "admin", pattern: "POST /wallets/{id}/reconciliation", status: "200"},
		{name: "a refused caller still gets a span, with the 401", method: http.MethodGet, path: "/wallets/" + walletID, pattern: "GET /wallets/{id}", status: "401"},
		{name: "a forbidden caller", method: http.MethodGet, path: "/wallets/" + walletID, token: "provider", pattern: "GET /wallets/{id}", status: "403"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := spantest.Install(t)
			e := newEnv(t)
			request(e, tt.method, tt.path, tt.token, "", map[string]string{"X-Correlation-Id": "corr-7"})

			spans := rec.Named(tt.pattern)
			if len(spans) != 1 {
				t.Fatalf("spans named %q = %d (all: %v)", tt.pattern, len(spans), rec.Ended())
			}
			attrs := spantest.Attrs(spans[0])
			if attrs["http.route"] != tt.pattern || attrs["http.request.method"] != tt.method || attrs["http.response.status_code"] != tt.status || attrs["correlation.id"] != "corr-7" {
				t.Fatalf("attributes = %v", attrs)
			}
			if spans[0].SpanKind() != trace.SpanKindServer {
				t.Fatalf("kind = %v", spans[0].SpanKind())
			}
			if spans[0].Status().Code == codes.Error {
				t.Fatalf("a %s is the caller's problem and must not mark the span as an error", tt.status)
			}
			if strings.Contains(strings.Join(mapValues(attrs), " "), "Bearer") || strings.Contains(strings.Join(mapValues(attrs), " "), walletID) {
				t.Fatalf("the span carries request data: %v", attrs)
			}
		})
	}
}

func mapValues(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

func TestTheCallersTraceContinuesThroughTheService(t *testing.T) {
	rec := spantest.Install(t)
	e := newEnv(t)
	request(e, http.MethodGet, "/wallets/"+walletID, "admin", "", map[string]string{"traceparent": incomingTraceparent})

	spans := rec.Named("GET /wallets/{id}")
	if len(spans) != 1 {
		t.Fatalf("spans = %d", len(spans))
	}
	sc, parent := spans[0].SpanContext(), spans[0].Parent()
	if sc.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" || parent.SpanID().String() != "00f067aa0ba902b7" || !parent.IsRemote() {
		t.Fatalf("trace = %s, parent = %s (remote %v)", sc.TraceID(), parent.SpanID(), parent.IsRemote())
	}
}

func TestOnlyServerFailuresMarkTheSpanAsAnError(t *testing.T) {
	rec := spantest.Install(t)
	e := newEnv(t)
	e.process.err = errors.New("boom")
	r := request(e, http.MethodPost, "/wagering/transactions", "provider", betBody, nil)
	if r.Code != http.StatusInternalServerError {
		t.Fatalf("an unexpected failure was answered with %d, want 500", r.Code)
	}
	spans := rec.Named("POST /wagering/transactions")
	if len(spans) != 1 || spans[0].Status().Code != codes.Error || spantest.Attrs(spans[0])["http.response.status_code"] != "500" {
		t.Fatalf("spans = %v", spans)
	}

	rec2 := spantest.Install(t)
	e2 := newEnv(t)
	e2.process.err = usecase.ErrTransient
	if r := request(e2, http.MethodPost, "/wagering/transactions", "provider", betBody, nil); r.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", r.Code)
	}
	if spans := rec2.Named("POST /wagering/transactions"); len(spans) != 1 || spans[0].Status().Code != codes.Error {
		t.Fatalf("a 503 must mark the span as an error: %v", spans)
	}
}

func TestHealthAndMetricsAreNotTraced(t *testing.T) {
	rec := spantest.Install(t)
	e := newEnv(t)
	for _, path := range []string{"/health/live", "/health/ready"} {
		request(e, http.MethodGet, path, "", "", nil)
	}
	if got := rec.Ended(); len(got) != 0 {
		t.Fatalf("polled endpoints produced %d spans", len(got))
	}
}
