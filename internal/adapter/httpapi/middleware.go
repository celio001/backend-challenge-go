package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strings"

	"github.com/celio001/backend-challenge-go/internal/auth"
	"github.com/celio001/backend-challenge-go/internal/usecase"
)

const correlationHeader = "X-Correlation-Id"

// Only a conservative alphabet is echoed back, since the value ends up in headers and logs.
var validCorrelationID = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

type correlationKey struct{}

func correlationIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

func withCorrelation(ids usecase.IDGenerator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(correlationHeader)
		if !validCorrelationID.MatchString(id) {
			id = ids.NewID()
		}
		w.Header().Set(correlationHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), correlationKey{}, id)))
	})
}

// authenticate runs before any other work so a rejected caller causes no database access.
func authenticate(v auth.Verifier, log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
			unauthenticated(w)
			return
		}
		p, err := v.Verify(r.Context(), strings.TrimSpace(token))
		if err != nil {
			if !errors.Is(err, auth.ErrUnauthenticated) {
				log.Error("token verification failed", "correlationId", correlationIDFrom(r.Context()), "error", err)
			}
			unauthenticated(w)
			return
		}
		next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
	})
}

func requireRole(role string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p, ok := auth.PrincipalFrom(r.Context()); !ok || !p.HasRole(role) {
			writeProblem(w, errForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func requireAnyRole(roles []string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p, ok := auth.PrincipalFrom(r.Context()); ok {
			for _, role := range roles {
				if p.HasRole(role) {
					next.ServeHTTP(w, r)
					return
				}
			}
		}
		writeProblem(w, errForbidden)
	})
}

func unauthenticated(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="wallet"`)
	writeProblem(w, errUnauthenticated)
}
