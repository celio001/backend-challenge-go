package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/celio001/backend-challenge-go/internal/usecase"
)

func TestClassify(t *testing.T) {
	pg := func(code, constraint string) error {
		return fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: code, ConstraintName: constraint})
	}
	plain := errors.New("something else")

	tests := []struct {
		name          string
		err           error
		wantTransient bool
		wantPermanent bool
	}{
		{name: "nil", err: nil},
		{name: "serialization failure", err: pg("40001", ""), wantTransient: true},
		{name: "deadlock", err: pg("40P01", ""), wantTransient: true},
		{name: "lock timeout", err: pg("55P03", ""), wantTransient: true},
		{name: "statement timeout", err: pg("57014", ""), wantTransient: true},
		{name: "admin shutdown", err: pg("57P01", ""), wantTransient: true},
		{name: "connection exception", err: pg("08006", ""), wantTransient: true},
		{name: "insufficient resources", err: pg("53300", ""), wantTransient: true},
		{name: "second reversal race", err: pg("23505", "ux_wtx_one_reversal"), wantTransient: true},
		{name: "other unique violation", err: pg("23505", "ux_wtx_provider_key"), wantPermanent: true},
		{name: "check violation", err: pg("23514", "wallets_balance_minor_check"), wantPermanent: true},
		{name: "data exception", err: pg("22P02", ""), wantPermanent: true},
		{name: "internal error class", err: pg("XX000", ""), wantPermanent: true},
		{name: "unknown sqlstate", err: pg("42P01", "")},
		{name: "deadline exceeded", err: context.DeadlineExceeded, wantTransient: true},
		{name: "canceled by the caller", err: context.Canceled},
		{name: "plain error", err: plain},
		{name: "already transient", err: fmt.Errorf("%w: x", usecase.ErrTransient), wantTransient: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classify(tt.err)
			if errors.Is(got, usecase.ErrTransient) != tt.wantTransient || errors.Is(got, usecase.ErrPermanent) != tt.wantPermanent {
				t.Fatalf("classify(%v): transient=%v permanent=%v, want %v/%v", tt.err,
					errors.Is(got, usecase.ErrTransient), errors.Is(got, usecase.ErrPermanent), tt.wantTransient, tt.wantPermanent)
			}
			if tt.err != nil && !errors.Is(got, tt.err) {
				t.Fatalf("original error lost: %v", got)
			}
		})
	}
}
