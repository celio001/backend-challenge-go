//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"

	"github.com/celio001/backend-challenge-go/internal/testutil/spantest"
	"github.com/celio001/backend-challenge-go/internal/usecase"
)

func TestEveryTransactionHasASpanThatSeparatesBusinessRollbacksFromFailures(t *testing.T) {
	pool := newTestPool(t)
	uow := NewUnitOfWork(pool)

	tests := []struct {
		name         string
		fn           func(ctx context.Context, tx usecase.Repos) error
		wantError    bool
		wantRollback bool
	}{
		{name: "a commit", fn: func(context.Context, usecase.Repos) error { return nil }},
		{name: "a rollback because the business said no", fn: func(context.Context, usecase.Repos) error { return errors.New("insufficient funds") }, wantRollback: true},
		{name: "a transient database failure", fn: func(context.Context, usecase.Repos) error { return fmt.Errorf("%w: deadlock", usecase.ErrTransient) }, wantError: true, wantRollback: true},
		{name: "a permanent database failure", fn: func(context.Context, usecase.Repos) error { return fmt.Errorf("%w: corrupt", usecase.ErrPermanent) }, wantError: true, wantRollback: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := spantest.Install(t)
			_ = uow.Do(context.Background(), tt.fn)

			spans := rec.Named("db.transaction")
			if len(spans) != 1 {
				t.Fatalf("spans = %d", len(spans))
			}
			if got := spans[0].Status().Code == codes.Error; got != tt.wantError {
				t.Fatalf("error status = %v, want %v", spans[0].Status(), tt.wantError)
			}
			if _, rolledBack := spantest.Attrs(spans[0])["db.rolled_back"]; rolledBack != (tt.wantRollback && !tt.wantError) {
				t.Fatalf("db.rolled_back = %v for %s", rolledBack, tt.name)
			}
		})
	}

	t.Run("the transaction span is a child of the caller's span", func(t *testing.T) {
		rec := spantest.Install(t)
		ctx, parent := otel.Tracer("test").Start(context.Background(), "wager.execute")
		_ = uow.Do(ctx, func(context.Context, usecase.Repos) error { return nil })
		parent.End()
		spans := rec.Named("db.transaction")
		if len(spans) != 1 || spans[0].Parent().SpanID() != parent.SpanContext().SpanID() {
			t.Fatal("db.transaction is not a child of the caller's span")
		}
	})
}
