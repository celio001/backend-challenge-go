package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/celio001/backend-challenge-go/internal/usecase"
)

// classify tags an infrastructure error as transient (retry may succeed) or permanent (it will not), per ARCHITECTURE.md §2.4.
// Errors that carry neither tag, and context cancellation, are returned untouched.
func classify(err error) error {
	if err == nil || errors.Is(err, usecase.ErrTransient) || errors.Is(err, usecase.ErrPermanent) {
		return err
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case isTransientCode(pgErr.Code), pgErr.Code == "23505" && pgErr.ConstraintName == "ux_wtx_one_reversal":
			return fmt.Errorf("%w: %w", usecase.ErrTransient, err)
		case strings.HasPrefix(pgErr.Code, "22"), strings.HasPrefix(pgErr.Code, "23"), strings.HasPrefix(pgErr.Code, "XX"):
			return fmt.Errorf("%w: %w", usecase.ErrPermanent, err)
		}
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err) || pgconn.SafeToRetry(err) {
		return fmt.Errorf("%w: %w", usecase.ErrTransient, err)
	}
	return err
}

func isTransientCode(code string) bool {
	switch code {
	case "40001", "40P01", "55P03", "57P01", "57014":
		return true
	}
	return strings.HasPrefix(code, "08") || strings.HasPrefix(code, "53")
}
