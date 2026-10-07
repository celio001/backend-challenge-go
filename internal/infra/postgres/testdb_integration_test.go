//go:build integration

package postgres

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/celio001/backend-challenge-go/internal/testutil/pgtest"
)

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, _ := pgtest.New(t)
	return pool
}

func newUUID(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
