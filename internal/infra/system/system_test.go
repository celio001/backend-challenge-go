package system

import (
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/pkg/uuid"
)

func TestIDs(t *testing.T) {
	seen := make(map[string]bool)
	for range 1000 {
		id := IDs{}.NewID()
		if !uuid.Valid(id) {
			t.Fatalf("invalid uuid %q", id)
		}
		if id[14] != '7' {
			t.Fatalf("not a v7 uuid: %q", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

func TestClockIsUTC(t *testing.T) {
	now := Clock{}.Now()
	if now.Location() != time.UTC || time.Since(now) > time.Second {
		t.Fatalf("now = %v", now)
	}
}
