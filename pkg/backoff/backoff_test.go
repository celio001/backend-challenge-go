package backoff

import (
	"testing"
	"time"
)

func TestDelayWithoutJitter(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second}, {1, 2 * time.Second}, {2, 4 * time.Second}, {5, 32 * time.Second},
		{6, time.Minute}, {7, time.Minute}, {100, time.Minute}, {-1, time.Second},
	}
	for _, tt := range tests {
		if got := Delay(tt.attempt, time.Second, time.Minute, 0); got != tt.want {
			t.Errorf("Delay(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestDelayJitterStaysWithinBounds(t *testing.T) {
	for range 1000 {
		got := Delay(3, time.Second, time.Minute, 0.2)
		if got < 6400*time.Millisecond || got > 9600*time.Millisecond {
			t.Fatalf("Delay = %v, outside ±20%% of 8s", got)
		}
	}
}
