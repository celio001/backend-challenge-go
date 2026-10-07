// Package backoff computes exponential retry delays with jitter.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Delay returns min(base*2^attempt, max) spread by ±jitter (0.2 means ±20%). attempt starts at 0.
func Delay(attempt int, base, max time.Duration, jitter float64) time.Duration {
	d := base
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	if jitter <= 0 {
		return d
	}
	spread := (rand.Float64()*2 - 1) * jitter
	return time.Duration(float64(d) * (1 + spread))
}
