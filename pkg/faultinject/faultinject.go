//go:build faultinject

// Package faultinject kills the process at named points, to test recovery from the worst possible moment.
// It exists only in binaries built with the faultinject tag; without it Hit is an empty function.
package faultinject

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Env names the points to trigger, as a comma-separated list: "point" fires on the first hit, "point:3" on the third.
const Env = "FAULT_INJECT"

// ExitCode is what a SIGKILLed process reports, so a crash here looks like the one the orchestrator would cause.
const ExitCode = 137

type plan struct {
	mu     sync.Mutex
	target map[string]int
	hits   map[string]int
	exit   func(int)
}

var active = newPlan(os.Getenv(Env), os.Exit)

func newPlan(spec string, exit func(int)) *plan {
	p := &plan{target: map[string]int{}, hits: map[string]int{}, exit: exit}
	for _, item := range strings.Split(spec, ",") {
		name, nth, hasNth := strings.Cut(strings.TrimSpace(item), ":")
		if name == "" {
			continue
		}
		n := 1
		if hasNth {
			parsed, err := strconv.Atoi(nth)
			if err != nil || parsed < 1 {
				continue
			}
			n = parsed
		}
		p.target[name] = n
	}
	return p
}

// Hit marks a point in the code; when the plan asks for it, the process exits at once, without running deferred calls.
func Hit(point string) { active.hit(point) }

func (p *plan) hit(point string) {
	p.mu.Lock()
	want, ok := p.target[point]
	if !ok {
		p.mu.Unlock()
		return
	}
	p.hits[point]++
	fire := p.hits[point] == want
	p.mu.Unlock()
	if fire {
		fmt.Fprintf(os.Stderr, "faultinject: exiting at %q\n", point)
		p.exit(ExitCode)
	}
}
