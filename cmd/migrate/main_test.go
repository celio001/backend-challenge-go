package main

import "testing"

func TestRunRejectsBadInput(t *testing.T) {
	tests := []struct {
		name string
		args []string
		url  string
	}{
		{name: "no command", url: "postgres://x"},
		{name: "unknown command", args: []string{"reset"}, url: "postgres://x"},
		{name: "extra args", args: []string{"up", "now"}, url: "postgres://x"},
		{name: "missing database url", args: []string{"up"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := run(tt.args, tt.url); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
