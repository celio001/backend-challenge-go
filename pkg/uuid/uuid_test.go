package uuid

import "testing"

func TestValid(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{name: "v7", in: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", want: true},
		{name: "nil uuid", in: "00000000-0000-0000-0000-000000000000", want: true},
		{name: "empty", in: ""},
		{name: "uppercase", in: "0192F28F-5DC0-7D58-BDB2-814AD6A0F4A1"},
		{name: "no dashes", in: "0192f28f5dc07d58bdb2814ad6a0f4a1"},
		{name: "too short", in: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a"},
		{name: "too long", in: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a11"},
		{name: "non hex", in: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4ag"},
		{name: "misplaced dash", in: "0192f28f5-dc0-7d58-bdb2-814ad6a0f4a1"},
		{name: "braces", in: "{0192f28f-5dc0-7d58-bdb2-814ad6a0f4a}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Valid(tt.in); got != tt.want {
				t.Fatalf("Valid(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
