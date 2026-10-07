//go:build faultinject

package faultinject

import "testing"

func TestPlan(t *testing.T) {
	tests := []struct {
		name      string
		spec      string
		hits      []string
		wantExits []int // exit codes, in order
	}{
		{name: "no plan never exits", spec: "", hits: []string{"a", "a"}},
		{name: "a point fires on its first hit", spec: "a", hits: []string{"b", "a", "a"}, wantExits: []int{ExitCode}},
		{name: "the nth hit fires, the others do not", spec: "a:3", hits: []string{"a", "a", "a", "a"}, wantExits: []int{ExitCode}},
		{name: "points count separately", spec: "a:2,b:1", hits: []string{"a", "b", "a"}, wantExits: []int{ExitCode, ExitCode}},
		{name: "spaces around items are ignored", spec: " a , b:2 ", hits: []string{"a", "b", "b"}, wantExits: []int{ExitCode, ExitCode}},
		{name: "a malformed count is ignored", spec: "a:zero,b:0,c:-1", hits: []string{"a", "b", "c"}},
		{name: "empty items are ignored", spec: ",,a,", hits: []string{"a"}, wantExits: []int{ExitCode}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var exits []int
			p := newPlan(tt.spec, func(code int) { exits = append(exits, code) })
			for _, h := range tt.hits {
				p.hit(h)
			}
			if len(exits) != len(tt.wantExits) {
				t.Fatalf("exits = %v, want %v", exits, tt.wantExits)
			}
			for i := range exits {
				if exits[i] != tt.wantExits[i] {
					t.Fatalf("exits = %v, want %v", exits, tt.wantExits)
				}
			}
		})
	}
}
