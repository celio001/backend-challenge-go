package canonicaljson

import "testing"

func TestMarshal(t *testing.T) {
	tests := []struct {
		name    string
		in      map[string]any
		want    string
		wantErr bool
	}{
		{name: "empty", in: map[string]any{}, want: `{}`},
		{name: "keys are sorted", in: map[string]any{"b": "2", "a": "1", "c": "3"}, want: `{"a":"1","b":"2","c":"3"}`},
		{
			name: "nested objects are sorted too",
			in:   map[string]any{"money": map[string]any{"currency": "BRL", "amount": "25.00"}, "kind": "BET"},
			want: `{"kind":"BET","money":{"amount":"25.00","currency":"BRL"}}`,
		},
		{name: "html is not escaped", in: map[string]any{"a": "<b>&</b>"}, want: `{"a":"<b>&</b>"}`},
		{name: "quotes and control characters are escaped", in: map[string]any{"a": "x\"y\n"}, want: `{"a":"x\"y\n"}`},
		{name: "unicode is kept as UTF-8", in: map[string]any{"a": "ação"}, want: `{"a":"ação"}`},
		{name: "numbers are rejected", in: map[string]any{"a": 1}, wantErr: true},
		{name: "nil is rejected", in: map[string]any{"a": nil}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Marshal(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && string(got) != tt.want {
				t.Fatalf("got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestMarshalIsIndependentOfInsertionOrder(t *testing.T) {
	a := map[string]any{"x": "1", "y": "2", "z": map[string]any{"p": "1", "q": "2"}}
	b := map[string]any{"z": map[string]any{"q": "2", "p": "1"}, "y": "2", "x": "1"}
	for range 50 {
		ga, _ := Marshal(a)
		gb, _ := Marshal(b)
		if string(ga) != string(gb) {
			t.Fatalf("%s != %s", ga, gb)
		}
	}
}
