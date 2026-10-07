package money

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name      string
		amount    string
		currency  string
		wantMinor int64
		wantErr   error
	}{
		{"zero", "0.00", "BRL", 0, nil},
		{"one cent", "0.01", "BRL", 1, nil},
		{"typical", "25.00", "BRL", 2500, nil},
		{"cents", "25.99", "USD", 2599, nil},
		{"large", "1000000.00", "EUR", 100000000, nil},
		{"max", "92233720368547758.07", "BRL", math.MaxInt64, nil},
		{"negative", "-5.00", "BRL", -500, nil},
		{"negative max", "-92233720368547758.07", "BRL", -math.MaxInt64, nil},
		{"overflow by one cent", "92233720368547758.08", "BRL", 0, ErrOverflow},
		{"overflow far", "999999999999999999999.99", "BRL", 0, ErrOverflow},
		{"negative overflow", "-92233720368547758.08", "BRL", 0, ErrOverflow},
		{"empty", "", "BRL", 0, ErrInvalidAmount},
		{"only sign", "-", "BRL", 0, ErrInvalidAmount},
		{"NaN", "NaN", "BRL", 0, ErrInvalidAmount},
		{"Infinity", "Infinity", "BRL", 0, ErrInvalidAmount},
		{"-Infinity", "-Infinity", "BRL", 0, ErrInvalidAmount},
		{"scientific", "1e3", "BRL", 0, ErrInvalidAmount},
		{"scientific with scale", "1.00e3", "BRL", 0, ErrInvalidAmount},
		{"no decimals", "25", "BRL", 0, ErrInvalidAmount},
		{"one decimal", "25.0", "BRL", 0, ErrInvalidAmount},
		{"three decimals", "25.001", "BRL", 0, ErrInvalidAmount},
		{"three decimals zeros", "25.000", "BRL", 0, ErrInvalidAmount},
		{"comma separator", "25,00", "BRL", 0, ErrInvalidAmount},
		{"no integer part", ".50", "BRL", 0, ErrInvalidAmount},
		{"leading zero", "05.00", "BRL", 0, ErrInvalidAmount},
		{"double zero", "00.00", "BRL", 0, ErrInvalidAmount},
		{"plus sign", "+5.00", "BRL", 0, ErrInvalidAmount},
		{"negative zero", "-0.00", "BRL", 0, ErrInvalidAmount},
		{"spaces", " 25.00", "BRL", 0, ErrInvalidAmount},
		{"trailing space", "25.00 ", "BRL", 0, ErrInvalidAmount},
		{"letters", "ab.cd", "BRL", 0, ErrInvalidAmount},
		{"hex", "0x1.00", "BRL", 0, ErrInvalidAmount},
		{"thousands separator", "1,000.00", "BRL", 0, ErrInvalidAmount},
		{"unicode digit", "٣.00", "BRL", 0, ErrInvalidAmount},
		{"empty currency", "1.00", "", 0, ErrInvalidCurrency},
		{"lowercase currency", "1.00", "brl", 0, ErrInvalidCurrency},
		{"unsupported currency", "1.00", "JPY", 0, ErrInvalidCurrency},
		{"long currency", "1.00", "BRLL", 0, ErrInvalidCurrency},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.amount, tt.currency)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if got.Minor() != tt.wantMinor || string(got.Currency()) != tt.currency {
				t.Fatalf("got %d %s, want %d %s", got.Minor(), got.Currency(), tt.wantMinor, tt.currency)
			}
		})
	}
}

func TestParseByPolicy(t *testing.T) {
	tests := []struct {
		name         string
		amount       string
		wantPositive error
		wantNonNeg   error
	}{
		{"positive", "10.00", nil, nil},
		{"zero", "0.00", ErrNotPositive, nil},
		{"negative", "-1.00", ErrNegative, ErrNegative},
		{"invalid", "1e3", ErrInvalidAmount, ErrInvalidAmount},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParsePositive(tt.amount, "BRL"); !errors.Is(err, tt.wantPositive) {
				t.Errorf("ParsePositive err = %v, want %v", err, tt.wantPositive)
			}
			if _, err := ParseNonNegative(tt.amount, "BRL"); !errors.Is(err, tt.wantNonNeg) {
				t.Errorf("ParseNonNegative err = %v, want %v", err, tt.wantNonNeg)
			}
		})
	}
}

func TestFromMinorAndZero(t *testing.T) {
	tests := []struct {
		name     string
		minor    int64
		currency Currency
		wantErr  error
	}{
		{"ok", 2500, BRL, nil},
		{"negative ok", -2500, USD, nil},
		{"max", math.MaxInt64, EUR, nil},
		{"min rejected", math.MinInt64, BRL, ErrOverflow},
		{"invalid currency", 1, "XXX", ErrInvalidCurrency},
		{"empty currency", 1, "", ErrInvalidCurrency},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := FromMinor(tt.minor, tt.currency)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil && (m.Minor() != tt.minor || m.Currency() != tt.currency) {
				t.Fatalf("got %+v", m)
			}
		})
	}

	z, err := Zero(BRL)
	if err != nil || !z.IsZero() || z.Currency() != BRL {
		t.Fatalf("Zero(BRL) = %+v, %v", z, err)
	}
	if _, err := Zero("XXX"); !errors.Is(err, ErrInvalidCurrency) {
		t.Fatalf("Zero(XXX) err = %v", err)
	}
}

func mustMinor(t *testing.T, minor int64, c Currency) Money {
	t.Helper()
	m, err := FromMinor(minor, c)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestAddSub(t *testing.T) {
	tests := []struct {
		name    string
		a, b    Money
		op      string
		want    int64
		wantErr error
	}{
		{"add", mustMinor(t, 1000, BRL), mustMinor(t, 250, BRL), "add", 1250, nil},
		{"add negative", mustMinor(t, 1000, BRL), mustMinor(t, -250, BRL), "add", 750, nil},
		{"add to zero", mustMinor(t, 1000, BRL), mustMinor(t, -1000, BRL), "add", 0, nil},
		{"add max overflow", mustMinor(t, math.MaxInt64, BRL), mustMinor(t, 1, BRL), "add", 0, ErrOverflow},
		{"add min overflow", mustMinor(t, -math.MaxInt64, BRL), mustMinor(t, -1, BRL), "add", 0, ErrOverflow},
		{"add currency mismatch", mustMinor(t, 1, BRL), mustMinor(t, 1, USD), "add", 0, ErrCurrencyMismatch},
		{"add uninitialized left", Money{}, mustMinor(t, 1, BRL), "add", 0, ErrUninitialized},
		{"add uninitialized right", mustMinor(t, 1, BRL), Money{}, "add", 0, ErrUninitialized},
		{"sub", mustMinor(t, 1000, BRL), mustMinor(t, 250, BRL), "sub", 750, nil},
		{"sub below zero", mustMinor(t, 100, BRL), mustMinor(t, 250, BRL), "sub", -150, nil},
		{"sub max overflow", mustMinor(t, math.MaxInt64, BRL), mustMinor(t, -1, BRL), "sub", 0, ErrOverflow},
		{"sub min overflow", mustMinor(t, -math.MaxInt64, BRL), mustMinor(t, 1, BRL), "sub", 0, ErrOverflow},
		{"sub currency mismatch", mustMinor(t, 1, EUR), mustMinor(t, 1, BRL), "sub", 0, ErrCurrencyMismatch},
		{"sub uninitialized", Money{}, Money{}, "sub", 0, ErrUninitialized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Money
			var err error
			if tt.op == "add" {
				got, err = tt.a.Add(tt.b)
			} else {
				got, err = tt.a.Sub(tt.b)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil && got.Minor() != tt.want {
				t.Fatalf("got %d, want %d", got.Minor(), tt.want)
			}
		})
	}
}

func TestNeg(t *testing.T) {
	tests := []struct {
		name    string
		in      Money
		want    int64
		wantErr error
	}{
		{"positive", mustMinor(t, 500, BRL), -500, nil},
		{"negative", mustMinor(t, -500, BRL), 500, nil},
		{"zero", mustMinor(t, 0, BRL), 0, nil},
		{"max", mustMinor(t, math.MaxInt64, BRL), -math.MaxInt64, nil},
		{"uninitialized", Money{}, 0, ErrUninitialized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.in.Neg()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil && got.Minor() != tt.want {
				t.Fatalf("got %d, want %d", got.Minor(), tt.want)
			}
		})
	}
}

func TestCmp(t *testing.T) {
	tests := []struct {
		name    string
		a, b    Money
		want    int
		wantErr error
	}{
		{"less", mustMinor(t, 1, BRL), mustMinor(t, 2, BRL), -1, nil},
		{"equal", mustMinor(t, 2, BRL), mustMinor(t, 2, BRL), 0, nil},
		{"greater", mustMinor(t, 3, BRL), mustMinor(t, 2, BRL), 1, nil},
		{"negative less", mustMinor(t, -3, BRL), mustMinor(t, 2, BRL), -1, nil},
		{"mismatch", mustMinor(t, 1, BRL), mustMinor(t, 1, USD), 0, ErrCurrencyMismatch},
		{"uninitialized", Money{}, mustMinor(t, 1, BRL), 0, ErrUninitialized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.a.Cmp(tt.b)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestString(t *testing.T) {
	tests := []struct {
		minor int64
		want  string
	}{
		{0, "0.00"},
		{1, "0.01"},
		{10, "0.10"},
		{2500, "25.00"},
		{2599, "25.99"},
		{-500, "-5.00"},
		{-1, "-0.01"},
		{math.MaxInt64, "92233720368547758.07"},
		{-math.MaxInt64, "-92233720368547758.07"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := mustMinor(t, tt.minor, BRL).String(); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseStringRoundTrip(t *testing.T) {
	for _, s := range []string{"0.00", "0.05", "1.00", "99.99", "1234567.89", "-42.10", "92233720368547758.07"} {
		m, err := Parse(s, "BRL")
		if err != nil {
			t.Fatalf("%s: %v", s, err)
		}
		if m.String() != s {
			t.Errorf("round trip %q -> %q", s, m.String())
		}
	}
}

func TestPredicates(t *testing.T) {
	tests := []struct {
		name                       string
		m                          Money
		valid, zero, positive, neg bool
	}{
		{"uninitialized", Money{}, false, false, false, false},
		{"zero", mustMinor(t, 0, BRL), true, true, false, false},
		{"positive", mustMinor(t, 1, BRL), true, false, true, false},
		{"negative", mustMinor(t, -1, BRL), true, false, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.m.IsValid() != tt.valid || tt.m.IsZero() != tt.zero ||
				tt.m.IsPositive() != tt.positive || tt.m.IsNegative() != tt.neg {
				t.Fatalf("predicates mismatch for %+v", tt.m)
			}
		})
	}
}

func TestMarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		in      Money
		want    string
		wantErr error
	}{
		{"ok", mustMinor(t, 2500, BRL), `{"amount":"25.00","currency":"BRL"}`, nil},
		{"negative", mustMinor(t, -5, USD), `{"amount":"-0.05","currency":"USD"}`, nil},
		{"uninitialized", Money{}, "", ErrUninitialized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil && string(b) != tt.want {
				t.Fatalf("got %s, want %s", b, tt.want)
			}
		})
	}
}

func TestUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    int64
		wantErr error
	}{
		{"ok", `{"amount":"25.00","currency":"BRL"}`, 2500, nil},
		{"number amount", `{"amount":25.00,"currency":"BRL"}`, 0, ErrInvalidAmount},
		{"integer amount", `{"amount":25,"currency":"BRL"}`, 0, ErrInvalidAmount},
		{"null amount", `{"amount":null,"currency":"BRL"}`, 0, ErrInvalidAmount},
		{"missing amount", `{"currency":"BRL"}`, 0, ErrInvalidAmount},
		{"missing currency", `{"amount":"1.00"}`, 0, ErrInvalidCurrency},
		{"bad scale", `{"amount":"1.0","currency":"BRL"}`, 0, ErrInvalidAmount},
		{"bad currency", `{"amount":"1.00","currency":"XXX"}`, 0, ErrInvalidCurrency},
		{"not an object", `"25.00"`, 0, ErrInvalidAmount},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m Money
			err := json.Unmarshal([]byte(tt.in), &m)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil && m.Minor() != tt.want {
				t.Fatalf("got %d, want %d", m.Minor(), tt.want)
			}
			if err != nil && m.IsValid() {
				t.Fatal("value must stay uninitialized on error")
			}
		})
	}
}
