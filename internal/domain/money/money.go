package money

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
)

var (
	ErrUninitialized    = errors.New("money: uninitialized value")
	ErrInvalidAmount    = errors.New("money: invalid amount")
	ErrInvalidCurrency  = errors.New("money: invalid currency")
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	ErrOverflow         = errors.New("money: overflow")
	ErrNegative         = errors.New("money: negative amount")
	ErrNotPositive      = errors.New("money: amount must be greater than zero")
)

type Currency string

const (
	BRL Currency = "BRL"
	USD Currency = "USD"
	EUR Currency = "EUR"
)

func (c Currency) valid() bool {
	return c == BRL || c == USD || c == EUR
}

// Minor units in int64 with fixed scale 2. math.MinInt64 is never representable so Neg cannot overflow.
type Money struct {
	minor    int64
	currency Currency
}

func NewCurrency(code string) (Currency, error) {
	c := Currency(code)
	if !c.valid() {
		return "", fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
	}
	return c, nil
}

func Zero(c Currency) (Money, error) {
	return FromMinor(0, c)
}

func FromMinor(minor int64, c Currency) (Money, error) {
	if !c.valid() {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, string(c))
	}
	if minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: minor, currency: c}, nil
}

// Parse accepts a signed amount so internal differences round-trip; external input must use the policy variants.
func Parse(amount, currency string) (Money, error) {
	c, err := NewCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	minor, err := parseMinor(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: c}, nil
}

func ParseNonNegative(amount, currency string) (Money, error) {
	m, err := Parse(amount, currency)
	if err != nil {
		return Money{}, err
	}
	if m.minor < 0 {
		return Money{}, ErrNegative
	}
	return m, nil
}

func ParsePositive(amount, currency string) (Money, error) {
	m, err := ParseNonNegative(amount, currency)
	if err != nil {
		return Money{}, err
	}
	if m.minor == 0 {
		return Money{}, ErrNotPositive
	}
	return m, nil
}

func parseMinor(s string) (int64, error) {
	neg := false
	if len(s) > 0 && s[0] == '-' {
		neg = true
		s = s[1:]
	}
	if len(s) < 4 || s[len(s)-3] != '.' {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	intPart, frac := s[:len(s)-3], s[len(s)-2:]
	if intPart[0] == '0' && len(intPart) > 1 {
		return 0, fmt.Errorf("%w: leading zero", ErrInvalidAmount)
	}

	var acc uint64
	for _, digits := range [2]string{intPart, frac} {
		for i := 0; i < len(digits); i++ {
			d := digits[i]
			if d < '0' || d > '9' {
				return 0, fmt.Errorf("%w: unexpected character", ErrInvalidAmount)
			}
			if acc > (math.MaxInt64-uint64(d-'0'))/10 {
				return 0, ErrOverflow
			}
			acc = acc*10 + uint64(d-'0')
		}
	}
	if neg && acc == 0 {
		return 0, fmt.Errorf("%w: negative zero", ErrInvalidAmount)
	}
	if neg {
		return -int64(acc), nil
	}
	return int64(acc), nil
}

func (m Money) Minor() int64       { return m.minor }
func (m Money) Currency() Currency { return m.currency }
func (m Money) IsValid() bool      { return m.currency.valid() }
func (m Money) IsZero() bool       { return m.IsValid() && m.minor == 0 }
func (m Money) IsPositive() bool   { return m.IsValid() && m.minor > 0 }
func (m Money) IsNegative() bool   { return m.IsValid() && m.minor < 0 }

func (m Money) Add(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) ||
		(o.minor < 0 && m.minor <= math.MinInt64-o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

func (m Money) Sub(o Money) (Money, error) {
	neg, err := o.Neg()
	if err != nil {
		return Money{}, err
	}
	return m.Add(neg)
}

func (m Money) Neg() (Money, error) {
	if !m.IsValid() {
		return Money{}, ErrUninitialized
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

func (m Money) Cmp(o Money) (int, error) {
	if err := m.compatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	}
	return 0, nil
}

func (m Money) compatible(o Money) error {
	if !m.IsValid() || !o.IsValid() {
		return ErrUninitialized
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

func (m Money) String() string {
	mag := uint64(m.minor)
	sign := ""
	if m.minor < 0 {
		mag = uint64(-m.minor)
		sign = "-"
	}
	return sign + strconv.FormatUint(mag/100, 10) + "." + fmt.Sprintf("%02d", mag%100)
}

type wire struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func (m Money) MarshalJSON() ([]byte, error) {
	if !m.IsValid() {
		return nil, ErrUninitialized
	}
	return json.Marshal(wire{Amount: m.String(), Currency: string(m.currency)})
}

// UnmarshalJSON rejects a non-string amount so a JSON number is never decoded as a float.
func (m *Money) UnmarshalJSON(b []byte) error {
	var w wire
	if err := json.Unmarshal(b, &w); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidAmount, err)
	}
	parsed, err := Parse(w.Amount, w.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
