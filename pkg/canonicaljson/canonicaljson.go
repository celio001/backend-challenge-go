// Package canonicaljson renders string-only JSON objects deterministically, so equal content always hashes equally.
package canonicaljson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// Marshal supports string values and nested map[string]any. Keys are sorted, there is no whitespace, and HTML characters are not escaped.
func Marshal(v map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	if err := write(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func write(buf *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case string:
		return writeString(buf, x)
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeString(buf, k); err != nil {
				return err
			}
			buf.WriteByte(':')
			if err := write(buf, x[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
		return nil
	default:
		return fmt.Errorf("canonicaljson: unsupported type %T", v)
	}
}

func writeString(buf *bytes.Buffer, s string) error {
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return err
	}
	// Encode appends a newline.
	buf.Truncate(buf.Len() - 1)
	return nil
}
