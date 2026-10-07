//go:build !faultinject

// Package faultinject kills the process at named points, to test recovery from the worst possible moment.
// It exists only in binaries built with the faultinject tag; without it Hit is an empty function.
package faultinject

// Hit does nothing in a normal build; the compiler removes the call.
func Hit(string) {}
