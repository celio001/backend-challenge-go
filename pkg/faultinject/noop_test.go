//go:build !faultinject

package faultinject

import (
	"os"
	"testing"
)

// A production binary must ignore the variable entirely: setting it can never kill the process.
func TestHitDoesNothingWithoutTheTag(t *testing.T) {
	t.Setenv("FAULT_INJECT", "after_commit_before_sqs_delete,after_publish_before_mark")
	Hit("after_commit_before_sqs_delete")
	Hit("after_publish_before_mark")
	if os.Getenv("FAULT_INJECT") == "" {
		t.Fatal("test setup")
	}
}
