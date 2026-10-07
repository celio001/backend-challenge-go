//go:build integration && faultinject

package recovery

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// Scenario 8: every process stops, gracefully and then abruptly; idempotency, pending work and balances must survive.
func TestRestartingEverythingKeepsIdempotencyPendingWorkAndBalances(t *testing.T) {
	// A short visibility: a long poll abandoned by a stopping consumer may still swallow the next message on the broker side,
	// which then stays invisible for this long. In production that is the same bounded delay, never a loss.
	c := newCluster(t, clusterOpts{visibilitySeconds: 3})
	first := []*proc{c.start("replica-1", nil), c.start("replica-2", nil), c.start("replica-3", nil)}
	w := c.openWallet(first[0], "1000.00")

	original := c.postWager(first[0], w, wagerSpec{ext: "keep-1", kind: "BET", amount: "25.00"})
	if original.status != http.StatusOK {
		t.Fatalf("first bet = %d %v", original.status, original.body)
	}
	if r := c.postWager(first[1], w, wagerSpec{ext: "keep-2", kind: "BET", amount: "100.00"}); r.status != http.StatusOK {
		t.Fatalf("second bet = %d %v", r.status, r.body)
	}
	early := c.postWager(first[2], w, wagerSpec{ext: "keep-refund", kind: "REFUND", amount: "40.00", ref: "keep-late-bet"})
	if early.status != http.StatusAccepted {
		t.Fatalf("early refund = %d %v", early.status, early.body)
	}

	// Everything goes down on SIGTERM: each process must finish what it is doing and leave cleanly.
	for _, p := range first {
		if code := p.stop(); code != 0 {
			t.Fatalf("%s left with %d on SIGTERM\n%s", p.name, code, p.out.String())
		}
		for _, line := range []string{"wager consumer stopped", "outbox relay stopped", "reference resolver stopped", "http server stopped"} {
			if !strings.Contains(p.out.String(), line) {
				t.Errorf("%s did not log %q while shutting down\n%s", p.name, line, p.out.String())
			}
		}
	}
	// A message that arrives while nobody is listening waits in the queue.
	c.sendSQS(w, "keep-msg-down", wagerSpec{ext: "keep-while-down", kind: "BET", amount: "10.00"})

	second := []*proc{c.start("replica-4", nil), c.start("replica-5", nil), c.start("replica-6", nil)}
	c.waitFor("the message sent while everything was down", 30*time.Second, func() bool { return c.status("keep-while-down") == "PROCESSED" })
	c.waitInboundIdle()

	t.Run("idempotency survived: the stored answer comes back, the balance is the one seen at the time", func(t *testing.T) {
		again := c.postWager(second[1], w, wagerSpec{ext: "keep-1", kind: "BET", amount: "25.00"})
		bal, _ := again.body["balance"].(map[string]any)
		if again.status != http.StatusOK || again.body["idempotentReplay"] != true || again.body["transactionId"] != original.body["transactionId"] || bal["amount"] != "975.00" {
			t.Fatalf("replay = %d %v, want the original answer with balance 975.00", again.status, again.body)
		}
		conflict := c.postWager(second[2], w, wagerSpec{ext: "keep-1", kind: "BET", amount: "99.00"})
		if conflict.status != http.StatusConflict {
			t.Fatalf("same key, other content = %d %v", conflict.status, conflict.body)
		}
		if c.balance(w) != "865.00" || c.debits(w) != "3" {
			t.Fatalf("balance = %s, debits = %s", c.balance(w), c.debits(w))
		}
	})

	t.Run("pending work survived and completes once its reference arrives", func(t *testing.T) {
		if c.status("keep-refund") != "PENDING_REFERENCE" {
			t.Fatalf("status = %s", c.status("keep-refund"))
		}
		if r := c.postWager(second[0], w, wagerSpec{ext: "keep-late-bet", kind: "BET", amount: "40.00"}); r.status != http.StatusOK {
			t.Fatalf("late bet = %d %v", r.status, r.body)
		}
		c.waitFor("the refund", 30*time.Second, func() bool { return c.status("keep-refund") == "PROCESSED" })
		if c.balance(w) != "865.00" {
			t.Fatalf("balance = %s, want the 40.00 bet refunded", c.balance(w))
		}
	})

	t.Run("an abrupt stop of every process changes nothing either", func(t *testing.T) {
		ledger := c.scalar(`SELECT count(*)::text FROM wallet_ledger_entries WHERE wallet_id = $1`, w.id)
		for _, p := range second {
			p.kill()
		}
		third := []*proc{c.start("replica-7", nil), c.start("replica-8", nil)}

		again := c.postWager(third[0], w, wagerSpec{ext: "keep-refund", kind: "REFUND", amount: "40.00", ref: "keep-late-bet"})
		if again.status != http.StatusOK || again.body["idempotentReplay"] != true || again.body["status"] != "PROCESSED" {
			t.Fatalf("replay of the resolved refund = %d %v", again.status, again.body)
		}
		if c.balance(w) != "865.00" || c.scalar(`SELECT count(*)::text FROM wallet_ledger_entries WHERE wallet_id = $1`, w.id) != ledger {
			t.Fatalf("balance = %s: something moved after the abrupt restart", c.balance(w))
		}
	})

	t.Run("no event was lost along the way", func(t *testing.T) {
		c.waitFor("the outbox to drain", 40*time.Second, func() bool {
			return c.scalar(`SELECT count(*)::text FROM outbox_events WHERE published_at IS NULL`) == "0"
		})
		total := c.scalar(`SELECT count(*)::text FROM outbox_events`)
		seen := map[string]bool{}
		for _, m := range c.events.WaitFor(t, 1, 10*time.Second) {
			seen[m.EventID] = true
		}
		// Whatever the queue still holds plus what was already drained must equal what the database committed.
		if got := len(seen); got == 0 || got > atoi(t, total) {
			t.Fatalf("events at the broker = %d, outbox rows = %s", got, total)
		}
	})
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	n := 0
	for _, r := range s {
		n = n*10 + int(r-'0')
	}
	return n
}
