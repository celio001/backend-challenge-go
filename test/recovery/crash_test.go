//go:build integration && faultinject

package recovery

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// Scenario 5: the process dies after the commit and before the message is deleted.
func TestConsumerDiesAfterCommitBeforeDelete(t *testing.T) {
	c := newCluster(t, clusterOpts{visibilitySeconds: 3})
	victim := c.start("victim", map[string]string{"FAULT_INJECT": "after_commit_before_sqs_delete"})
	w := c.openWallet(victim, "1000.00")

	c.sendSQS(w, "crash-msg-1", wagerSpec{ext: "crash-1", kind: "BET", amount: "40.00"})
	if code := victim.waitExit(30 * time.Second); code != 137 {
		t.Fatalf("the victim exited with %d, want 137 (killed at the injected point)\n%s", code, victim.out.String())
	}

	// The money moved and the inbox remembers the message, but the broker never heard that it was handled.
	if c.status("crash-1") != "PROCESSED" || c.balance(w) != "960.00" || c.debits(w) != "1" {
		t.Fatalf("after the crash: status = %s, balance = %s, debits = %s", c.status("crash-1"), c.balance(w), c.debits(w))
	}
	if n := c.scalar(`SELECT count(*)::text FROM inbox_messages WHERE message_id = 'crash-msg-1' AND processed_at IS NOT NULL`); n != "1" {
		t.Fatalf("inbox rows = %s", n)
	}

	// Two healthy replicas appear; the message comes back when its visibility ends.
	survivors := []*proc{c.start("survivor-1", nil), c.start("survivor-2", nil)}
	c.waitInboundIdle()

	if c.balance(w) != "960.00" || c.debits(w) != "1" {
		t.Fatalf("after the redelivery: balance = %s, debits = %s, the redelivered message must not move money again", c.balance(w), c.debits(w))
	}
	if n := c.scalar(`SELECT count(*)::text FROM inbox_messages WHERE message_id = 'crash-msg-1'`); n != "1" {
		t.Fatalf("inbox rows = %s", n)
	}
	var duplicates float64
	for _, p := range survivors {
		duplicates += c.metric(p, `sqs_messages_total{outcome="duplicate"}`)
	}
	if duplicates < 1 {
		t.Fatalf("no survivor reported the redelivery as a duplicate\n%s\n%s", survivors[0].out.String(), survivors[1].out.String())
	}
	c.assertNothingDeadLettered()
}

// Atomicity: the process dies after the work is done and before the commit.
func TestProcessDiesBeforeCommit(t *testing.T) {
	c := newCluster(t, clusterOpts{visibilitySeconds: 3})
	healthy := c.start("healthy", nil)
	w := c.openWallet(healthy, "1000.00")
	victim := c.start("victim", map[string]string{"FAULT_INJECT": "before_commit"})

	ledgerBefore := c.scalar(`SELECT count(*)::text FROM wallet_ledger_entries WHERE wallet_id = $1`, w.id)
	eventsBefore := c.scalar(`SELECT count(*)::text FROM outbox_events WHERE partition_key = $1`, w.id)

	t.Run("an HTTP request: nothing is left behind and the retry applies once", func(t *testing.T) {
		spec := wagerSpec{ext: "atomic-http", kind: "BET", amount: "100.00"}
		if r := c.postWager(victim, w, spec); r.status != -1 {
			t.Fatalf("the victim answered %d %v; it should have died before answering", r.status, r.body)
		}
		if code := victim.waitExit(20 * time.Second); code != 137 {
			t.Fatalf("exit code = %d", code)
		}
		if c.status("atomic-http") != "" || c.balance(w) != "1000.00" {
			t.Fatalf("status = %q, balance = %s: a transaction that never committed left traces", c.status("atomic-http"), c.balance(w))
		}
		if got := c.scalar(`SELECT count(*)::text FROM wallet_ledger_entries WHERE wallet_id = $1`, w.id); got != ledgerBefore {
			t.Fatalf("ledger entries = %s, want %s", got, ledgerBefore)
		}
		if got := c.scalar(`SELECT count(*)::text FROM outbox_events WHERE partition_key = $1`, w.id); got != eventsBefore {
			t.Fatalf("outbox events = %s, want %s: an event escaped without its transaction", got, eventsBefore)
		}

		// The client retries with the same key on a healthy replica.
		r := c.postWager(healthy, w, spec)
		if r.status != http.StatusOK || r.body["idempotentReplay"] != false {
			t.Fatalf("retry = %d %v", r.status, r.body)
		}
		if c.balance(w) != "900.00" || c.debits(w) != "1" {
			t.Fatalf("balance = %s, debits = %s", c.balance(w), c.debits(w))
		}
	})

	t.Run("an SQS message: nothing is left behind, not even the inbox row, and the redelivery applies once", func(t *testing.T) {
		victim2 := c.start("victim-2", map[string]string{"FAULT_INJECT": "before_commit"})
		// Keep the healthy replica from taking the message first: it is stopped until the victim has died.
		healthy.stop()

		c.sendSQS(w, "atomic-msg", wagerSpec{ext: "atomic-sqs", kind: "BET", amount: "50.00"})
		if code := victim2.waitExit(30 * time.Second); code != 137 {
			t.Fatalf("exit code = %d\n%s", code, victim2.out.String())
		}
		if c.status("atomic-sqs") != "" || c.scalar(`SELECT count(*)::text FROM inbox_messages WHERE message_id = 'atomic-msg'`) != "0" {
			t.Fatal("the message left a transaction or an inbox row although its commit never happened")
		}
		if c.balance(w) != "900.00" {
			t.Fatalf("balance = %s", c.balance(w))
		}

		c.start("healthy-2", nil)
		c.waitInboundIdle()
		if c.status("atomic-sqs") != "PROCESSED" || c.balance(w) != "850.00" || c.debits(w) != "2" {
			t.Fatalf("after the redelivery: status = %s, balance = %s, debits = %s", c.status("atomic-sqs"), c.balance(w), c.debits(w))
		}
		c.assertNothingDeadLettered()
	})
}

// Scenario 6: a publisher dies right after publishing, before marking the event; two others compete to take over.
func TestPublisherDiesAfterPublishAndTwoOthersTakeOver(t *testing.T) {
	c := newCluster(t, clusterOpts{outboxLease: "3s"})
	victim := c.start("victim", map[string]string{"FAULT_INJECT": "after_publish_before_mark"})
	w := c.openWallet(victim, "1000.00")

	if code := victim.waitExit(30 * time.Second); code != 137 {
		t.Fatalf("the victim exited with %d, want 137\n%s", code, victim.out.String())
	}
	// Event ids that reached the broker before the crash.
	reached := map[string]bool{}
	for _, m := range c.events.WaitFor(t, 1, 15*time.Second) {
		reached[m.EventID] = true
	}
	if len(reached) == 0 {
		t.Fatal("the victim died before anything reached the broker; the scenario did not happen")
	}
	for id := range reached {
		if c.scalar(`SELECT (published_at IS NULL)::text FROM outbox_events WHERE id = $1`, id) != "true" {
			t.Fatalf("event %s is marked published although the publisher died before marking it", id)
		}
	}

	// Two publishers appear and keep producing events while they compete for the backlog.
	b, cc := c.start("publisher-b", nil), c.start("publisher-c", nil)
	for i := range 6 {
		if r := c.postWager([]*proc{b, cc}[i%2], w, wagerSpec{ext: fmt.Sprintf("pub-%d", i), kind: "BET", amount: "10.00"}); r.status != http.StatusOK {
			t.Fatalf("bet %d: %d %v", i, r.status, r.body)
		}
	}
	c.waitFor("the whole outbox to be published", 40*time.Second, func() bool {
		return c.scalar(`SELECT count(*)::text FROM outbox_events WHERE published_at IS NULL`) == "0"
	})

	total := c.scalar(`SELECT count(*)::text FROM outbox_events`)
	got := c.events.WaitFor(t, 1, 5*time.Second)
	for _, m := range got {
		reached[m.EventID] = true
	}
	if fmt.Sprint(len(reached)) != total {
		t.Fatalf("distinct events at the broker = %d, outbox rows = %s: an event was lost", len(reached), total)
	}
	// What the dead publisher had sent was sent again by a survivor with the same id, which the broker then discarded.
	published := c.metric(b, `outbox_publish_attempts_total{result="published"}`) + c.metric(cc, `outbox_publish_attempts_total{result="published"}`)
	if fmt.Sprint(int(published)) != total {
		t.Fatalf("the survivors published %v events, outbox has %s: the event the victim had already sent was not sent again", published, total)
	}
	for id := range reached {
		if c.scalar(`SELECT count(*)::text FROM outbox_events WHERE id = $1`, id) != "1" {
			t.Fatalf("event %s at the broker has no row in the outbox", id)
		}
	}
}

// Scenario 7 and 8: a reversal comes before its bet, and the replica that claimed it dies before resolving it.
func TestResolverDiesAfterClaimAndAnotherCompletesTheReversal(t *testing.T) {
	c := newCluster(t, clusterOpts{referenceLease: "3s"})
	victim := c.start("victim", map[string]string{"FAULT_INJECT": "after_claim_before_resolve"})
	w := c.openWallet(victim, "100.00")

	early := c.postWager(victim, w, wagerSpec{ext: "refund-1", kind: "REFUND", amount: "30.00", ref: "bet-1"})
	if early.status != http.StatusAccepted || early.body["status"] != "PENDING_REFERENCE" {
		t.Fatalf("early refund = %d %v", early.status, early.body)
	}
	if code := victim.waitExit(30 * time.Second); code != 137 {
		t.Fatalf("the victim exited with %d, want 137\n%s", code, victim.out.String())
	}
	if c.status("refund-1") != "PENDING_REFERENCE" {
		t.Fatalf("status after the crash = %s: the pending operation must survive the process", c.status("refund-1"))
	}

	// Others take over once the victim's lease ends; the bet that the refund waits for arrives at one of them.
	b, cc := c.start("replica-b", nil), c.start("replica-c", nil)
	if r := c.postWager(b, w, wagerSpec{ext: "bet-1", kind: "BET", amount: "30.00"}); r.status != http.StatusOK {
		t.Fatalf("bet = %d %v", r.status, r.body)
	}
	c.waitFor("the refund to be completed by a surviving replica", 30*time.Second, func() bool { return c.status("refund-1") == "PROCESSED" })

	if c.balance(w) != "100.00" {
		t.Fatalf("balance = %s, want the bet refunded exactly once", c.balance(w))
	}
	if n := c.scalar(`SELECT count(*)::text FROM wallet_ledger_entries l JOIN wager_transactions t ON t.id = l.transaction_id WHERE t.external_transaction_id = 'refund-1'`); n != "1" {
		t.Fatalf("ledger entries of the refund = %s", n)
	}
	view := c.do(cc, http.MethodGet, "/providers/provider-a/wagering/transactions/refund-1", c.pa, "", "")
	if view.status != http.StatusOK || view.body["status"] != "PROCESSED" || view.body["attempts"] != nil {
		t.Fatalf("view = %d %v", view.status, view.body)
	}
}

// Scenario 7, expiry: no bet ever comes.
func TestReversalWithoutReferenceIsRejectedAtTheDeadline(t *testing.T) {
	c := newCluster(t, clusterOpts{referenceTTL: "3s"})
	procs := []*proc{c.start("replica-1", nil), c.start("replica-2", nil), c.start("replica-3", nil)}
	w := c.openWallet(procs[0], "100.00")

	early := c.postWager(procs[0], w, wagerSpec{ext: "orphan-1", kind: "ROLLBACK", amount: "10.00", ref: "never-comes"})
	if early.status != http.StatusAccepted {
		t.Fatalf("early rollback = %d %v", early.status, early.body)
	}
	c.waitFor("the deadline rejection", 40*time.Second, func() bool { return c.status("orphan-1") == "REJECTED" })

	view := c.do(procs[2], http.MethodGet, "/providers/provider-a/wagering/transactions/orphan-1", c.pa, "", "")
	if view.body["failureCode"] != "REFERENCE_NOT_FOUND" {
		t.Fatalf("view = %v", view.body)
	}
	if n := c.scalar(`SELECT count(*)::text FROM outbox_events WHERE partition_key = $1 AND event_type = 'WagerTransactionRejected'`, w.id); n != "1" {
		t.Fatalf("rejection events = %s, want exactly one even with three resolvers", n)
	}
	if c.balance(w) != "100.00" {
		t.Fatalf("balance = %s: a rejection must not move money", c.balance(w))
	}
}
