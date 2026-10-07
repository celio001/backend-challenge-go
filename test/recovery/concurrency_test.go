//go:build integration && faultinject

package recovery

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// waitInboundIdle waits until the inbound queue holds nothing, visible or in flight, for three polls in a row.
func (c *cluster) waitInboundIdle() {
	c.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	quiet := 0
	for quiet < 3 {
		if time.Now().After(deadline) {
			c.t.Fatal("the inbound queue did not drain")
		}
		out, err := c.inbound.Client.GetQueueAttributes(context.Background(), &awssqs.GetQueueAttributesInput{
			QueueUrl: aws.String(c.inbound.URL),
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible, types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
			},
		})
		if err != nil {
			c.t.Fatal(err)
		}
		if out.Attributes["ApproximateNumberOfMessages"] == "0" && out.Attributes["ApproximateNumberOfMessagesNotVisible"] == "0" && out.Attributes["ApproximateNumberOfMessagesDelayed"] == "0" {
			quiet++
		} else {
			quiet = 0
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func (c *cluster) assertNothingDeadLettered() {
	c.t.Helper()
	if got := c.dlq.Drain(c.t); len(got) != 0 {
		c.t.Fatalf("dead-lettered messages: %+v", got)
	}
}

// Scenario 1 and 4: the same bet 50 times at once, spread over three processes and both channels.
func TestSameBetFiftyTimesAcrossThreeProcessesAndBothChannels(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	procs := []*proc{c.start("replica-1", nil), c.start("replica-2", nil), c.start("replica-3", nil)}
	w := c.openWallet(procs[0], "1000.00")
	spec := wagerSpec{ext: "dup-1", kind: "BET", amount: "25.00"}

	const perChannel = 25
	var wg sync.WaitGroup
	var firsts, replays, failed atomic.Int32
	gate := make(chan struct{})
	for i := range perChannel {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-gate
			r := c.postWager(procs[i%len(procs)], w, spec)
			switch {
			case r.status == http.StatusOK && r.body["idempotentReplay"] == false:
				firsts.Add(1)
			case r.status == http.StatusOK && r.body["idempotentReplay"] == true:
				replays.Add(1)
			default:
				failed.Add(1)
				t.Errorf("HTTP = %d %v", r.status, r.body)
			}
		}()
		go func() {
			defer wg.Done()
			<-gate
			c.sendSQS(w, fmt.Sprintf("dup-msg-%d", i), spec) // each message has its own id: only the idempotency key can stop the repeat
		}()
	}
	close(gate)
	wg.Wait()
	c.waitInboundIdle()

	if c.debits(w) != "1" || c.balance(w) != "975.00" {
		t.Fatalf("debits = %s, balance = %s, want exactly one debit of 25.00", c.debits(w), c.balance(w))
	}
	if n := c.scalar(`SELECT count(*)::text FROM wager_transactions WHERE external_transaction_id = 'dup-1'`); n != "1" {
		t.Fatalf("stored operations = %s", n)
	}
	if failed.Load() != 0 || firsts.Load() > 1 {
		t.Fatalf("HTTP first answers = %d, failures = %d", firsts.Load(), failed.Load())
	}

	// The repeats really reached the application and were recognized there, not merely dropped on the way.
	processed := c.sumMetric(`wager_transactions_total{channel="http",failure_code="",kind="BET",status="PROCESSED"}`) +
		c.sumMetric(`wager_transactions_total{channel="sqs",failure_code="",kind="BET",status="PROCESSED"}`)
	replayed := c.sumMetric(`wager_idempotent_replays_total{channel="http"}`) + c.sumMetric(`wager_idempotent_replays_total{channel="sqs"}`)
	if processed != 2*perChannel || replayed != 2*perChannel-1 {
		t.Fatalf("answers = %v and replays = %v, want %d and %d", processed, replayed, 2*perChannel, 2*perChannel-1)
	}
	c.assertNothingDeadLettered()
}

// Scenario 2: 80 + 80 on a balance of 100, from different processes, by HTTP and by SQS.
func TestTwoBetsOf80OnABalanceOf100(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	procs := []*proc{c.start("replica-1", nil), c.start("replica-2", nil), c.start("replica-3", nil)}

	t.Run("by HTTP on different processes, then resent to the third", func(t *testing.T) {
		const wallets = 12
		for i := range wallets {
			w := c.openWallet(procs[0], "100.00")
			a, b := wagerSpec{ext: fmt.Sprintf("eighty-a-%d", i), kind: "BET", amount: "80.00"}, wagerSpec{ext: fmt.Sprintf("eighty-b-%d", i), kind: "BET", amount: "80.00"}

			var ra, rb reply
			var wg sync.WaitGroup
			gate := make(chan struct{})
			wg.Add(2)
			go func() { defer wg.Done(); <-gate; ra = c.postWager(procs[i%3], w, a) }()
			go func() { defer wg.Done(); <-gate; rb = c.postWager(procs[(i+1)%3], w, b) }()
			close(gate)
			wg.Wait()

			ok := func(r reply) bool { return r.status == http.StatusOK && r.body["status"] == "PROCESSED" }
			rejected := func(r reply) bool {
				return r.status == http.StatusUnprocessableEntity && r.body["status"] == "REJECTED" && r.body["failureCode"] == "INSUFFICIENT_FUNDS"
			}
			if !(ok(ra) && rejected(rb)) && !(ok(rb) && rejected(ra)) {
				t.Fatalf("wallet %d: answers %d %v and %d %v, want one processed and one INSUFFICIENT_FUNDS", i, ra.status, ra.body, rb.status, rb.body)
			}
			if c.balance(w) != "20.00" || c.debits(w) != "1" {
				t.Fatalf("wallet %d: balance = %s, debits = %s", i, c.balance(w), c.debits(w))
			}

			// Sending both again, to the process that has not seen them, changes nothing and returns the same verdicts.
			ra2, rb2 := c.postWager(procs[(i+2)%3], w, a), c.postWager(procs[(i+2)%3], w, b)
			if ra2.status != ra.status || rb2.status != rb.status || ra2.body["idempotentReplay"] != true || rb2.body["idempotentReplay"] != true {
				t.Fatalf("wallet %d: replays = %d %v and %d %v", i, ra2.status, ra2.body, rb2.status, rb2.body)
			}
			if c.balance(w) != "20.00" || c.debits(w) != "1" {
				t.Fatalf("wallet %d changed after the resend: balance = %s, debits = %s", i, c.balance(w), c.debits(w))
			}
		}
	})

	t.Run("by SQS, with the messages handled by different processes", func(t *testing.T) {
		w := c.openWallet(procs[0], "100.00")
		c.sendSQS(w, "eighty-msg-a", wagerSpec{ext: "sqs-eighty-a", kind: "BET", amount: "80.00"})
		c.sendSQS(w, "eighty-msg-b", wagerSpec{ext: "sqs-eighty-b", kind: "BET", amount: "80.00"})
		c.waitInboundIdle()

		statuses := []string{c.status("sqs-eighty-a"), c.status("sqs-eighty-b")}
		if !(statuses[0] == "PROCESSED" && statuses[1] == "REJECTED") && !(statuses[0] == "REJECTED" && statuses[1] == "PROCESSED") {
			t.Fatalf("statuses = %v", statuses)
		}
		if c.balance(w) != "20.00" || c.debits(w) != "1" {
			t.Fatalf("balance = %s, debits = %s", c.balance(w), c.debits(w))
		}
		if code := c.scalar(`SELECT failure_code FROM wager_transactions WHERE status = 'REJECTED' AND wallet_id = $1`, w.id); code != "INSUFFICIENT_FUNDS" {
			t.Fatalf("failure code = %s", code)
		}
		c.assertNothingDeadLettered()
	})
}

// Scenario 3: independent wallets must advance in parallel; a global lock would make 100 at once as slow as 100 in a row.
func TestIndependentWalletsAdvanceInParallelAcrossProcesses(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	procs := []*proc{c.start("replica-1", nil), c.start("replica-2", nil), c.start("replica-3", nil)}

	const wallets = 100
	all := make([]testWallet, wallets)
	for i := range all {
		all[i] = c.openWallet(procs[i%3], "100.00")
	}

	// Baseline: the same kind of bet, one after another.
	const samples = 20
	start := time.Now()
	for i := range samples {
		if r := c.postWager(procs[i%3], all[i], wagerSpec{ext: fmt.Sprintf("serial-%d", i), kind: "BET", amount: "1.00"}); r.status != http.StatusOK {
			t.Fatalf("serial bet %d: %d %v", i, r.status, r.body)
		}
	}
	perBet := time.Since(start) / samples

	start = time.Now()
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for i := samples; i < wallets; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			if r := c.postWager(procs[i%3], all[i], wagerSpec{ext: fmt.Sprintf("parallel-%d", i), kind: "BET", amount: "1.00"}); r.status != http.StatusOK {
				t.Errorf("parallel bet %d: %d %v", i, r.status, r.body)
			}
		}()
	}
	close(gate)
	wg.Wait()
	concurrent := time.Since(start)

	serialEstimate := perBet * (wallets - samples)
	t.Logf("one bet after another: %v each (%v for %d); %d at once: %v", perBet, serialEstimate, wallets-samples, wallets-samples, concurrent)
	if concurrent > serialEstimate*6/10 {
		t.Fatalf("%d bets on distinct wallets took %v at once, against an estimated %v one after another: they are not running in parallel", wallets-samples, concurrent, serialEstimate)
	}
	for i, w := range all {
		if c.balance(w) != "99.00" {
			t.Fatalf("wallet %d balance = %s", i, c.balance(w))
		}
	}
}
