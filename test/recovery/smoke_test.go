//go:build integration && faultinject

package recovery

import (
	"bytes"
	"net/http"
	"os/exec"
	"testing"
)

func TestThreeProcessesShareOneService(t *testing.T) {
	c := newCluster(t, clusterOpts{})
	procs := []*proc{c.start("replica-1", nil), c.start("replica-2", nil), c.start("replica-3", nil)}

	w := c.openWallet(procs[0], "100.00")
	// A wallet opened through one process is visible to the others: nothing lives in a process's memory.
	for i, p := range procs {
		r := c.do(p, http.MethodGet, "/wallets/"+w.id, c.admin, "", "")
		if r.status != http.StatusOK {
			t.Fatalf("replica %d read the wallet: %d %v", i+1, r.status, r.body)
		}
	}
	if r := c.postWager(procs[1], w, wagerSpec{ext: "smoke-1", kind: "BET", amount: "10.00"}); r.status != http.StatusOK {
		t.Fatalf("bet = %d %v", r.status, r.body)
	}
	if c.balance(w) != "90.00" {
		t.Fatalf("balance = %s", c.balance(w))
	}

	for _, p := range procs {
		if code := p.stop(); code != 0 {
			t.Fatalf("%s left with %d on SIGTERM\n%s", p.name, code, p.out.String())
		}
	}
}

// The production binary must not contain the code that kills it.
func TestTheProductionBuildHasNoFaultInjection(t *testing.T) {
	dir := t.TempDir()
	out := dir + "/wallet-service"
	if b, err := exec.Command("go", "build", "-trimpath", "-o", out, "../../cmd/wallet-service").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, b)
	}
	prod, err := exec.Command("strings", out).Output()
	if err != nil {
		t.Skipf("strings is not available: %v", err)
	}
	if bytes.Contains(prod, []byte("faultinject: exiting at")) {
		t.Fatal("the normal build contains the fault injection code")
	}
	tagged, err := exec.Command("strings", serviceBinary).Output()
	if err != nil || !bytes.Contains(tagged, []byte("faultinject: exiting at")) {
		t.Fatalf("the tagged build lacks the fault injection code (err = %v)", err)
	}
}
