//go:build integration && faultinject

// Package recovery runs the real service as separate OS processes, sharing one database and the same queues, and kills
// them at chosen moments. It is the proof that coordination lives in the database and the queue, not in any process.
package recovery

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/celio001/backend-challenge-go/internal/infra/postgres"
	"github.com/celio001/backend-challenge-go/internal/testutil/pgtest"
	"github.com/celio001/backend-challenge-go/internal/testutil/sqstest"
	"github.com/celio001/backend-challenge-go/internal/usecase/reconcile"
)

// localstackSender is the SenderId LocalStack reports for every message; the only provider it can represent is provider-a.
const localstackSender = "000000000000"

var serviceBinary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "recovery-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := func() int {
		defer os.RemoveAll(dir)
		serviceBinary = filepath.Join(dir, "wallet-service")
		build := exec.Command("go", "build", "-tags", "faultinject", "-trimpath", "-o", serviceBinary, "../../cmd/wallet-service")
		build.Stdout, build.Stderr = os.Stdout, os.Stderr
		if err := build.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "building the service:", err)
			return 1
		}
		return m.Run()
	}()
	os.Exit(code)
}

// syncBuffer collects a process's output; the test reads it while the process still writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type proc struct {
	t    *testing.T
	name string
	base string
	cmd  *exec.Cmd
	out  *syncBuffer
	done chan struct{}
	code int
}

// waitExit returns the exit code of a process that is expected to end (by itself or because it was signaled).
func (p *proc) waitExit(timeout time.Duration) int {
	p.t.Helper()
	select {
	case <-p.done:
		return p.code
	case <-time.After(timeout):
		p.t.Fatalf("%s did not exit within %s\n%s", p.name, timeout, p.out.String())
		return -1
	}
}

func (p *proc) alive() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// stop is a graceful shutdown (SIGTERM); the service must leave with code 0.
func (p *proc) stop() int {
	p.t.Helper()
	if !p.alive() {
		return p.code
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	return p.waitExit(20 * time.Second)
}

func (p *proc) kill() {
	if p.alive() {
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

type clusterOpts struct {
	visibilitySeconds int
	referenceTTL      string
	outboxLease       string
	referenceLease    string
}

type cluster struct {
	t       *testing.T
	kc      string
	pool    *pgxpool.Pool
	dbURL   string
	events  sqstest.Queue
	inbound sqstest.Queue
	dlq     sqstest.Queue
	opts    clusterOpts
	admin   string
	pa      string
	procs   []*proc
}

func newCluster(t *testing.T, opts clusterOpts) *cluster {
	t.Helper()
	kc := os.Getenv("TEST_KEYCLOAK_URL")
	if kc == "" {
		t.Skip("TEST_KEYCLOAK_URL not set")
	}
	if opts.visibilitySeconds == 0 {
		opts.visibilitySeconds = 30
	}
	pool, dbURL := pgtest.New(t)
	events := sqstest.New(t)
	inbound, dlq := sqstest.NewInbound(t, opts.visibilitySeconds)
	c := &cluster{t: t, kc: kc, pool: pool, dbURL: dbURL, events: events, inbound: inbound, dlq: dlq, opts: opts}
	c.admin = c.token("wallet-internal", "wallet-internal-secret")
	c.pa = c.token("provider-a", "provider-a-secret")

	t.Cleanup(c.stopAll)
	// Registered last, so it runs first: every wallet the test touched must agree with its ledger, however it was killed.
	t.Cleanup(c.assertEveryWalletReconciles)
	return c
}

func (c *cluster) token(client, secret string) string {
	c.t.Helper()
	resp, err := http.PostForm(c.kc+"/realms/wallet/protocol/openid-connect/token", url.Values{
		"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {secret},
	})
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		c.t.Fatalf("token for %s: %d %v", client, resp.StatusCode, err)
	}
	return body.AccessToken
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// start runs one replica as its own OS process and waits until it is ready. extra overrides the cluster's environment.
func (c *cluster) start(name string, extra map[string]string) *proc {
	c.t.Helper()
	addr := freeAddr(c.t)
	env := map[string]string{
		"HTTP_ADDR": addr, "DATABASE_URL": c.dbURL, "OIDC_ISSUER": c.kc + "/realms/wallet", "OIDC_DISCOVERY_URL": "",
		"AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "SQS_ENDPOINT": os.Getenv("TEST_SQS_ENDPOINT"),
		"EVENTS_QUEUE_NAME": c.events.Name, "WAGER_QUEUE_NAME": c.inbound.Name, "WAGER_DLQ_NAME": c.dlq.Name,
		"SQS_SENDER_PROVIDER_MAP": localstackSender + "=provider-a",
	}
	if c.opts.referenceTTL != "" {
		env["REFERENCE_TTL"] = c.opts.referenceTTL
	}
	if c.opts.outboxLease != "" {
		env["OUTBOX_LEASE"] = c.opts.outboxLease
	}
	if c.opts.referenceLease != "" {
		env["REFERENCE_LEASE"] = c.opts.referenceLease
	}
	for k, v := range extra {
		env[k] = v
	}

	cmd := exec.Command(serviceBinary)
	cmd.Env = append(os.Environ()[:0:0], "PATH="+os.Getenv("PATH"))
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out := &syncBuffer{}
	cmd.Stdout, cmd.Stderr = out, out
	p := &proc{t: c.t, name: name, base: "http://" + addr, cmd: cmd, out: out, done: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		c.t.Fatalf("start %s: %v", name, err)
	}
	go func() {
		err := cmd.Wait()
		p.code = cmd.ProcessState.ExitCode()
		if err != nil && p.code == -1 {
			p.code = 137 // killed by a signal
		}
		close(p.done)
	}()
	c.procs = append(c.procs, p)

	deadline := time.Now().Add(40 * time.Second)
	for {
		if !p.alive() {
			c.t.Fatalf("%s exited with %d before it was ready\n%s", name, p.code, out.String())
		}
		resp, err := http.Get(p.base + "/health/ready")
		if err == nil {
			ready := resp.StatusCode == http.StatusOK
			resp.Body.Close()
			if ready {
				return p
			}
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("%s was not ready in time\n%s", name, out.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *cluster) stopAll() {
	for _, p := range c.procs {
		if p.alive() {
			p.stop()
		}
	}
}

func (c *cluster) live() []*proc {
	var out []*proc
	for _, p := range c.procs {
		if p.alive() {
			out = append(out, p)
		}
	}
	return out
}

type reply struct {
	status int
	body   map[string]any
}

func (c *cluster) do(p *proc, method, path, token, key, body string) reply {
	c.t.Helper()
	req, err := http.NewRequest(method, p.base+path, strings.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return reply{status: -1, body: map[string]any{"error": err.Error()}}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	r := reply{status: resp.StatusCode, body: map[string]any{}}
	_ = json.Unmarshal(raw, &r.body)
	return r
}

func newUUID(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type testWallet struct{ player, id string }

func (c *cluster) openWallet(p *proc, balance string) testWallet {
	c.t.Helper()
	player := newUUID(c.t)
	r := c.do(p, http.MethodPost, "/wallets", c.admin, "", fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":%q,"currency":"BRL"}}`, player, balance))
	if r.status != http.StatusCreated {
		c.t.Fatalf("open wallet: %d %v", r.status, r.body)
	}
	return testWallet{player: player, id: r.body["id"].(string)}
}

type wagerSpec struct {
	ext, kind, amount, ref string
}

func (w testWallet) httpBody(s wagerSpec) string {
	ref := ""
	if s.ref != "" {
		ref = fmt.Sprintf(`,"referenceExternalTransactionId":%q`, s.ref)
	}
	return fmt.Sprintf(`{"providerId":"provider-a","externalTransactionId":%q,"playerId":%q,"walletId":%q,"roundId":"round-1","gameId":"game-1","kind":%q,"money":{"amount":%q,"currency":"BRL"}%s}`,
		s.ext, w.player, w.id, s.kind, s.amount, ref)
}

func (w testWallet) sqsBody(messageID string, s wagerSpec) string {
	ref := ""
	if s.ref != "" {
		ref = fmt.Sprintf(`,"referenceExternalTransactionId":%q`, s.ref)
	}
	return fmt.Sprintf(`{"messageId":%q,"type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z","data":{"providerId":"provider-a","externalTransactionId":%q,"idempotencyKey":%q,"playerId":%q,"walletId":%q,"roundId":"round-1","gameId":"game-1","kind":%q,"money":{"amount":%q,"currency":"BRL"}%s}}`,
		messageID, s.ext, "provider-a:"+s.ext, w.player, w.id, s.kind, s.amount, ref)
}

func (c *cluster) postWager(p *proc, w testWallet, s wagerSpec) reply {
	c.t.Helper()
	return c.do(p, http.MethodPost, "/wagering/transactions", c.pa, "provider-a:"+s.ext, w.httpBody(s))
}

var dedupSeq int64
var dedupMu sync.Mutex

// sendSQS puts a message on the inbound queue with a fresh deduplication id, so the broker's own filter never hides a repeat.
func (c *cluster) sendSQS(w testWallet, messageID string, s wagerSpec) {
	c.t.Helper()
	dedupMu.Lock()
	dedupSeq++
	id := fmt.Sprintf("dedup-%d-%d", time.Now().UnixNano(), dedupSeq)
	dedupMu.Unlock()
	if err := sendMessage(c.inbound, w.id, id, w.sqsBody(messageID, s)); err != nil {
		c.t.Fatal(err)
	}
}

func (c *cluster) scalar(q string, args ...any) string {
	c.t.Helper()
	var v string
	if err := c.pool.QueryRow(context.Background(), q, args...).Scan(&v); err != nil {
		c.t.Fatalf("%s: %v", q, err)
	}
	return v
}

func (c *cluster) balance(w testWallet) string {
	return c.scalar(`SELECT (balance_minor / 100) || '.' || lpad((balance_minor % 100)::text, 2, '0') FROM wallets WHERE id = $1`, w.id)
}

func (c *cluster) debits(w testWallet) string {
	return c.scalar(`SELECT count(*)::text FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.id)
}

func (c *cluster) status(ext string) string {
	var s string
	if err := c.pool.QueryRow(context.Background(), `SELECT status FROM wager_transactions WHERE provider_id = 'provider-a' AND external_transaction_id = $1`, ext).Scan(&s); err != nil {
		return ""
	}
	return s
}

func (c *cluster) waitFor(what string, timeout time.Duration, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			c.t.Fatalf("%s: not reached in %s", what, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// metric reads one series from a live process's /metrics; absent series count as 0.
func (c *cluster) metric(p *proc, series string) float64 {
	c.t.Helper()
	resp, err := http.Get(p.base + "/metrics")
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, series+" ") {
			var v float64
			if _, err := fmt.Sscanf(strings.TrimPrefix(line, series+" "), "%g", &v); err != nil {
				c.t.Fatalf("series %s: %v", series, err)
			}
			return v
		}
	}
	return 0
}

// sumMetric adds a series over the live processes: each one only counts what it handled itself.
func (c *cluster) sumMetric(series string) float64 {
	c.t.Helper()
	var total float64
	for _, p := range c.live() {
		total += c.metric(p, series)
	}
	return total
}

func (c *cluster) assertEveryWalletReconciles() {
	ctx := context.Background()
	rows, err := c.pool.Query(ctx, `SELECT id FROM wallets`)
	if err != nil {
		c.t.Errorf("list wallets for the closing reconciliation: %v", err)
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			c.t.Errorf("scan wallet: %v", err)
		}
		ids = append(ids, id)
	}
	rows.Close()

	uc := reconcile.New(postgres.NewReconciliationReader(c.pool), silentObserver{})
	for _, id := range ids {
		report, err := uc.Execute(ctx, id)
		if err != nil || !report.Consistent {
			c.t.Errorf("closing reconciliation of wallet %s: consistent=%v difference=%s err=%v", id, report.Consistent, report.Difference, err)
		}
	}
}

type silentObserver struct{}

func (silentObserver) Divergence(context.Context, reconcile.Report) {}

func sendMessage(q sqstest.Queue, group, dedup, body string) error {
	return q.Send(context.Background(), group, dedup, body)
}
