package observability

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/usecase/reconcile"
)

// ownMetric picks this service's metric names out of a query or rule; the standard Go and process metrics are not ours to check.
var ownMetric = regexp.MustCompile(`\b(?:wager|db|wallet|sqs|outbox|pending_reference)_[a-z_]+\b`)

// exposedFamilies lists the metric families the service exposes, after every labelled series has been touched once.
func exposedFamilies(t *testing.T) map[string]bool {
	t.Helper()
	m, stats := newMetrics(t)
	stats.pending = 1
	m.transactions.WithLabelValues("http", "BET", "PROCESSED", "").Inc()
	m.replays.WithLabelValues("http").Inc()
	m.conflicts.WithLabelValues("key_reused").Inc()
	m.duration.WithLabelValues("http", "BET").Observe(0.01)
	m.TransientFailure("40001")
	m.LockWait(time.Millisecond)
	m.Message("deleted")
	m.ReceiveError()
	m.Attempt("published")
	m.References().Attempt("PROCESSED")
	m.Divergence(context.Background(), reconcile.Report{})

	families := map[string]bool{}
	for _, line := range strings.Split(scrape(t, m), "\n") {
		if name, ok := strings.CutPrefix(line, "# TYPE "); ok {
			families[strings.Fields(name)[0]] = true
		}
	}
	return families
}

// namesIn resolves the metric names a query uses to their families: a histogram is read through _bucket, _sum and _count.
func namesIn(text string) []string {
	seen := map[string]bool{}
	for _, name := range ownMetric.FindAllString(text, -1) {
		for _, suffix := range []string{"_bucket", "_sum", "_count"} {
			name = strings.TrimSuffix(name, suffix)
		}
		seen[name] = true
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// The dashboard and the alert rules are written by hand against metric names; renaming a metric must not leave them silently empty.
func TestDashboardAndAlertsOnlyUseMetricsTheServiceExposes(t *testing.T) {
	families := exposedFamilies(t)

	t.Run("dashboard", func(t *testing.T) {
		raw, err := os.ReadFile("../../../deploy/observability/grafana/dashboards/wallet-service.json")
		if err != nil {
			t.Fatal(err)
		}
		var dash struct {
			Panels []struct {
				Title   string `json:"title"`
				Targets []struct {
					Expr       string `json:"expr"`
					Datasource struct {
						Type string `json:"type"`
					} `json:"datasource"`
				} `json:"targets"`
			} `json:"panels"`
		}
		if err := json.Unmarshal(raw, &dash); err != nil {
			t.Fatal(err)
		}
		queries := 0
		for _, p := range dash.Panels {
			for _, target := range p.Targets {
				if target.Datasource.Type != "prometheus" {
					continue
				}
				queries++
				names := namesIn(target.Expr)
				if len(names) == 0 {
					continue
				}
				for _, name := range names {
					if !families[name] {
						t.Errorf("panel %q queries %q, which the service does not expose", p.Title, name)
					}
				}
			}
		}
		if queries < 10 {
			t.Fatalf("only %d Prometheus queries were found: the dashboard did not parse as expected", queries)
		}
	})

	t.Run("alert rules", func(t *testing.T) {
		raw, err := os.ReadFile("../../../deploy/observability/prometheus/alerts.yml")
		if err != nil {
			t.Fatal(err)
		}
		names := namesIn(string(raw))
		if len(names) < 4 {
			t.Fatalf("only %d metrics found in the rules: %v", len(names), names)
		}
		for _, name := range names {
			if !families[name] {
				t.Errorf("an alert rule uses %q, which the service does not expose", name)
			}
		}
	})
}
