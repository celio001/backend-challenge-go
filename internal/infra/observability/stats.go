package observability

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	statsTimeout = 2 * time.Second
	// statsCacheTTL keeps a burst of scrapes (several Prometheus replicas) from running the same aggregate queries each time.
	statsCacheTTL = 5 * time.Second
)

var (
	outboxPendingDesc = prometheus.NewDesc("outbox_pending_events", "Outbox events committed and not yet published.", nil, nil)
	outboxOldestDesc  = prometheus.NewDesc("outbox_oldest_pending_age_seconds", "Age of the oldest unpublished outbox event; 0 when there is none.", nil, nil)
	pendingRefDesc    = prometheus.NewDesc("pending_reference_open", "Operations waiting for a reference that has not arrived.", nil, nil)
)

type statsCollector struct {
	stats Stats

	mu        sync.Mutex
	fetchedAt time.Time
	snapshot  statsSnapshot
}

type statsSnapshot struct {
	outboxPending int64
	outboxOldest  time.Duration
	pendingRefs   int64
	outboxOK      bool
	refsOK        bool
}

func newStatsCollector(s Stats) *statsCollector { return &statsCollector{stats: s} }

func (c *statsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- outboxPendingDesc
	ch <- outboxOldestDesc
	ch <- pendingRefDesc
}

// Collect leaves a metric out when its query fails: a missing series is honest, a stale or zero one would hide a backlog.
func (c *statsCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.current()
	if s.outboxOK {
		ch <- prometheus.MustNewConstMetric(outboxPendingDesc, prometheus.GaugeValue, float64(s.outboxPending))
		ch <- prometheus.MustNewConstMetric(outboxOldestDesc, prometheus.GaugeValue, s.outboxOldest.Seconds())
	}
	if s.refsOK {
		ch <- prometheus.MustNewConstMetric(pendingRefDesc, prometheus.GaugeValue, float64(s.pendingRefs))
	}
}

func (c *statsCollector) current() statsSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.fetchedAt.IsZero() && time.Since(c.fetchedAt) < statsCacheTTL {
		return c.snapshot
	}
	ctx, cancel := context.WithTimeout(context.Background(), statsTimeout)
	defer cancel()

	var s statsSnapshot
	if pending, oldest, err := c.stats.OutboxBacklog(ctx); err == nil {
		s.outboxPending, s.outboxOldest, s.outboxOK = pending, oldest, true
	}
	if open, err := c.stats.PendingReferences(ctx); err == nil {
		s.pendingRefs, s.refsOK = open, true
	}
	c.snapshot, c.fetchedAt = s, time.Now()
	return s
}
