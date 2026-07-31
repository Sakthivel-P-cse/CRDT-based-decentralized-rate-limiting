package node

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sakthivel-p-cse/crdt-rate-limiter/util"
)

var (
	allowTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ratelimit_allow_total",
		Help: "Total number of allow requests",
	}, []string{"result", "node"})

	decisionLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ratelimit_decision_latency_seconds",
		Help:    "Latency of allow decisions",
		Buckets: prometheus.DefBuckets,
	}, []string{"node"})

	gossipPayloadProcessing = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gossip_payload_processing_seconds",
		Help:    "Latency of gossip payload processing",
		Buckets: prometheus.DefBuckets,
	}, []string{"node"})

	estimatedOvershoot = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ratelimit_estimated_overshoot",
		Help: "Estimated overshoot ratio",
	}, []string{"node", "client_id"})

	activeClients = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ratelimit_active_clients",
		Help: "Number of active clients",
	}, []string{"node"})
)

type MetricsServer struct {
	nodeID   string
	server   *http.Server
	mu       sync.Mutex
	listener net.Listener
}

func NewMetricsServer(nodeID, addr string) *MetricsServer {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return &MetricsServer{
		nodeID: nodeID,
		server: server,
	}
}

func (m *MetricsServer) Start() error {
	m.mu.Lock()
	if m.listener != nil {
		m.mu.Unlock()
		return fmt.Errorf("metrics server already started")
	}
	listener, err := net.Listen("tcp", m.server.Addr)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	m.listener = listener
	m.mu.Unlock()
	go util.SafeGo("metrics-server", func() {
		if err := m.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			slog.Error("Metrics server failed to start", "error", err)
		}
	})
	return nil
}

func (m *MetricsServer) Stop(ctx context.Context) error {
	err := m.server.Shutdown(ctx)
	m.mu.Lock()
	m.listener = nil
	m.mu.Unlock()
	return err
}

func RecordAllow(nodeID string, allowed bool, latency time.Duration) {
	result := "allowed"
	if !allowed {
		result = "denied"
	}
	allowTotal.WithLabelValues(result, nodeID).Inc()
	decisionLatency.WithLabelValues(nodeID).Observe(latency.Seconds())
}

func RecordPayloadProcessing(nodeID string, latency time.Duration) {
	gossipPayloadProcessing.WithLabelValues(nodeID).Observe(latency.Seconds())
}

func SetOvershoot(nodeID string, clientID string, ratio float64) {
	estimatedOvershoot.WithLabelValues(nodeID, clientID).Set(ratio)
}

func SetActiveClients(nodeID string, count int) {
	activeClients.WithLabelValues(nodeID).Set(float64(count))
}
