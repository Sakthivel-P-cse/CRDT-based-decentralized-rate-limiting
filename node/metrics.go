package node

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	gossipRoundLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gossip_round_latency_seconds",
		Help:    "Latency of gossip rounds",
		Buckets: prometheus.DefBuckets,
	}, []string{"node"})

	gossipConvergence = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gossip_convergence_seconds",
		Help:    "Time to convergence after partition heal",
		Buckets: prometheus.DefBuckets,
	}, []string{"node"})

	estimatedOvershoot = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ratelimit_estimated_overshoot",
		Help: "Estimated overshoot ratio",
	}, []string{"node"})

	activeClients = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ratelimit_active_clients",
		Help: "Number of active clients",
	}, []string{"node"})
)

type MetricsServer struct {
	nodeID string
	server *http.Server
	mu     sync.Mutex
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
	go func() {
		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()
	return nil
}

func (m *MetricsServer) Stop(ctx context.Context) error {
	return m.server.Shutdown(ctx)
}

func RecordAllow(nodeID string, allowed bool, latency time.Duration) {
	result := "allowed"
	if !allowed {
		result = "denied"
	}
	allowTotal.WithLabelValues(result, nodeID).Inc()
	decisionLatency.WithLabelValues(nodeID).Observe(latency.Seconds())
}

func RecordGossipRound(nodeID string, latency time.Duration) {
	gossipRoundLatency.WithLabelValues(nodeID).Observe(latency.Seconds())
}

func RecordConvergence(nodeID string, latency time.Duration) {
	gossipConvergence.WithLabelValues(nodeID).Observe(latency.Seconds())
}

func SetOvershoot(nodeID string, ratio float64) {
	estimatedOvershoot.WithLabelValues(nodeID).Set(ratio)
}

func SetActiveClients(nodeID string, count int) {
	activeClients.WithLabelValues(nodeID).Set(float64(count))
}
