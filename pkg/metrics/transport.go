package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type TransportMetrics struct {
	rpcDuration   *prometheus.HistogramVec
	rpcTotal      *prometheus.CounterVec
	sentBytes     *prometheus.CounterVec
	receivedBytes *prometheus.CounterVec
	poolActive    *prometheus.GaugeVec
	poolIdle      *prometheus.GaugeVec
	poolMu        sync.Mutex
	poolPeers     map[string]struct{}
}

func newTransportMetrics(registry prometheus.Registerer, labels prometheus.Labels) *TransportMetrics {
	m := &TransportMetrics{
		rpcDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "wukongim_transport_rpc_duration_seconds",
			Help:        "Transport RPC latency in seconds.",
			ConstLabels: labels,
			Buckets:     gatewayFrameDurationBuckets,
		}, []string{"service"}),
		rpcTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "wukongim_transport_rpc_total",
			Help:        "Total number of transport RPC calls.",
			ConstLabels: labels,
		}, []string{"service", "result"}),
		sentBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "wukongim_transport_sent_bytes_total",
			Help:        "Total outbound transport payload bytes.",
			ConstLabels: labels,
		}, []string{"msg_type"}),
		receivedBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "wukongim_transport_received_bytes_total",
			Help:        "Total inbound transport payload bytes.",
			ConstLabels: labels,
		}, []string{"msg_type"}),
		poolActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "wukongim_transport_connections_pool_active",
			Help:        "Number of active transport pool connections grouped by peer node.",
			ConstLabels: labels,
		}, []string{"peer_node"}),
		poolIdle: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "wukongim_transport_connections_pool_idle",
			Help:        "Number of idle transport pool connections grouped by peer node.",
			ConstLabels: labels,
		}, []string{"peer_node"}),
		poolPeers: make(map[string]struct{}),
	}

	registry.MustRegister(
		m.rpcDuration,
		m.rpcTotal,
		m.sentBytes,
		m.receivedBytes,
		m.poolActive,
		m.poolIdle,
	)

	return m
}

func (m *TransportMetrics) ObserveRPC(service, result string, dur time.Duration) {
	if m == nil {
		return
	}
	m.rpcDuration.WithLabelValues(service).Observe(dur.Seconds())
	m.rpcTotal.WithLabelValues(service, result).Inc()
}

func (m *TransportMetrics) ObserveSentBytes(msgType string, bytes int) {
	if m == nil {
		return
	}
	m.sentBytes.WithLabelValues(msgType).Add(float64(bytes))
}

func (m *TransportMetrics) ObserveReceivedBytes(msgType string, bytes int) {
	if m == nil {
		return
	}
	m.receivedBytes.WithLabelValues(msgType).Add(float64(bytes))
}

func (m *TransportMetrics) SetPoolConnections(activeByPeer, idleByPeer map[string]int) {
	if m == nil {
		return
	}

	m.poolMu.Lock()
	defer m.poolMu.Unlock()

	currentPeers := make(map[string]struct{}, len(activeByPeer)+len(idleByPeer))
	for peer, active := range activeByPeer {
		currentPeers[peer] = struct{}{}
		m.poolActive.WithLabelValues(peer).Set(float64(active))
	}
	for peer, idle := range idleByPeer {
		currentPeers[peer] = struct{}{}
		m.poolIdle.WithLabelValues(peer).Set(float64(idle))
	}

	for peer := range currentPeers {
		if _, ok := activeByPeer[peer]; !ok {
			m.poolActive.WithLabelValues(peer).Set(0)
		}
		if _, ok := idleByPeer[peer]; !ok {
			m.poolIdle.WithLabelValues(peer).Set(0)
		}
	}

	for peer := range m.poolPeers {
		if _, ok := currentPeers[peer]; ok {
			continue
		}
		m.poolActive.WithLabelValues(peer).Set(0)
		m.poolIdle.WithLabelValues(peer).Set(0)
		delete(m.poolPeers, peer)
	}
	for peer := range currentPeers {
		m.poolPeers[peer] = struct{}{}
	}
}
