package dkafka

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	messagesSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dkafka_sent_messages",
		Help: "The total number of sent messages",
	})
	transactionTracesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dkafka_received_transaction_traces",
		Help: "The total number of transaction traces received from firehose",
	})
	actionTracesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dkafka_received_action_traces",
		Help: "The total number of action traces received from firehose",
	})
	blocksReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dkafka_received_blocks",
		Help: "The total number of blocks receivedfrom firehose",
	})
)

func startPrometheusMetrics(path string, listenAddr string) {
	zlog.Info("Starting prometheus HTTP server", zap.String("listen_addr", listenAddr), zap.String("path", path))
	http.Handle(path, promhttp.Handler())
	if err := http.ListenAndServe(listenAddr, nil); err != nil {
		zlog.Warn("prometheus server failed", zap.String("listen_addr", listenAddr), zap.String("path", path), zap.Error(err))
	}
}
