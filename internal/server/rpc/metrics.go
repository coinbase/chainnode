package rpc

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

var (
	rpcRequestGauge        = metrics.NewRegisteredGauge("rpc/requests", nil)
	successfulRequestGauge = metrics.NewRegisteredGauge("rpc/success", nil)
	failedRequestGauge     = metrics.NewRegisteredGauge("rpc/failure", nil)

	// serveTimeHistName is the prefix of the per-request serving time histograms.
	serveTimeHistName = "rpc/duration"

	rpcServingTimer = metrics.NewRegisteredTimer("rpc/duration/all", nil)
)

// updateServeTimeHistogram tracks the serving time of a remote RPC call.
func updateServeTimeHistogram(method string, success bool, elapsed time.Duration) {
	note := "success"
	if !success {
		note = "failure"
	}
	h := fmt.Sprintf("%s/%s/%s", serveTimeHistName, method, note)
	sampler := func() metrics.Sample {
		return metrics.ResettingSample(
			metrics.NewExpDecaySample(1028, 0.015),
		)
	}
	metrics.GetOrRegisterHistogramLazy(h, nil, sampler).Update(elapsed.Microseconds())
}
