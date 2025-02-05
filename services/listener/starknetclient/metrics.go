// Copyright © 2024 Weald Technology Limited.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package starknetclient

import (
	"context"
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/wealdtech/go-starknet-listener/services/metrics"
)

var metricsNamespace = "eth_listener"

var (
	latestBlockMetric prometheus.Gauge
	failuresMetric    prometheus.Counter
)

func registerMetrics(_ context.Context, monitor metrics.Service) error {
	if failuresMetric != nil {
		// Already registered.
		return nil
	}
	if monitor == nil {
		// No monitor.
		return nil
	}
	if monitor.Presenter() == "prometheus" {
		return registerPrometheusMetrics()
	}

	return nil
}

// We should have separate metrics for blocks, txs and events.
// Also latest should be per-trigger.
func registerPrometheusMetrics() error {
	latestBlockMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "ethclient",
		Name:      "latest_block",
		Help:      "The latest block processed",
	})
	if err := prometheus.Register(latestBlockMetric); err != nil {
		return errors.Join(errors.New("failed to register latest block metric"), err)
	}

	failuresMetric = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "ethclient",
		Name:      "failures_total",
		Help:      "The number of failures.",
	})
	if err := prometheus.Register(failuresMetric); err != nil {
		return errors.Join(errors.New("failed to register total failures"), err)
	}

	return nil
}

func monitorLatestBlock(block uint32) {
	if latestBlockMetric != nil {
		latestBlockMetric.Set(float64(block))
	}
}

func monitorFailure() {
	if failuresMetric != nil {
		failuresMetric.Inc()
	}
}
