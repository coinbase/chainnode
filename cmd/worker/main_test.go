package main

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/testapp"
)

type WorkerTestSuite struct {
	testsuite.WorkflowTestSuite
	t *testing.T
}

func (s WorkerTestSuite) T() *testing.T {
	return s.t
}

func TestIntegrationWorker(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		if !cfg.IsFunctionalTest() {
			t.Skip()
		}

		ts := &WorkerTestSuite{t: t}
		env := cadence.NewTestEnv(ts)
		manager := startManager(
			config.WithCustomConfig(cfg),
			cadence.WithTestEnv(env),
		)

		time.Sleep(100 * time.Millisecond)
		manager.Shutdown()
		manager.WaitForInterrupt()
	})
}
