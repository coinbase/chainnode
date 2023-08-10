package main

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/testapp"
)

type ServerTestSuite struct {
	testsuite.WorkflowTestSuite
	t *testing.T
}

func (s ServerTestSuite) T() *testing.T {
	return s.t
}

func TestIntegrationServer(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		if !cfg.IsFunctionalTest() {
			t.Skip()
		}

		ts := &ServerTestSuite{t: t}
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
