package main

import (
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/testapp"
)

type (
	CronTestSuite struct {
		testsuite.WorkflowTestSuite
		t *testing.T
	}
)

func (s CronTestSuite) T() *testing.T {
	return s.t
}

func TestIntegrationCron(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		if !cfg.IsFunctionalTest() {
			t.Skip()
		}

		ts := &CronTestSuite{t: t}
		env := cadence.NewTestEnv(ts)
		manager := startManager(
			config.WithCustomConfig(cfg),
			cadence.WithTestEnv(env),
		)

		manager.Shutdown()
		manager.WaitForInterrupt()
	})
}
