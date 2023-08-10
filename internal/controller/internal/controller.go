package internal

import (
	"github.com/coinbase/chainnode/internal/api"
)

type (
	Controller interface {
		Checkpointer() Checkpointer
		Handler() Handler
		Indexer(collection api.Collection) (Indexer, error)
		CronTasks() []CronTask
		ReverseProxies() []ReverseProxy
	}
)
