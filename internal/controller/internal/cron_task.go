package internal

import (
	"context"
	"time"
)

type (
	CronTask interface {
		Name() string
		Spec() string
		Parallelism() int64
		Enabled() bool
		DelayStartDuration() time.Duration
		Run(ctx context.Context) error
	}
)
