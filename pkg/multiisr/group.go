package multiisr

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/isr"
)

type group struct {
	id         uint64
	generation uint64
	replica    isr.Replica
}

func (g *group) ID() uint64 {
	return g.id
}

func (g *group) Status() isr.ReplicaState {
	return g.replica.Status()
}

func (g *group) Append(ctx context.Context, records []isr.Record) (isr.CommitResult, error) {
	return g.replica.Append(ctx, records)
}
