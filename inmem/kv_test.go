package inmem_test

import (
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/convergetest/portcheck"
	"github.com/GareArc/converge/inmem"
)

var _ converge.KV = (*inmem.KV)(nil)

func TestKVContract(t *testing.T) {
	clock := convergetest.NewClock(time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC))
	portcheck.KV(t,
		func(t *testing.T) converge.KV { return inmem.NewKVWithClock(clock) },
		portcheck.KVOptions{Advance: clock.Advance},
	)
}
