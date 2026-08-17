package inmem_test

import (
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/convergetest/portcheck"
	"github.com/GareArc/converge/inmem"
)

var _ converge.MQ = (*inmem.MQ)(nil)

func TestMQContract(t *testing.T) {
	clock := convergetest.NewClock(time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC))
	portcheck.MQ(t,
		func(t *testing.T) converge.MQ { return inmem.NewMQWithClock(clock) },
		portcheck.MQOptions{Advance: clock.Advance, Visibility: inmem.DefaultVisibility},
	)
}
