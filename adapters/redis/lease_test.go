package convredis_test

import (
	"testing"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/convergetest/portcheck"
)

func TestLeasePortMiniredis(t *testing.T) {
	var advance func(d time.Duration)
	portcheck.Lease(t, func(t *testing.T) converge.Lease {
		client, _, adv := openMini(t)
		advance = adv
		return convredis.NewLease(client)
	}, portcheck.LeaseOptions{Advance: func(d time.Duration) { advance(d) }})
}

func TestLeasePortRealRedis(t *testing.T) {
	portcheck.Lease(t, func(t *testing.T) converge.Lease {
		return convredis.NewLease(openReal(t))
	}, portcheck.LeaseOptions{})
}
