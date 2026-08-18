package durfmt

import (
	"fmt"
	"strings"
	"time"
)

type unit struct {
	v   int64
	suf string
}

func Format(d time.Duration) string {
	if d%time.Second != 0 {
		return d.String()
	}
	neg := d < 0
	abs := d.Abs()
	h := int64(abs / time.Hour)
	m := int64(abs % time.Hour / time.Minute)
	s := int64(abs % time.Minute / time.Second)
	units := make([]unit, 0, 3)
	if h != 0 {
		units = append(units, unit{h, "h"})
	}
	if h != 0 || m != 0 {
		units = append(units, unit{m, "m"})
	}
	units = append(units, unit{s, "s"})
	for len(units) > 1 && units[len(units)-1].v == 0 {
		units = units[:len(units)-1]
	}
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	for _, u := range units {
		fmt.Fprintf(&b, "%d%s", u.v, u.suf)
	}
	return b.String()
}
