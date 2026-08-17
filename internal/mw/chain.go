package mw

import "github.com/GareArc/converge"

func Chain(list []converge.Middleware, final converge.Handler) converge.Handler {
	h := final
	for i := len(list) - 1; i >= 0; i-- {
		h = list[i](h)
	}
	return h
}
