// Package mw composes middleware chains for the surface engines.
package mw

import "github.com/GareArc/converge"

// Chain wraps final with list, outermost first: Chain([a, b], h) runs
// a → b → h. Engines compose Options.Middleware before spec middleware.
func Chain(list []converge.Middleware, final converge.Handler) converge.Handler {
	h := final
	for i := len(list) - 1; i >= 0; i-- {
		h = list[i](h)
	}
	return h
}
