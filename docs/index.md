# converge documentation

converge is a Go library that gives your services one model for all
background work. Every job you register is one of two kinds: a
[reconcile](glossary.md#reconcile) job, which answers "is everything as it
should be?" — you tell converge what to check and how often, and it calls
your function once per thing, which looks at how things actually are and
fixes what's wrong; or a worker job, which answers "do this one specific
thing that just happened?" — something sends a message, converge hands it to
your function, and tries it again if that function fails.

- [Glossary](glossary.md) — every converge-specific word, defined once.
- [Internals](https://github.com/GareArc/converge/blob/main/CONTEXT.md) — the terminology contract and contributor
  conventions this project holds itself to; start with
  [`AGENT.md`](https://github.com/GareArc/converge/blob/main/AGENT.md) for the verification commands.

The guide, the cookbook, and the API reference are being rewritten against
the v2 surface and will return here.
