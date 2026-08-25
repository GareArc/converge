# converge documentation

converge is a Go library that gives your services one model for all
background work. Every job you register is one of two kinds: a
[reconcile](glossary.md#reconcile) job, which answers "is everything as it
should be?" — you hand converge a list of things to check and how often,
and it calls your function once per thing, which looks at how things
actually are and fixes what's wrong; or a worker job, which answers "do
this one specific thing that just happened?" — something sends a message,
converge hands it to your function, and retries it if that function fails.

- [Guide](guide/index.md) — a numbered path through ten chapters, from one
  scheduled job to watching it run in production.
- [Cookbook](cookbook/scenario-a-safety-net.md) — six worked scenarios,
  plus the outbox/inbox recipes.
- [Reference](reference/kernel.md) — the condensed API: `Options`, the
  ports a backend implements, and where the shipped adapters live.
- [Internals](../CONTEXT.md) — the terminology contract and contributor
  conventions this project holds itself to; start with
  [`AGENT.md`](../AGENT.md) for the verification commands.
