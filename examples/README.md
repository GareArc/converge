# Examples

Each set here is a complete, runnable service with converge wired into a
real framework, organized by the framework rather than by scenario — the
[cookbook](../docs/cookbook/index.md) already works individual problems end
to end; these show what a whole program built on one looks like.

| Set                  | Framework  | Jobs                                                                 |
| --------------------- | ---------- | --------------------------------------------------------------------- |
| [`gin`](gin/README.md) | Gin        | `expire-unpaid-orders` (reconcile), `deliver-webhook` (worker), `index-documents` (reconcile) |

A Kratos set and a polyglot set — a producer in one language enqueueing for
a handler in another — arrive in their own later plans.

Start with [the Gin example](gin/README.md).
