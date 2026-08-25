# Run modes and concurrency

Two orthogonal knobs from the kernel package:

```go
RunMode:     converge.OnOneReplica, // WHO runs, across replicas
Concurrency: 4,                     // HOW MANY IDs/messages at once, within the runner(s)
```

| Run mode | Meaning |
|---|---|
| `converge.OnOneReplica` | one replica active (lease + heartbeat), others stand by |
| `converge.SplitAcrossReplicas` | replicas divide the work — worker surface only in v1 (consumer group; MQ must have the `GroupConsumer` capability). On a reconcile spec: clear registration error |
| `converge.OnAllReplicas` | every replica runs it — for per-replica state (local caches) |

Defaults: reconcilers `OnOneReplica` + `Concurrency: 1`; workers
`SplitAcrossReplicas` + `Concurrency: 4`.

**Single-flight, stated precisely.** Within a process, the same ID never runs
concurrently — that is an invariant. **Across replicas it is best-effort**:
leases make overlap rare, but a paused process with an expired lease can
briefly overlap its successor. Any handler whose double-execution would be
harmful must be idempotent and, for external writes, guarded by
[version tracking](versions.md). This is why the lease is an *efficiency*
device — it prevents duplicate work; it does not provide correctness.

`OnAllReplicas` runs N independent copies, so it **rejects at registration**
any spec feature that assumes one logical runner over shared state:
`Versions`, `DeadLetterAfter`, `IDsByPage`, `RateLimit`. Per-replica jobs
converge per-replica state; shared bookkeeping would race. On the worker
surface, `OnAllReplicas` additionally changes retry semantics.
