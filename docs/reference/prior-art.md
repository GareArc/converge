# Converge terms in other systems

A lookup table for readers arriving from Kubernetes, controller-runtime, or
Kafka. Nothing in the guide depends on this page; it exists so a familiar
concept under an unfamiliar name is one search away.

## Terminology map

| Converge | Known elsewhere as |
|---|---|
| `Schedule` (the trigger) | resync / periodic sweep (Kubernetes) |
| `CheckAgain{In: d}` | `RequeueAfter` (controller-runtime) |
| run modes | coordination postures; `OnOneReplica` = leader election |
| `Version` / `Tracker` | generation / observedGeneration (k8s), fencing token (Kleppmann) |
| `reconcile.ID` | workqueue key (Kubernetes) |
| queue | topic (Kafka) |
| parked (reconcile) / dead-lettered (worker) | both are "dead-lettering" elsewhere; converge keeps the words apart because revival differs — a parked ID revives on poke or version change, a DLQ'd message only via ops requeue |
