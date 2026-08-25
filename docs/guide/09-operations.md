# 9. Running it in production

Chapter 8 gave you a way to prove a handler works before it ships. This one
is for after it ships. If you are the person who gets paged when a job in
your service stops making progress, this chapter is for you: it covers
looking at a running job from outside the process, pausing one without a
redeploy, and getting a message back that converge had already given up on.
If nobody pages you about a converge job, skip ahead to
[10. Seeing what it is doing](10-observability.md). By the end of this
chapter you will have asked a running program what one of its jobs is doing
over plain HTTP, watched it answer while the schedule kept going underneath,
and read the same facts back out of the process itself.

## The code

The whole program:

```go title=examples/guide/09-operations/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/debughttp"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

func main() {
	rt, err := converge.New(converge.Options{
		Lease: inmem.NewLease(),
		KV:    inmem.NewKV(),
	})
	if err != nil {
		log.Fatal(err)
	}

	err = reconcile.Periodic(rt, "refresh-licenses", reconcile.Every(time.Second), func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/debug/jobs/", debughttp.ReadOnlyHandler(rt))
	server := &http.Server{Addr: "localhost:6060", Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Println(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
	if err := server.Close(); err != nil {
		log.Println(err)
	}

	for _, s := range rt.Stats() {
		fmt.Printf("%+v\n", s)
	}
}
```

`refresh-licenses` is the same job chapter 1 started with, minus the
`fmt.Println` — this chapter cares about what the job looks like from
outside, not what it prints. Three things are new over chapter 1's shape:
an HTTP mux with `debughttp.ReadOnlyHandler(rt)` mounted at `/debug/jobs/`,
a server goroutine running alongside `rt.Run`, and a `Stats()` dump once
both have stopped. `server.Close()` on the way out matches the shutdown
discipline `rt.Run` itself follows — releasing the listener explicitly
instead of leaving it for the process exit to clean up. The `rt.Stats()`
loop runs after both have shut down, so it reads the runtime's own
bookkeeping directly, not back out over HTTP.

## Run it

```sh
cd examples && go run ./guide/09-operations &
sleep 1 && curl -s localhost:6060/debug/jobs/
wait
```

```
{"jobs":[{"job":"refresh-licenses","surface":"reconcile","run_mode":"OnOneReplica","queue":"","paused":false,"settings":{"concurrency":"1","schedule":"every 1s","triggers":"schedule"},"queue_depth":0,"parked":0,"last_success":"2026-08-25T04:26:53.919396Z","consecutive_fails":0}]}
{Job:refresh-licenses Surface:reconcile RunMode:OnOneReplica QueueDepth:0 Parked:0 LastSuccess:2026-08-24 21:26:55.918817 -0700 PDT m=+3.002647251 ConsecutiveFails:0}
```

## What happened

1. The moment `go run` started, `refresh-licenses` fired its first pass
   immediately — the same thing every scheduled job in this guide has done
   from chapter 1 on — and the HTTP server started listening on
   `localhost:6060` at the same time.
2. One second later, `curl localhost:6060/debug/jobs/` answered with a JSON
   object naming the one registered job: `"surface":"reconcile"`,
   `"run_mode":"OnOneReplica"`, `"paused":false`, and a `last_success`
   timestamp already set — the pass from step 1 had already finished by the
   time the request landed.
3. Three seconds after start, the context deadline passed, `rt.Run`
   returned `nil`, and `server.Close()` shut the listener down.
4. The program then printed one line per job from `rt.Stats()` — the same
   facts the curl response already showed, as a Go struct instead of JSON,
   read directly from the process instead of over HTTP.

## The principle

`debughttp.ReadOnlyHandler` answers with what converge already knows about
a job: its name, whether it's paused, when it last succeeded, how many
times in a row it's failed. That's also why this chapter is comfortable
mounting it on a plain, unauthenticated `localhost` listener: every route
on it only reads. Anything that changes behavior — pausing a job, resuming
one, putting a message back — lives on a second handler,
`debughttp.OpsHandler`, meant to be mounted separately, behind whatever
auth your service already has for admin routes. Both handlers reach the
same jobs the same way: a mutating call goes out on the
[control queue](../glossary.md#control-queue), the same queue every
replica of your service is already listening on, so pausing a job through
any one replica's endpoint pauses it everywhere — auditably, since the
caller gets back a record of which replica or replicas actually acted, not
just an acknowledgement from whichever process happened to answer the HTTP
request.

## Other shapes

Two more things worth knowing follow the same shape as the principle above.

**Pausing a job.** `OpsHandler` exposes pause and resume as their own
routes. This chapter's example only mounts `ReadOnlyHandler`, on
`localhost:6060` — pausing isn't available there. `OpsHandler` carries no
auth of its own, so it belongs on a separate, admin-only listener you
mount yourself, alongside the one this example already runs. If you
mounted it on, say, `localhost:6061`, pausing `refresh-licenses` would be
one call:

```sh
curl -X POST http://localhost:6061/debug/jobs/refresh-licenses/pause
```

**Putting a message back.** A worker message that runs out of retries ends
up as a [dead-letter](../glossary.md#dead-letter-dlq), kept for a person to
look at instead of thrown away — chapter 4's `sendWelcome` task showed one
going there once `MaxAttempts` ran out. Given that message's ID — found
with `dlq.List(ctx)`, or the `GET /debug/jobs/{job}/dlq` route if you're
driving this over HTTP — putting it back is `worker.DLQFrom` plus one call:

```go
dlq, err := worker.DLQFrom(rt, "send-welcome")
if err != nil {
	log.Fatal(err)
}
if err := dlq.Requeue(ctx, messageID); err != nil {
	log.Fatal(err)
}
```

Requeue does more than move the message back onto its queue. A worker
message can also be dead-lettered for simply sitting around too long —
`Retry.MaxAge` caps how long it's allowed to keep being retried at all,
independent of how many attempts it has used. Requeue resets that clock
along with everything else: the message that comes back out is stamped
with a fresh enqueued time, taken at the moment `Requeue` runs, not the
moment the message was first sent. That is why requeue can rescue a
message that had aged out — without that reset, putting it back on the
queue would run it straight into the same age check that dead-lettered it
in the first place.

The full verb catalog — every route `OpsHandler` exposes, what each one
needs, and how its response is collected — is on the
[operations reference](../reference/operations.md#ops-verbs) page.

## Try breaking it

Ask the endpoint about a job that was never registered:

```sh
curl -s -w '\nHTTP %{http_code}\n' localhost:6060/debug/jobs/does-not-exist
```

```
{"error":"unknown job \"does-not-exist\""}
HTTP 404
```

Nothing panicked and nothing hung. `ReadOnlyHandler` looked up
`does-not-exist` against the runtime's own registry, found nothing, and
answered with a plain 404 naming exactly what it couldn't find — the same
shape a typo in a deploy script or a dashboard pointed at the wrong job
name would produce: an immediate, specific answer, not a stuck request or
an empty body left for you to guess about.

## A caveat

This chapter's example keeps everything in memory, like every example so
far in this guide — so there's nothing here to restart and check. But the
pause set by the call above isn't a property of any one running process:
it's written to storage before it's broadcast to every replica, which is
exactly what makes it survive a restart or a redeploy against a real
backend. A job paused during an incident stays paused — not just for the
replicas that were listening when the pause was sent, but for the next one
that boots an hour later — until someone explicitly resumes it. Nothing in
this example demonstrates that directly; take it as a property of the
mechanism, not something `inmem` can show you happening.

Next: [10. Seeing what it is doing](10-observability.md) — turning what
converge already knows into metrics, so a job that quietly stops running
doesn't have to wait for someone to ask about it.
