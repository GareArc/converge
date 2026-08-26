# 2. Many things to check

Chapter 1's job watched one thing: the whole warehouse, checked by a single
function on a timer. Most real jobs watch a table, not a single row — every
product, every customer, every deployed region needs the same check, and the
list of them lives in your own data, not in your code. This chapter turns
that one function into one call per row of your own data, with converge
reading the list itself, fresh, at the start of every round. By the end you
will have a job that keeps up with your data as it grows, with no line of
code to change when a new product is added.

## The code

The whole program:

```go title=examples/guide/02-ids/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

func skuIDs(ctx context.Context) ([]string, error) {
	return []string{"SKU-1001", "SKU-1002"}, nil
}

func main() {
	rt, err := converge.New(converge.Options{
		Lease: inmem.NewLease(),
		KV:    inmem.NewKV(),
	})
	if err != nil {
		log.Fatal(err)
	}

	err = reconcile.Register(rt, reconcile.Spec{
		Name: "sync-inventory",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			fmt.Println("checking stock for", id)
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.StringIDs(skuIDs), reconcile.Every(2*time.Second)),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

`skuIDs` stands in for a query against your own database — in a real
service it would list rows from a table, not return a fixed slice. Everything
else is `reconcile.Register` in place of chapter 1's `reconcile.Periodic`: the
longer form is what you reach for once a job has a list to read instead of
one fixed thing to do.

## Run it

```sh
cd examples && go run ./guide/02-ids
```

```
checking stock for SKU-1001
checking stock for SKU-1002
checking stock for SKU-1001
checking stock for SKU-1002
checking stock for SKU-1001
checking stock for SKU-1002
```

## What happened

1. `SKU-1001` and `SKU-1002` printed together, in that order, before either name
   appeared a second time — converge checks the whole list before it starts
   over.
2. That pair repeated every two seconds: three rounds inside the five-second
   window, at roughly zero, two, and four seconds.
3. `skuIDs` ran again at the start of each round — nothing here remembers
   last round's list. A SKU you add to your own data between rounds shows
   up on the very next round, with no restart.
4. At five seconds the deadline passed, `rt.Run` returned, and the shell got
   its prompt back with status 0 — six lines, not eight, because the deadline
   landed between rounds.

## The principle

Your function's second argument is what changed shape from chapter 1:
instead of taking nothing, it takes one value — the name of whichever SKU
converge wants checked right now. That name is an
[ID](../glossary.md#id): the name of one thing a job looks after. converge
doesn't tell your function what changed about `"SKU-1001"` or why
it's due; it just says `"SKU-1001"`, and your function looks at that SKU's
real state and puts it right, the same way chapter 1's job did for the one
thing it watched.

That is the whole difference from a queue: a queue hands you one message per
event, so if `"SKU-1001"` changes twice before you get to it, two messages are
waiting. Here there is no message to pile up, only a name on a list — if
`"SKU-1001"` needs checking twice before converge gets to it, converge checks it
once.

## Try breaking it

Change `skuIDs` to return `nil, errors.New("catalog unavailable")`
instead of a list (and add `"errors"` to the import block), and run it
again. All five seconds pass without a single line of output — no
`checking stock for` lines and no error either — and the process exits with
status 0, the same as a run that went well.

The job does not die, but it does not tell you anything is wrong either: this
program never checks a single SKU, and nothing here prints when the list
fails to load. converge keeps calling `skuIDs` on its own, waiting a
little longer each time it fails, and if `skuIDs` starts working again the
very next round runs normally — but none of that is visible unless your own
code, or something watching the process, is looking for it. A round that
never got its list is easy to mistake for a job that has nothing to do.

## A caveat

`skuIDs` runs once at the start of every round, and converge waits for it
to return before checking the first SKU. Query ten thousand rows and take
three seconds doing it, and that is three seconds before anything in that
round gets checked — every round, not just the first. A slow list holds up
everything on it, not only the SKU it was slow to find.

Next: [3. Reacting to events](03-triggers.md) — the other way work
arrives, alongside the schedule: a message from the rest of your system.
