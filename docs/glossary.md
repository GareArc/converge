# Glossary

Every term converge gives a specific meaning, in plain language. If a word
here also has an everyday meaning, this page describes the converge one.

## The two kinds of background work

### Job

One piece of background work you registered with converge, under a name you
chose. Every job is either a reconcile job or a worker job, and the name is
how you refer to it later — in logs, in metrics, and when you pause it.

### Reconcile

The kind of job that fixes drift. You give converge a list of things to check
and how often; converge calls your function once per thing, and your function
looks at how the world actually is and puts it right. Messages, if any, only
tell it *what* to look at — never *what to do*.

### Worker

The kind of job where the message is the work. Something sends a message —
"email this receipt", "build this export" — and converge hands it to your
function exactly as it was sent. If your function fails, converge sends the
same message again later; if it keeps failing, converge sets the message
aside rather than dropping it.

### Task

One kind of worker message, described in one place: its name, the shape of
its payload, and the queue it travels on. The code that sends the message and
the code that handles it both refer to that one description, so the two
cannot drift apart without the compiler noticing.

### Surface

Which of the two kinds a job is — reconcile or worker. You choose once per
job, and the choice settles everything else about it: what sets it running,
what your function is handed, and what happens when your function fails.

## Words for reconcile jobs

### ID

The name of one thing a reconcile job looks after — one customer, one app,
one deployment. A job responsible for ten thousand customers has ten thousand
IDs, and converge treats each one separately: its own failures, its own
retries, its own history.

### Wake

Making converge look at one ID sooner than it otherwise would. A wake carries
no data and no instructions; it only says *which* ID deserves attention.
Several different things can wake an ID, and the rest of this section names
them.

### Trigger

Anything that produces wakes for a job: a clock, a stream of messages from
elsewhere in your system, a piece of code you wrote yourself. A job can have
several at once, they all feed the same place, and none of them ever runs
your function directly — a trigger only ever says which ID to look at, and
converge decides what happens next.

### Schedule

The trigger that walks the whole list: every so often it wakes every ID the
job knows about. It is the trigger that makes a job correct. Turn off every
other trigger and the job is slower to notice that something changed, but it
still puts everything right in the end.

### Cadence

How often the schedule comes round — either a plain interval, "every five
minutes", or a cron expression, "07:00 on weekdays".

### Pass

One complete trip through the schedule's list, every ID visited once. If a
pass is still running when the next one is due, converge skips the next one
and tells you, rather than stacking passes on top of each other.

### Hint

A wake from some trigger other than the schedule — usually a message from
another part of your system saying "this one changed". Hints are allowed to
go missing: losing one costs you the wait until the next scheduled visit,
never correctness. A hint also will not cut short the growing pause converge
puts between retries of an ID that keeps failing.

### Poke

Telling converge "look at this one thing now, don't wait for the schedule".
A poke does not queue up work; if the same thing is poked ten times before
converge gets to it, it still gets checked once. A poke also cuts through
the growing pause after repeated failures, and brings back something
converge had set aside.

### Version

A number that goes up each time somebody changes what one ID is supposed to
look like. Your function reads it before it reads anything else and passes it
along with whatever it writes, so a write based on information that has since
changed can be refused instead of applied.

### Tracker

The version counter converge keeps for you, stored with the rest of its
bookkeeping. If your own rows already carry a column that moves whenever
somebody changes what an ID should look like, point converge at that column
instead and skip the tracker entirely.

### Parked

Converge tried to check something, it kept failing, and converge has stopped
retrying it until something changes. That happens once the job's
`DeadLetterAfter` limit is set and reached; left unset, the ID retries
forever instead. It is not lost — it starts again on the
next poke or when its version changes — and it is not the same as a
dead-lettered message, which only comes back if an operator requeues it.

## Words for worker jobs

### Queue

A named line that worker messages wait in. Jobs and messages find each other
by queue name; what a queue actually is underneath depends on what you wired
converge up to, and your code never has to know.

### Message ID

The identity a worker message is given once, when it is first sent, and keeps
for the rest of its life. It survives every retry, and it survives converge
sending the message again as a fresh one, so it is the single value you can
search your logs for to follow one piece of work from end to end.

### Logical attempt

How many times converge has genuinely tried to do a message's work. This is
the number your function is shown, and the number the retry limit is measured
against: when it reaches the limit, converge stops trying.

### Transport delivery

A second, lower-level count — how many times the queue itself has handed a
message out to be processed. It is not the same number as the logical
attempt, because converge sometimes sends a message again as a brand-new
one, which starts the queue's count over at one while the logical attempt
carries on from where it was. When the two disagree, the logical attempt is
the one that means anything to you.

### Snooze

Your function saying "not this one yet — bring it back in ten minutes". The
message goes away and returns later, and the wait costs it nothing: its
logical attempt does not move, so a message can snooze many times without
spending its retries. A separate limit on total age stops a message snoozing
forever.

### Dead-letter (DLQ)

Where a worker message goes when it has failed too many times. The message
stops being retried and is kept so a person can look at it, fix whatever was
wrong, and put it back.

### Dead-letter record

What a dead-lettered message is kept as: its payload, the extra fields it was
carrying, the error it finally failed with, and when that happened. Listing
these, putting one back, or deleting one is always a deliberate act by a
person — converge never revives one on its own.

### Envelope

How a worker message remembers anything about itself. converge attaches a
few extra fields to every message — its message ID, its logical attempt,
when it was first sent — and you never write them yourself. When converge
sends a message again as a fresh one, it carries these fields across, which
is how a snoozed message keeps its logical attempt instead of starting over
at one. Putting a dead-lettered message back is the deliberate exception: a
requeue sets the logical attempt back to zero and drops any snoozes it had
folded in, so the message starts again with its whole retry limit to spend.
Its message ID rides along unchanged, so it is still the same piece of work
in your logs.

## Words for how converge runs your jobs

### Run mode

Which of the running copies of your service — its replicas — run a given job:
exactly one of them, all of them, or all of them dividing the work between
themselves. Every job has one, and it is what decides whether running four
copies of your service means this job happens once or four times.

### Lease

How converge keeps one copy of your service in charge of a job at a time.
Whichever copy holds the lease does the work; the others wait. If the holder
dies, the lease expires and another copy picks it up. It makes duplicate
work rare, not impossible — so your function still has to be safe to run
twice.

### Outcome

A value your function returns to tell converge what should happen next,
instead of simply succeeding or failing. "Check this again in an hour", "come
back to this message later", "this work no longer matters, drop it" — each is
an ordinary return value, and none of them is counted as an error.

## Words for driving a running system

### Ops verb

Something you tell a running system to do from outside it: poke an ID, send a
hint about one, run a whole pass now, pause a job, resume it. Every replica
hears the command, so pausing a job pauses it everywhere instead of only on
the copy that happened to receive your request.

### Control queue

The queue every replica listens on for ops verbs. It belongs to converge, not
to you — you never send messages to it yourself. It exists so that one
request can reach every replica at once.

### Replica ID

The random name a replica makes up for itself when it starts. It exists so
that the answers coming back from a command can say which copy gave them.

### Which-replica-acted response

What you get back when you send a command: one answer per replica that heard
it, each saying who it was, whether it actually did anything, and what went
wrong if something did. You learn that the copy holding the lease ran your
pass, rather than only that "someone" did.

### Response expiry

The replies to a command are written down somewhere the sender can collect
them, and they are set to delete themselves about a minute later. Whoever
asked gets a clean set of answers to their own command instead of an
ever-growing pile of every command anyone has ever sent.

### Durable pause flag

Pausing a job also writes down a note that outlives the process. Every
replica reads that note when it starts, so a job you paused stays paused
across a restart or a redeploy — you do not come back to find it running
again because somebody deployed.

### DLQ op

Listing dead-lettered messages, putting one back, or deleting one. These do
not travel through the control queue: dead-letter records live somewhere
every replica can read and write, so whichever replica takes the request can
simply do the work itself.

### Payload display opt-in

Dead-lettered messages can hold real user data, so converge leaves payloads
out of what it shows unless you explicitly turn them on. Left off, the
payload is simply absent from the output; nothing is replaced with stars, so
what you are shown is never a doctored version of the real thing.

### Ops exposure

converge hands you HTTP handlers; it never opens a port itself. The handler
that can change things carries no login of its own, so you mount it behind
whatever authentication your service already has — or mount the read-only
one instead, which can only tell you what is registered and what ran last.

Coming from Kubernetes or Kafka? [Converge terms in other systems](reference/prior-art.md).
