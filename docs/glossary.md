# Glossary

Every word converge gives a specific meaning, in plain language. If a word
here also has an everyday meaning, this page describes the converge one.

## The two kinds of background work

### Job

One piece of background work you registered with converge, under a name you
chose. The name is how you refer to it afterwards — in logs, in metrics,
and when you send it something. Jobs are fixed at startup: you write them
in code, and no job appears or disappears while your service is running.

### Surface

Which of the two kinds a job is. One question decides it: *can you write a
query that lists everything still to be done, without reading the queue?*
If you can, the job is a reconcile job; if you cannot, it is a worker job.
You choose once per job, and the choice settles what starts a run, what
your function is handed, and what happens when your function fails.

### Reconcile

The kind of job that fixes drift. You tell converge what to check and how
often; it calls your function once per thing, and your function looks at
how the world actually is and puts it right. The real answer always lives
in your own store, so a message here only says *what to look at*, never
*what to do*.

### Worker

The kind of job where the message is the work. Something sends a message —
"email this receipt", "build this export" — and converge hands it to your
function exactly as it was sent. If your function fails, converge tries the
same message again later; if it keeps failing, converge sets the message
aside rather than dropping it.

### Task

One kind of worker message, described in one place: its name, the shape of
its payload, which version of that shape it is, and the queue it travels
on. The code that sends the message and the code that handles it both refer
to that one description, so the two cannot drift apart without the compiler
noticing. Nothing else has to be agreed between them — not a route, and not
a queue name unless you chose to give it one.

## Words for reconcile jobs

### ID

The name of one thing a reconcile job looks after — one customer, one app,
one deployment. A job responsible for ten thousand customers has ten
thousand IDs, and converge treats each one separately: its own failures,
its own retries, its own history. IDs are the only thing that comes and
goes while your service runs; they appear when they appear in your data and
leave when they leave it.

### Notification

A message that says "look at this one sooner". It carries no instructions
and no data beyond which ID it is about, several of them for the same ID
collapse into one, and losing one costs you nothing but the wait until the
next scheduled visit. A notification also clears the growing wait converge
puts between retries of something that keeps failing, so the thing you just
told converge about is looked at straight away.

### Sweep

One trip through everything a reconcile job is responsible for, putting
each ID in line to be checked. A sweep only queues the work; your function
runs afterwards, one call per ID. If a sweep is still going when the next
one falls due, converge tells you it is running late rather than starting a
second one on top.

### Trigger

Anything that puts an ID in line to be looked at: the clock, messages from
elsewhere in your system, a queue another system writes to. A job can have
several at once and they all feed the same line, where duplicates for the
same ID collapse. No trigger ever runs your function directly — it only
ever names an ID.

### Schedule

The trigger that walks the whole list: every so often it sweeps every ID
the job knows about. It is the trigger that makes a job correct, and every
reconcile job has to have one. Turn off every other trigger and the job is
slower to notice that something changed, but it still puts everything right
in the end.

### Cadence

How often the schedule comes round — either a plain "every five minutes",
or a cron expression, "07:00 on weekdays". If your service was down or busy
and the moment passed, converge runs once when it comes back and then
carries on as normal, rather than either skipping silently or working
through every round it missed.

### Version

A number that goes up each time somebody changes what one ID is supposed to
look like. Your function reads it before it reads anything else and passes
it along with whatever it writes, so a write based on information that has
since changed can be refused instead of applied. converge does not keep
this number for you — you point it at one you already have, usually a
column.

### Failing ID

Something converge tried to check, which failed, and which is now waiting
before being tried again. Each failure lengthens the wait, up to a ceiling,
so a thing that is broken for a week costs a handful of calls an hour and
never stops being tried. You can see how many there are, and which ones
they are with their last error, without going near a log.

## Words for worker jobs

### Message ID

The identity a worker message is given once, when it is first sent, and
keeps for the rest of its life. It survives every retry, it survives
converge sending the message again as a fresh one, and it survives a person
putting the message back after it was set aside. It is the single value you
can search your logs for to follow one piece of work end to end.

### Logical attempt

How many times converge has genuinely tried to do a message's work. This is
the number your function is shown, and the number the retry limit is
measured against: when it reaches the limit, converge stops trying.

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
spending its retries. A separate limit on total age stops a message
snoozing forever.

### Envelope

How a worker message remembers anything about itself. converge attaches a
few extra fields to every message — its message ID, how many attempts it
has had, when it was first sent — and you never write them yourself. They
are what lets a message come back later without starting over at attempt
one, and putting a set-aside message back is the one deliberate exception:
that resets the count so the message begins again with its whole retry
budget.

### Shelf

Where a worker message goes when converge will not try it again: its
retries or its age ran out, its payload did not match the shape your
handler expects, it would not decode at all, or your function asked for it
to be kept. The message stops moving and is kept so a person can look at
it, fix whatever was wrong, and put it back. Nothing comes off the shelf on
its own.

### Shelved message

What a message on the shelf is kept as: the payload it was carrying, its
extra fields, the reason it stopped, and when that happened. The reason is
a short phrase you can read — it ran out of attempts, it ran out of time,
it was the wrong shape, it would not decode, or whatever your own function
said when it asked for the message to be kept.

## Words for how converge runs your jobs

### Notifications

Where a reconcile job receives things. A notification only says *look at
this one* — or *look at everything* — so this channel can be flushed,
trimmed, or lost and the job stays correct; the next sweep finds the same
work. converge derives its name from your namespace and the job's name
unless you give the job one yourself, which you do when a service in
another language needs to write to it by a name it can read.

### Queue

Where a worker job's messages wait. Each message is the only copy of that
piece of work, so this is the channel you must not flush: a message stays
until your handler has acknowledged it, comes back if the handler fails,
and is set aside on the shelf when the retries run out. converge derives
its name from your namespace and the task's name unless the task declares
one.

### Run mode

Which of the running copies of your service — its replicas — run a given
job: exactly one of them, all of them, or all of them sharing out the
incoming messages between themselves. Every job has one, and it is what
decides whether running four copies of your service means this job happens
once or four times.

### Outcome

How one run ended, in one word: it succeeded, it failed and will be tried
again, it asked to be called back later, it was dropped on purpose, or it
was set aside. Your function can ask for the last three by returning an
ordinary value instead of an error — none of them is counted as a failure.

### Lease

How converge keeps one copy of your service in charge of a job at a time.
Whichever copy holds the lease does the work; the others wait. If the
holder dies, the lease expires and another copy picks it up. It makes
duplicate work rare, not impossible — so your function still has to be safe
to run twice.

### Time limit

How long one run of your function is allowed to take. When it is up,
converge cancels the context it handed you, so a call that hangs on a dead
dependency releases the job instead of holding it forever. Leave it unset
and a run may take as long as it takes.

### Stop condition

How you say, when you register a job, that the job will one day be finished
— either a moment in time, or a key that somebody will set when the work is
done. A job that stops this way is done everywhere and stays done across
restarts. There is no pausing a job and no resuming one: a job is not
started, running, or finished for good.

### Tombstone

The note converge writes down when a job is finished for good, so that
finishing survives a restart or a redeploy. Every copy of your service
checks for it before it starts working and keeps checking while it works.
The note stays; the job itself only goes away when you delete its code.

### Backlog

How much is actually waiting in a job's channel — its notifications or its
queue — as reported by the queue itself rather than counted inside your
process. Not every backend can answer that question, so the number always
comes with a flag saying whether it is real — a job whose backend cannot
tell you shows as unknown, never as zero.

### Replica ID

The random name a running copy of your service makes up for itself when it
starts. It is how one copy is told apart from another whenever converge has
to say which one it means. converge makes the name up rather than taking
one from your deployment, so nothing depends on how your instances happen
to be named.
