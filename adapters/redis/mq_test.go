package convredis_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"slices"
	"strings"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/convergetest/portcheck"
	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
)

const miniredisStubsGroupLag = true

const (
	testGroup      = "converge"
	testStreamKey  = "q"
	testPendingKey = "convredis:p:q:" + testGroup
	testDelayedKey = "convredis:d:q"
)

type miniEnv struct {
	client  *redis.Client
	clock   *convergetest.Clock
	advance func(d time.Duration)
}

func TestMQPortMiniredis(t *testing.T) {
	const retention = time.Hour
	var (
		mu   sync.Mutex
		envs = map[string]*miniEnv{}
	)
	var advance func(d time.Duration)
	open := func(t *testing.T) converge.MQ {
		mu.Lock()
		e, ok := envs[t.Name()]
		if !ok {
			client, clock, adv := openMini(t)
			e = &miniEnv{client: client, clock: clock, advance: adv}
			envs[t.Name()] = e
		}
		mu.Unlock()
		advance = e.advance
		return convredis.NewStreamsMQ(e.client, convredis.StreamsOpts{Clock: e.clock, Visibility: time.Minute, Retention: retention})
	}
	portcheck.MQ(t, open, portcheck.MQOptions{
		Advance:           func(d time.Duration) { advance(d) },
		Visibility:        time.Minute,
		Retention:         retention,
		GroupLagIsStubbed: miniredisStubsGroupLag,
	})
}

func TestMQPortMiniredisWithoutGroupLag(t *testing.T) {
	const retention = time.Hour
	var (
		mu   sync.Mutex
		envs = map[string]*miniEnv{}
	)
	var advance func(d time.Duration)
	open := func(t *testing.T) converge.MQ {
		mu.Lock()
		e, ok := envs[t.Name()]
		if !ok {
			mr, client, clock, adv := openMiniServer(t)
			stubRedis62GroupInfo(t, mr)
			e = &miniEnv{client: client, clock: clock, advance: adv}
			envs[t.Name()] = e
		}
		mu.Unlock()
		advance = e.advance
		return convredis.NewStreamsMQ(e.client, convredis.StreamsOpts{Clock: e.clock, Visibility: time.Minute, Retention: retention})
	}
	portcheck.MQ(t, open, portcheck.MQOptions{
		Advance:    func(d time.Duration) { advance(d) },
		Visibility: time.Minute,
		Retention:  retention,
	})
}

func TestMQPortRealRedis(t *testing.T) {
	portcheck.MQ(t, func(t *testing.T) converge.MQ {
		return convredis.NewStreamsMQ(openReal(t), convredis.StreamsOpts{})
	}, portcheck.MQOptions{})
}

func TestStreamsMQWithoutRetentionKeepsEntriesUntilConsumed(t *testing.T) {
	_, client, clock, advance := openMiniServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	mq := convredis.NewStreamsMQ(client, convredis.StreamsOpts{Clock: clock, Visibility: time.Minute})

	if err := mq.Publish(ctx, "q", converge.Message{Payload: []byte("old")}); err != nil {
		t.Fatal(err)
	}
	advance(90 * 24 * time.Hour)

	got := make(chan converge.Delivery, 1)
	go mq.Consume(ctx, "q", func(d converge.Delivery) { got <- d })
	d := recvDelivery(t, got)
	if string(d.Message().Payload) != "old" {
		t.Fatalf("payload = %q, want %q: an unset Retention must not trim anything", d.Message().Payload, "old")
	}
}

func TestStreamsMQBacklogForGroupSumsUndeliveredAndUnacked(t *testing.T) {
	f := newStreamsMQ(t)
	f.publish(t, converge.Message{Payload: []byte("a")})
	stubGroupInfo(t, f.mr, groupInfo{name: testGroup, pending: 2, lag: 5, shape: lagReported})

	n, err := backlogReporter(t, f.mq).Backlog(f.ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	if n != 7 {
		t.Fatalf("Backlog = %d, want 7: 5 entries never delivered to the group plus 2 delivered and unacked", n)
	}
}

func TestStreamsMQBacklogForGroupWithoutALagFieldIsUnknown(t *testing.T) {
	f := newStreamsMQ(t)
	f.publish(t, converge.Message{Payload: []byte("a")})
	stubGroupInfo(t, f.mr, groupInfo{name: testGroup, pending: 2, shape: lagAbsent})

	n, err := backlogReporter(t, f.mq).Backlog(f.ctx, "q")
	if !errors.Is(err, converge.ErrBacklogUnknown) {
		t.Fatalf("Backlog = (%d, %v), want converge.ErrBacklogUnknown: Redis 6.2 sends no lag field", n, err)
	}
	if n != 0 {
		t.Fatalf("Backlog = %d alongside ErrBacklogUnknown, want 0", n)
	}
}

func TestStreamsMQBacklogForGroupReportsUnknownLagAsUnknown(t *testing.T) {
	f := newStreamsMQ(t)
	f.publish(t, converge.Message{Payload: []byte("a")})
	stubGroupInfo(t, f.mr, groupInfo{name: testGroup, pending: 2, shape: lagNull})

	n, err := backlogReporter(t, f.mq).Backlog(f.ctx, "q")
	if err == nil {
		t.Fatalf("Backlog = %d with a nil lag, want an error: a guessed depth is worse than none", n)
	}
	if n != 0 {
		t.Fatalf("Backlog = %d alongside an error, want 0", n)
	}
}

func TestStreamsMQBacklogAsksAboutTheGivenGroup(t *testing.T) {
	f := newStreamsMQ(t)
	f.publish(t, converge.Message{Payload: []byte("a")})
	stubGroupInfo(t, f.mr, groupInfo{name: "other", pending: 4, lag: 4, shape: lagReported})

	gbr, ok := f.mq.(converge.GroupBacklogReporter)
	if !ok {
		t.Fatal("streams MQ must implement converge.GroupBacklogReporter")
	}
	n, err := gbr.BacklogForGroup(f.ctx, "q", testGroup)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("BacklogForGroup(%q) = %d, want 1: a group that has read nothing is owed every entry", testGroup, n)
	}
}

func backlogReporter(t *testing.T, mq converge.MQ) converge.BacklogReporter {
	t.Helper()
	br, ok := mq.(converge.BacklogReporter)
	if !ok {
		t.Fatal("streams MQ must implement converge.BacklogReporter")
	}
	return br
}

type lagShape int

const (
	lagAbsent lagShape = iota
	lagNull
	lagReported
)

func (s lagShape) fieldCount() int {
	if s == lagAbsent {
		return 4
	}
	return 6
}

type groupInfo struct {
	name    string
	pending int
	lag     int
	shape   lagShape
}

func stubGroupInfo(t *testing.T, mr *miniredis.Miniredis, g groupInfo) {
	t.Helper()
	mr.Server().SetPreHook(func(c *server.Peer, cmd string, args ...string) bool {
		if cmd != "XINFO" {
			return false
		}
		c.WriteLen(1)
		writeGroupInfo(c, g)
		return true
	})
	t.Cleanup(func() { mr.Server().SetPreHook(nil) })
}

func writeGroupInfo(c *server.Peer, g groupInfo) {
	c.WriteMapLen(g.shape.fieldCount())
	c.WriteBulk("name")
	c.WriteBulk(g.name)
	c.WriteBulk("consumers")
	c.WriteInt(1)
	c.WriteBulk("pending")
	c.WriteInt(g.pending)
	c.WriteBulk("last-delivered-id")
	c.WriteBulk("0-0")
	switch g.shape {
	case lagNull:
		c.WriteBulk("entries-read")
		c.WriteNull()
		c.WriteBulk("lag")
		c.WriteNull()
	case lagReported:
		c.WriteBulk("entries-read")
		c.WriteNull()
		c.WriteBulk("lag")
		c.WriteInt(g.lag)
	}
}

type groupTracker struct {
	mu     sync.Mutex
	groups map[string][]string
}

func (tr *groupTracker) created(args []string) {
	if len(args) < 3 || !strings.EqualFold(args[0], "CREATE") {
		return
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	key, group := args[1], args[2]
	if slices.Contains(tr.groups[key], group) {
		return
	}
	tr.groups[key] = append(tr.groups[key], group)
}

func (tr *groupTracker) namesFor(key string) []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return slices.Clone(tr.groups[key])
}

func stubRedis62GroupInfo(t *testing.T, mr *miniredis.Miniredis) {
	t.Helper()
	tr := &groupTracker{groups: map[string][]string{}}
	mr.Server().SetPreHook(func(c *server.Peer, cmd string, args ...string) bool {
		switch cmd {
		case "XGROUP":
			tr.created(args)
			return false
		case "XINFO":
			if len(args) < 2 || !strings.EqualFold(args[0], "GROUPS") {
				return false
			}
			names := tr.namesFor(args[1])
			c.WriteLen(len(names))
			for _, name := range names {
				writeGroupInfo(c, groupInfo{name: name, shape: lagAbsent})
			}
			return true
		}
		return false
	})
	t.Cleanup(func() { mr.Server().SetPreHook(nil) })
}

func TestStreamsMQNegativeVisibilityDefaults(t *testing.T) {
	_, client, clock, advance := openMiniServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	mq := convredis.NewStreamsMQ(client, convredis.StreamsOpts{Clock: clock, Visibility: -time.Second})

	if err := mq.Publish(ctx, "q", converge.Message{Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	got := make(chan converge.Delivery, 16)
	go mq.Consume(ctx, "q", func(d converge.Delivery) { got <- d })
	recvDelivery(t, got)

	advance(time.Minute)
	assertNoDelivery(t, got)
}

func TestStreamsMQDeliveryCarriesHeadersAndEnqueuedAt(t *testing.T) {
	f := newStreamsMQ(t)
	msg := converge.Message{
		Kind:    "k",
		Headers: map[string]string{converge.HeaderMessageID: "m-1"},
		Payload: []byte("p"),
	}
	f.publish(t, msg)
	d := recvDelivery(t, f.consume(t))
	if got := d.Message().Headers[converge.HeaderMessageID]; got != "m-1" {
		t.Fatalf("header = %q, want m-1", got)
	}
	if !d.EnqueuedAt().Equal(f.clock.Now()) {
		t.Fatalf("EnqueuedAt = %v, want the publishing clock time %v", d.EnqueuedAt(), f.clock.Now())
	}
	if err := d.Ack(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStreamsMQExtendAfterAck(t *testing.T) {
	f := newStreamsMQ(t)
	f.publish(t, converge.Message{Payload: []byte("a")})
	d := recvDelivery(t, f.consume(t))
	if err := d.Ack(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.Extend(f.ctx, time.Minute); !errors.Is(err, convredis.ErrSettled) {
		t.Fatalf("Extend after Ack = %v, want ErrSettled", err)
	}
}

func TestStreamsMQAckSettlesTheHandleWhenCleanupFails(t *testing.T) {
	f := newStreamsMQ(t)
	f.publish(t, converge.Message{Payload: []byte("a")})
	d := recvDelivery(t, f.consume(t))
	restore := failCommand(t, f.mr, "HDEL")
	defer restore()

	if err := d.Ack(f.ctx); err == nil {
		t.Fatal("Ack = nil, want the failed bookkeeping cleanup reported")
	}
	if err := d.Extend(f.ctx, time.Hour); !errors.Is(err, convredis.ErrSettled) {
		t.Fatalf("Extend after an acked delivery whose cleanup failed = %v, want ErrSettled", err)
	}
}

func TestStreamsMQStaleExtendDoesNotPostponeRedelivery(t *testing.T) {
	f := newStreamsMQ(t)
	f.publish(t, converge.Message{Payload: []byte("a")})
	got := f.consume(t)
	stale := recvDelivery(t, got)
	if stale.Attempt() != 1 {
		t.Fatalf("Attempt = %d, want 1", stale.Attempt())
	}

	f.advance(time.Minute + time.Second)
	live := recvDelivery(t, got)
	if live.Attempt() != 2 {
		t.Fatalf("reclaimed Attempt = %d, want 2", live.Attempt())
	}

	if err := stale.Extend(f.ctx, time.Hour); !errors.Is(err, convredis.ErrSettled) {
		t.Fatalf("Extend on a stale handle = %v, want ErrSettled", err)
	}

	f.advance(time.Minute + time.Second)
	next := recvDelivery(t, got)
	if next.Attempt() != 3 {
		t.Fatalf("Attempt after a stale extend = %d, want 3", next.Attempt())
	}
	if err := next.Ack(f.ctx); err != nil {
		t.Fatal(err)
	}

	if err := stale.Nack(f.ctx, time.Hour); err != nil {
		t.Fatalf("Nack on a stale handle = %v, want nil", err)
	}
	if n, err := f.client.ZCard(f.ctx, testPendingKey).Result(); err != nil || n != 0 {
		t.Fatalf("pending set holds %d entries (err %v), want 0 after a stale nack", n, err)
	}
}

func TestStreamsMQReadBatchSurvivesCancelMidBatch(t *testing.T) {
	f := newStreamsMQ(t)
	const batch = 4
	for i := range batch {
		f.publish(t, converge.Message{Payload: []byte{byte(i)}})
	}

	cctx, stop := context.WithCancel(f.ctx)
	first := make(chan converge.Delivery, 1)
	stopped := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(stopped)
		f.mq.Consume(cctx, "q", func(d converge.Delivery) {
			once.Do(func() {
				first <- d
				stop()
			})
		})
	}()
	delivered := recvDelivery(t, first)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Consume did not return after cancel")
	}
	if err := delivered.Ack(f.ctx); err != nil {
		t.Fatal(err)
	}

	got := f.consume(t)
	f.await(t, fmt.Sprintf("%s never reached %d entries", testPendingKey, batch-1), func() bool {
		n, err := f.client.ZCard(f.ctx, testPendingKey).Result()
		return err == nil && n == int64(batch-1)
	})
	f.advance(time.Minute + time.Second)
	seen := map[byte]bool{}
	for range batch - 1 {
		d := recvDelivery(t, got)
		seen[d.Message().Payload[0]] = true
		if err := d.Ack(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != batch-1 {
		t.Fatalf("reclaimed %d distinct messages, want %d", len(seen), batch-1)
	}
	if seen[delivered.Message().Payload[0]] {
		t.Fatal("acked message was redelivered")
	}
}

func TestStreamsMQReclaimsPendingEntryThatWasNeverTracked(t *testing.T) {
	f := newStreamsMQ(t)
	f.publish(t, converge.Message{Payload: []byte("a")})
	if err := f.client.XGroupCreateMkStream(f.ctx, testStreamKey, testGroup, "0").Err(); err != nil {
		t.Fatal(err)
	}
	streams, err := f.client.XReadGroup(f.ctx, &redis.XReadGroupArgs{
		Group:    testGroup,
		Consumer: "rogue",
		Streams:  []string{testStreamKey, ">"},
		Count:    1,
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	id := streams[0].Messages[0].ID
	if n, err := f.client.ZCard(f.ctx, testPendingKey).Result(); err != nil || n != 0 {
		t.Fatalf("pending set holds %d entries (err %v), want an untracked pending entry", n, err)
	}

	got := f.consume(t)
	f.await(t, fmt.Sprintf("entry %s was never tracked in %s", id, testPendingKey), func() bool {
		return f.client.ZScore(f.ctx, testPendingKey, id).Err() == nil
	})
	f.advance(time.Minute + time.Second)
	d := recvDelivery(t, got)
	if string(d.Message().Payload) != "a" {
		t.Fatalf("got %q, want a", d.Message().Payload)
	}
	if err := d.Ack(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStreamsMQDelayedPublishKeepsIdenticalMessagesApart(t *testing.T) {
	f := newStreamsMQ(t)
	msg := converge.Message{Kind: "k", Payload: []byte("a")}
	f.publishDelayed(t, msg, time.Hour)
	f.publishDelayed(t, msg, 24*time.Hour)

	got := f.consume(t)
	f.advance(time.Hour + time.Second)
	if err := recvDelivery(t, got).Ack(f.ctx); err != nil {
		t.Fatal(err)
	}
	assertNoDelivery(t, got)
	f.advance(23 * time.Hour)
	if err := recvDelivery(t, got).Ack(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStreamsMQDelayedReleaseRetriesAfterFailedPublish(t *testing.T) {
	f := newStreamsMQ(t)
	f.publishDelayed(t, converge.Message{Payload: []byte("a")}, time.Hour)
	restore := failCommand(t, f.mr, "XADD")

	got := f.consume(t)
	f.advance(time.Hour + time.Second)
	assertNoDelivery(t, got)
	if n, err := f.client.ZCard(f.ctx, testDelayedKey).Result(); err != nil || n != 1 {
		t.Fatalf("delayed set holds %d records (err %v), want the claimed record retained", n, err)
	}

	restore()
	f.advance(time.Minute + time.Second)
	d := recvDelivery(t, got)
	if string(d.Message().Payload) != "a" {
		t.Fatalf("got %q, want a", d.Message().Payload)
	}
	if err := d.Ack(f.ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := f.client.ZCard(f.ctx, testDelayedKey).Result(); err != nil || n != 0 {
		t.Fatalf("delayed set holds %d records (err %v), want 0 after a successful release", n, err)
	}
}

func TestStreamsMQSettlesUndecodableEntries(t *testing.T) {
	f := newStreamsMQ(t)
	if err := f.client.XAdd(f.ctx, &redis.XAddArgs{
		Stream: testStreamKey,
		Values: map[string]any{"foreign": "x"},
	}).Err(); err != nil {
		t.Fatal(err)
	}
	f.publish(t, converge.Message{Payload: []byte("a")})

	got := f.consume(t)
	d := recvDelivery(t, got)
	if string(d.Message().Payload) != "a" {
		t.Fatalf("got %q, want a", d.Message().Payload)
	}
	if err := d.Ack(f.ctx); err != nil {
		t.Fatal(err)
	}

	f.advance(time.Minute + time.Second)
	assertNoDelivery(t, got)
	if n, err := f.client.ZCard(f.ctx, testPendingKey).Result(); err != nil || n != 0 {
		t.Fatalf("pending set holds %d entries (err %v), want 0 after settling the foreign entry", n, err)
	}
	if p, err := f.client.XPending(f.ctx, testStreamKey, testGroup).Result(); err != nil || p.Count != 0 {
		t.Fatalf("PEL holds %+v (err %v), want no pending entries after settling the foreign entry", p, err)
	}
}

func TestStreamsMQRecoversFromDeletedStream(t *testing.T) {
	f := newStreamsMQ(t)
	got := f.consume(t)
	f.publish(t, converge.Message{Payload: []byte("a")})
	if err := recvDelivery(t, got).Ack(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Del(f.ctx, testStreamKey).Err(); err != nil {
		t.Fatal(err)
	}
	f.publish(t, converge.Message{Payload: []byte("b")})
	d := recvDelivery(t, got)
	if string(d.Message().Payload) != "b" {
		t.Fatalf("got %q, want b", d.Message().Payload)
	}
	if err := d.Ack(f.ctx); err != nil {
		t.Fatal(err)
	}
}

type streamsMQFixture struct {
	mq      converge.MQ
	client  *redis.Client
	mr      *miniredis.Miniredis
	clock   *convergetest.Clock
	advance func(d time.Duration)
	ctx     context.Context
}

func newStreamsMQ(t *testing.T) *streamsMQFixture {
	t.Helper()
	mr, client, clock, advance := openMiniServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &streamsMQFixture{
		mq:      convredis.NewStreamsMQ(client, convredis.StreamsOpts{Clock: clock, Visibility: time.Minute}),
		client:  client,
		mr:      mr,
		clock:   clock,
		advance: advance,
		ctx:     ctx,
	}
}

func (f *streamsMQFixture) publish(t *testing.T, msg converge.Message) {
	t.Helper()
	if err := f.mq.Publish(f.ctx, "q", msg); err != nil {
		t.Fatal(err)
	}
}

func (f *streamsMQFixture) publishDelayed(t *testing.T, msg converge.Message, delay time.Duration) {
	t.Helper()
	dp, ok := f.mq.(converge.DelayedPublisher)
	if !ok {
		t.Fatal("streams MQ must implement converge.DelayedPublisher")
	}
	if err := dp.PublishDelayed(f.ctx, "q", msg, delay); err != nil {
		t.Fatal(err)
	}
}

func (f *streamsMQFixture) consume(t *testing.T) chan converge.Delivery {
	t.Helper()
	got := make(chan converge.Delivery, 16)
	go f.mq.Consume(f.ctx, "q", func(d converge.Delivery) { got <- d })
	return got
}

func (f *streamsMQFixture) await(t *testing.T, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func recvDelivery(t *testing.T, ch chan converge.Delivery) converge.Delivery {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a delivery")
		return nil
	}
}

func assertNoDelivery(t *testing.T, ch chan converge.Delivery) {
	t.Helper()
	select {
	case d := <-ch:
		t.Fatalf("unexpected delivery: %+v", d.Message())
	case <-time.After(200 * time.Millisecond):
	}
}

func TestStreamsMQStreamKeyIsTheQueueNameVerbatim(t *testing.T) {
	client, clock, _ := openMini(t)
	mq := convredis.NewStreamsMQ(client, convredis.StreamsOpts{Clock: clock})
	ctx := context.Background()
	const queue = "dify:credential:rotate"
	if err := mq.Publish(ctx, queue, converge.Message{Payload: []byte(`{"workspace_id":"ws-1"}`)}); err != nil {
		t.Fatal(err)
	}
	if n, err := client.XLen(ctx, queue).Result(); err != nil || n != 1 {
		t.Fatalf("XLEN %q = %d, %v; want 1: the declared name is the stream key", queue, n, err)
	}
	if prefixed, err := client.Keys(ctx, "convredis:s:*").Result(); err != nil || len(prefixed) != 0 {
		t.Fatalf("keys convredis:s:* = %v, %v; want none", prefixed, err)
	}
	values, err := client.XRange(ctx, queue, "-", "+").Result()
	if err != nil || len(values) != 1 {
		t.Fatalf("XRANGE = %v, %v", values, err)
	}
	if got := values[0].Values["payload"]; got != `{"workspace_id":"ws-1"}` {
		t.Fatalf("payload field = %v", got)
	}
}
