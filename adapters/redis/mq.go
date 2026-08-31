package convredis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/GareArc/converge"
	"github.com/redis/go-redis/v9"
)

const (
	DefaultVisibility = 5 * time.Minute

	reservedGroup    = "converge"
	blockInterval    = 100 * time.Millisecond
	handoffTimeout   = 5 * time.Second
	readCount        = 16
	pendingPageCount = 64
	randomIDBytes    = 8
	busyGroupPrefix  = "BUSYGROUP"
	noGroupPrefix    = "NOGROUP"
	groupStartID     = "0"
	newEntriesID     = ">"
	broadcastStartID = "$"
	pendingRangeMin  = "-"
	pendingRangeMax  = "+"
)

const (
	fieldKind       = "kind"
	fieldPayload    = "payload"
	fieldHeaders    = "headers"
	fieldEnqueuedAt = "enq"
)

const (
	groupFieldName    = "name"
	groupFieldPending = "pending"
	groupFieldLag     = "lag"
)

type StreamsOpts struct {
	Clock      converge.Clock
	Retention  time.Duration
	Visibility time.Duration
}

func NewStreamsMQ(rdb *redis.Client, o StreamsOpts) *StreamsMQ {
	m := &StreamsMQ{rdb: rdb, clock: o.Clock, visibility: o.Visibility, retention: o.Retention}
	if m.clock == nil {
		m.clock = wallClock{}
	}
	if m.visibility <= 0 {
		m.visibility = DefaultVisibility
	}
	return m
}

type StreamsMQ struct {
	rdb        *redis.Client
	clock      converge.Clock
	visibility time.Duration
	retention  time.Duration
}

type wallClock struct{}

func (wallClock) Now() time.Time                         { return time.Now() }
func (wallClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func pendingKey(q, g string) string  { return "convredis:p:" + q + ":" + g }
func attemptsKey(q, g string) string { return "convredis:a:" + q + ":" + g }
func delayedKey(queue string) string { return "convredis:d:" + queue }

func dueScore(t time.Time) float64 { return float64(t.UnixMilli()) }

type delayedRecord struct {
	Nonce   string            `json:"nonce"`
	Kind    string            `json:"kind"`
	Headers map[string]string `json:"headers,omitempty"`
	Payload []byte            `json:"payload,omitempty"`
}

func (m *StreamsMQ) Publish(ctx context.Context, queue string, msg converge.Message) error {
	values, err := encodeMessage(msg, m.clock.Now())
	if err != nil {
		return err
	}
	if err := m.rdb.XAdd(ctx, &redis.XAddArgs{Stream: queue, Values: values}).Err(); err != nil {
		return err
	}
	return m.trimRetention(ctx, queue)
}

func (m *StreamsMQ) trimRetention(ctx context.Context, key string) error {
	if m.retention <= 0 {
		return nil
	}
	return m.rdb.XTrimMinID(ctx, key, retentionMinID(m.clock.Now(), m.retention)).Err()
}

func retentionMinID(now time.Time, retention time.Duration) string {
	return strconv.FormatInt(now.Add(-retention).UnixMilli(), 10)
}

func (m *StreamsMQ) Backlog(ctx context.Context, queue string) (int, error) {
	return m.BacklogForGroup(ctx, queue, reservedGroup)
}

func (m *StreamsMQ) BacklogForGroup(ctx context.Context, queue, group string) (int, error) {
	entries, err := m.rdb.XLen(ctx, queue).Result()
	if err != nil {
		return 0, err
	}
	if entries == 0 {
		return 0, nil
	}
	reply, err := m.rdb.Do(ctx, "xinfo", "groups", queue).Result()
	if err != nil {
		return 0, err
	}
	g, found := findGroupBacklog(reply, group)
	if !found {
		return int(entries), nil
	}
	if !g.lagKnown {
		return 0, converge.ErrBacklogUnknown
	}
	return int(g.lag + g.pending), nil
}

type groupBacklog struct {
	lag      int64
	lagKnown bool
	pending  int64
}

func findGroupBacklog(reply any, group string) (groupBacklog, bool) {
	groups, ok := reply.([]any)
	if !ok {
		return groupBacklog{}, false
	}
	for _, entry := range groups {
		fields, ok := replyFields(entry)
		if !ok {
			continue
		}
		if name, _ := fields[groupFieldName].(string); name != group {
			continue
		}
		lag, lagKnown := fields[groupFieldLag].(int64)
		pending, _ := fields[groupFieldPending].(int64)
		return groupBacklog{lag: lag, lagKnown: lagKnown, pending: pending}, true
	}
	return groupBacklog{}, false
}

func replyFields(entry any) (map[string]any, bool) {
	switch v := entry.(type) {
	case map[any]any:
		out := make(map[string]any, len(v))
		for k, val := range v {
			if name, ok := k.(string); ok {
				out[name] = val
			}
		}
		return out, true
	case []any:
		if len(v)%2 != 0 {
			return nil, false
		}
		out := make(map[string]any, len(v)/2)
		for i := 0; i < len(v); i += 2 {
			name, ok := v[i].(string)
			if !ok {
				return nil, false
			}
			out[name] = v[i+1]
		}
		return out, true
	}
	return nil, false
}

func (m *StreamsMQ) PublishDelayed(ctx context.Context, queue string, msg converge.Message, delay time.Duration) error {
	nonce, err := randomID()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(delayedRecord{Nonce: nonce, Kind: msg.Kind, Headers: msg.Headers, Payload: msg.Payload})
	if err != nil {
		return err
	}
	due := m.clock.Now().Add(delay)
	return m.rdb.ZAdd(ctx, delayedKey(queue), redis.Z{Score: dueScore(due), Member: string(raw)}).Err()
}

func (m *StreamsMQ) Consume(ctx context.Context, queue string, deliver func(converge.Delivery)) error {
	return m.consumeGroup(ctx, queue, reservedGroup, deliver)
}

func (m *StreamsMQ) ConsumeGroup(ctx context.Context, queue, group string, deliver func(converge.Delivery)) error {
	return m.consumeGroup(ctx, queue, group, deliver)
}

func (m *StreamsMQ) ConsumeBroadcast(ctx context.Context, queue string, deliver func(converge.Delivery)) error {
	last := broadcastStartID
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := m.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{queue, last},
			Count:   readCount,
			Block:   blockInterval,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			if !pause(ctx) {
				return ctx.Err()
			}
			continue
		}
		for _, stream := range res {
			for _, entry := range stream.Messages {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				last = entry.ID
				d, ok := newBroadcastDelivery(entry)
				if !ok {
					continue
				}
				deliver(d)
			}
		}
	}
}

func (m *StreamsMQ) consumeGroup(ctx context.Context, queue, group string, deliver func(converge.Delivery)) error {
	if err := m.ensureGroup(ctx, queue, group); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	consumer, err := randomID()
	if err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := m.moveDue(ctx, queue, group, consumer, deliver); err != nil {
			if !m.resume(ctx, queue, group, err) {
				return ctx.Err()
			}
			continue
		}
		entries, err := m.readNew(ctx, queue, group, consumer)
		if err != nil {
			if !m.resume(ctx, queue, group, err) {
				return ctx.Err()
			}
			continue
		}
		for _, entry := range entries {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := m.deliverEntry(ctx, queue, group, entry, deliver); err != nil {
				break
			}
		}
	}
}

func (m *StreamsMQ) resume(ctx context.Context, queue, group string, err error) bool {
	if strings.HasPrefix(err.Error(), noGroupPrefix) && m.ensureGroup(ctx, queue, group) == nil {
		return true
	}
	return pause(ctx)
}

func pause(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	timer := time.NewTimer(blockInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (m *StreamsMQ) ensureGroup(ctx context.Context, queue, group string) error {
	if err := m.trimRetention(ctx, queue); err != nil {
		return err
	}
	err := m.rdb.XGroupCreateMkStream(ctx, queue, group, groupStartID).Err()
	if err != nil && strings.HasPrefix(err.Error(), busyGroupPrefix) {
		return nil
	}
	return err
}

func (m *StreamsMQ) readNew(ctx context.Context, queue, group, consumer string) ([]redis.XMessage, error) {
	res, err := m.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{queue, newEntriesID},
		Count:    readCount,
		Block:    blockInterval,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var entries []redis.XMessage
	for _, stream := range res {
		entries = append(entries, stream.Messages...)
	}
	return entries, nil
}

func (m *StreamsMQ) moveDue(ctx context.Context, queue, group, consumer string, deliver func(converge.Delivery)) error {
	if err := m.releaseDelayed(ctx, queue); err != nil {
		return err
	}
	if err := m.reconcilePending(ctx, queue, group); err != nil {
		return err
	}
	return m.redeliverDue(ctx, queue, group, consumer, deliver)
}

func (m *StreamsMQ) claimDue(ctx context.Context, key string, grace time.Duration) ([]string, error) {
	now := m.clock.Now()
	members, err := claimDueScript.Run(ctx, m.rdb, []string{key},
		dueScore(now), dueScore(now.Add(grace)), readCount).StringSlice()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return members, err
}

func (m *StreamsMQ) releaseDelayed(ctx context.Context, queue string) error {
	key := delayedKey(queue)
	records, err := m.claimDue(ctx, key, m.visibility)
	if err != nil || len(records) == 0 {
		return err
	}
	hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), handoffTimeout)
	defer cancel()
	for _, raw := range records {
		var rec delayedRecord
		if err := json.Unmarshal([]byte(raw), &rec); err == nil {
			msg := converge.Message{Kind: rec.Kind, Headers: rec.Headers, Payload: rec.Payload}
			if err := m.Publish(hctx, queue, msg); err != nil {
				return err
			}
		}
		if err := m.rdb.ZRem(hctx, key, raw).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (m *StreamsMQ) reconcilePending(ctx context.Context, queue, group string) error {
	start := pendingRangeMin
	for {
		pending, err := m.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: queue,
			Group:  group,
			Start:  start,
			End:    pendingRangeMax,
			Count:  pendingPageCount,
		}).Result()
		if errors.Is(err, redis.Nil) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(pending) == 0 {
			return nil
		}
		score := dueScore(m.clock.Now().Add(m.visibility))
		members := make([]redis.Z, 0, len(pending))
		for _, entry := range pending {
			members = append(members, redis.Z{Score: score, Member: entry.ID})
		}
		if err := m.rdb.ZAddNX(ctx, pendingKey(queue, group), members...).Err(); err != nil {
			return err
		}
		if len(pending) < pendingPageCount {
			return nil
		}
		next := nextEntryID(pending[len(pending)-1].ID)
		if !entryIDAdvanced(next, start) {
			return nil
		}
		start = next
	}
}

func nextEntryID(id string) string {
	ms, seq, ok := strings.Cut(id, "-")
	if !ok {
		return id
	}
	n, err := strconv.ParseUint(seq, 10, 64)
	if err != nil {
		return id
	}
	return ms + "-" + strconv.FormatUint(n+1, 10)
}

func entryIDAdvanced(next, start string) bool {
	nms, nseq, nok := entryIDParts(next)
	sms, sseq, sok := entryIDParts(start)
	if !nok || !sok {
		return next > start
	}
	if nms != sms {
		return nms > sms
	}
	return nseq > sseq
}

func entryIDParts(id string) (ms, seq uint64, ok bool) {
	m, s, found := strings.Cut(id, "-")
	if !found {
		return 0, 0, false
	}
	msVal, err := strconv.ParseUint(m, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	seqVal, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return msVal, seqVal, true
}

func (m *StreamsMQ) redeliverDue(ctx context.Context, queue, group, consumer string, deliver func(converge.Delivery)) error {
	ids, err := m.claimDue(ctx, pendingKey(queue, group), m.visibility)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		entries, err := m.rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream:   queue,
			Group:    group,
			Consumer: consumer,
			Messages: []string{id},
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		if len(entries) == 0 {
			if err := m.forget(ctx, queue, group, id); err != nil {
				return err
			}
			continue
		}
		for _, entry := range entries {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := m.deliverEntry(ctx, queue, group, entry, deliver); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *StreamsMQ) deliverEntry(ctx context.Context, queue, group string, entry redis.XMessage, deliver func(converge.Delivery)) error {
	msg, enq, err := decodeMessage(entry.Values)
	if err != nil {
		return m.ack(ctx, queue, group, entry.ID)
	}
	attempt, err := m.startAttempt(ctx, queue, group, entry.ID)
	if err != nil {
		return err
	}
	deliver(&streamDelivery{
		mq:      m,
		queue:   queue,
		group:   group,
		id:      entry.ID,
		msg:     msg,
		attempt: attempt,
		enq:     enq,
	})
	return nil
}

func (m *StreamsMQ) ack(ctx context.Context, queue, group, id string) error {
	if err := m.ackStream(ctx, queue, group, id); err != nil {
		return err
	}
	return m.forget(ctx, queue, group, id)
}

func (m *StreamsMQ) ackStream(ctx context.Context, queue, group, id string) error {
	return m.rdb.XAck(ctx, queue, group, id).Err()
}

func (m *StreamsMQ) ackAttempt(ctx context.Context, queue, group, id string, attempt int) (bool, error) {
	n, err := ackScript.Run(ctx, m.rdb,
		[]string{attemptsKey(queue, group), queue},
		id, strconv.Itoa(attempt), group).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (m *StreamsMQ) deferTo(ctx context.Context, queue, group, id string, attempt int, after time.Duration) (bool, error) {
	n, err := deferScript.Run(ctx, m.rdb,
		[]string{pendingKey(queue, group), attemptsKey(queue, group)},
		id, strconv.Itoa(attempt), dueScore(m.clock.Now().Add(after))).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (m *StreamsMQ) forget(ctx context.Context, queue, group, id string) error {
	pipe := m.rdb.Pipeline()
	pipe.ZRem(ctx, pendingKey(queue, group), id)
	pipe.HDel(ctx, attemptsKey(queue, group), id)
	_, err := pipe.Exec(ctx)
	return err
}

func (m *StreamsMQ) startAttempt(ctx context.Context, queue, group, id string) (int, error) {
	pipe := m.rdb.Pipeline()
	pipe.ZAdd(ctx, pendingKey(queue, group), redis.Z{Score: dueScore(m.clock.Now().Add(m.visibility)), Member: id})
	attempt := pipe.HIncrBy(ctx, attemptsKey(queue, group), id, 1)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return int(attempt.Val()), nil
}

func encodeMessage(msg converge.Message, enq time.Time) (map[string]any, error) {
	headers, err := json.Marshal(msg.Headers)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		fieldKind:       msg.Kind,
		fieldPayload:    string(msg.Payload),
		fieldHeaders:    string(headers),
		fieldEnqueuedAt: enq.UTC().Format(time.RFC3339Nano),
	}, nil
}

func decodeMessage(values map[string]any) (converge.Message, time.Time, error) {
	msg := converge.Message{Kind: stringField(values, fieldKind)}
	if payload := stringField(values, fieldPayload); payload != "" {
		msg.Payload = []byte(payload)
	}
	if headers := stringField(values, fieldHeaders); headers != "" {
		if err := json.Unmarshal([]byte(headers), &msg.Headers); err != nil {
			return converge.Message{}, time.Time{}, err
		}
	}
	enq, err := time.Parse(time.RFC3339Nano, stringField(values, fieldEnqueuedAt))
	if err != nil {
		return converge.Message{}, time.Time{}, err
	}
	return msg, enq, nil
}

func stringField(values map[string]any, name string) string {
	s, _ := values[name].(string)
	return s
}

func randomID() (string, error) {
	buf := make([]byte, randomIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
