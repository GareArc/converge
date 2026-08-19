package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/hook"
)

type DeadLetter struct {
	Task           string            `json:"task"`
	Queue          string            `json:"queue"`
	MessageID      string            `json:"message_id"`
	Attempt        int               `json:"attempt"`
	Reason         string            `json:"reason"`
	Error          string            `json:"error,omitempty"`
	EnqueuedAt     time.Time         `json:"enqueued_at"`
	DeadLetteredAt time.Time         `json:"dead_lettered_at"`
	Headers        map[string]string `json:"headers,omitempty"`
	Payload        []byte            `json:"payload,omitempty"`
}

var ErrDeadLetterNotFound = errors.New("worker: dead letter not found")

const reasonUndecodableRecord = "undecodable-record"

type DLQ struct {
	kv        converge.KV
	mq        converge.MQ
	clock     converge.Clock
	namespace string
	job       string
	queueMQ   func(queue string) converge.MQ
}

func DLQFrom(rt *converge.Runtime, job string) (*DLQ, error) {
	w, err := hook.OpsDeps(rt)
	if err != nil {
		return nil, err
	}
	if job == "" {
		return nil, errors.New("worker: DLQFrom needs a job name")
	}
	q := &DLQ{clock: wallClock{}, namespace: w.Namespace, job: job}
	if kv, ok := w.KV.(converge.KV); ok && kv != nil {
		q.kv = kv
	}
	if m, ok := w.MQ.(converge.MQ); ok && m != nil {
		q.mq = m
	}
	if c, ok := w.Clock.(converge.Clock); ok && c != nil {
		q.clock = c
	}
	q.queueMQ = func(queue string) converge.MQ {
		if w.QueueMQ == nil {
			return nil
		}
		if m, ok := w.QueueMQ(queue).(converge.MQ); ok {
			return m
		}
		return nil
	}
	return q, nil
}

func (q *DLQ) requireKV() error {
	if q.kv == nil {
		return fmt.Errorf("worker: job %q: DLQ needs Options.KV", q.job)
	}
	return nil
}

func (q *DLQ) prefix() string {
	elems := make([]string, 0, 4)
	if q.namespace != "" {
		elems = append(elems, q.namespace)
	}
	elems = append(elems, "converge", "worker", q.job, "dlq")
	return strings.Join(elems, "/") + "/"
}

func (q *DLQ) key(messageID string) string { return q.prefix() + messageID }

func toDeadLetter(id string, raw []byte) DeadLetter {
	var rec dlqRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return DeadLetter{MessageID: id, Reason: reasonUndecodableRecord, Error: err.Error()}
	}
	return DeadLetter(rec)
}

func (q *DLQ) List(ctx context.Context) ([]DeadLetter, error) {
	if err := q.requireKV(); err != nil {
		return nil, err
	}
	prefix := q.prefix()
	var out []DeadLetter
	seen := map[string]struct{}{}
	cursor := ""
	for {
		keys, next, err := q.kv.Scan(ctx, prefix, cursor)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			raw, ok, err := q.kv.Get(ctx, key)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			out = append(out, toDeadLetter(strings.TrimPrefix(key, prefix), raw))
		}
		if next == "" {
			break
		}
		cursor = next
	}
	return out, nil
}

func (q *DLQ) Get(ctx context.Context, messageID string) (DeadLetter, error) {
	if err := q.requireKV(); err != nil {
		return DeadLetter{}, err
	}
	raw, ok, err := q.kv.Get(ctx, q.key(messageID))
	if err != nil {
		return DeadLetter{}, err
	}
	if !ok {
		return DeadLetter{}, ErrDeadLetterNotFound
	}
	return toDeadLetter(messageID, raw), nil
}

func (q *DLQ) resolveQueue(queue string) (converge.MQ, error) {
	if q.queueMQ != nil {
		if m := q.queueMQ(queue); m != nil {
			return m, nil
		}
	}
	if q.mq != nil {
		return q.mq, nil
	}
	return nil, fmt.Errorf("worker: queue %q: no handler binding and no default Options.MQ", queue)
}

func (q *DLQ) Requeue(ctx context.Context, messageID string) error {
	if err := q.requireKV(); err != nil {
		return err
	}
	key := q.key(messageID)
	raw, ok, err := q.kv.Get(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return ErrDeadLetterNotFound
	}
	var rec dlqRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return fmt.Errorf("worker: job %q: requeue %q: %w", q.job, messageID, err)
	}
	mq, err := q.resolveQueue(rec.Queue)
	if err != nil {
		return err
	}
	headers := maps.Clone(rec.Headers)
	if headers == nil {
		headers = map[string]string{}
	}
	delete(headers, converge.HeaderAttempt)
	delete(headers, converge.HeaderSnoozes)
	headers[converge.HeaderEnqueuedAt] = q.clock.Now().UTC().Format(time.RFC3339Nano)
	msg := converge.Message{Kind: rec.Task, Headers: headers, Payload: rec.Payload}
	if err := mq.Publish(ctx, rec.Queue, msg); err != nil {
		return err
	}
	if err := q.kv.Delete(ctx, key); err != nil {
		return fmt.Errorf("worker: job %q: requeue %q: republished but record not purged: %w", q.job, messageID, err)
	}
	return nil
}

func (q *DLQ) Purge(ctx context.Context, messageID string) error {
	if err := q.requireKV(); err != nil {
		return err
	}
	return q.kv.Delete(ctx, q.key(messageID))
}

func (q *DLQ) PurgeAll(ctx context.Context) (int, error) {
	if err := q.requireKV(); err != nil {
		return 0, err
	}
	prefix := q.prefix()
	count := 0
	cursor := ""
	for {
		keys, next, err := q.kv.Scan(ctx, prefix, cursor)
		if err != nil {
			return count, err
		}
		for _, key := range keys {
			if err := q.kv.Delete(ctx, key); err != nil {
				return count, err
			}
			count++
		}
		if next == "" {
			break
		}
		cursor = next
	}
	return count, nil
}
