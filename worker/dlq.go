package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/keys"
	"github.com/GareArc/converge/internal/wiring"
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
}

func DLQFrom(rt *converge.Runtime, job string) (*DLQ, error) {
	w, err := wiring.OpsFor(rt)
	if err != nil {
		return nil, err
	}
	if job == "" {
		return nil, errors.New("worker: DLQFrom needs a job name")
	}
	if w.Clock == nil {
		return nil, fmt.Errorf("worker: job %q: DLQ needs a runtime clock", job)
	}
	return &DLQ{kv: w.KV, mq: w.MQ, clock: w.Clock, namespace: w.Namespace, job: job}, nil
}

func (q *DLQ) requireKV() error {
	if q.kv == nil {
		return fmt.Errorf("worker: job %q: DLQ needs Options.KV", q.job)
	}
	return nil
}

func (q *DLQ) prefix() string { return keys.WorkerDLQPrefix(q.namespace, q.job) }

func (q *DLQ) key(messageID string) string { return keys.WorkerDLQ(q.namespace, q.job, messageID) }

func toDeadLetter(id string, raw []byte) DeadLetter {
	var rec DeadLetter
	if err := json.Unmarshal(raw, &rec); err != nil {
		return DeadLetter{MessageID: id, Reason: reasonUndecodableRecord, Error: err.Error()}
	}
	return rec
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
	var rec DeadLetter
	if err := json.Unmarshal(raw, &rec); err != nil {
		return fmt.Errorf("worker: job %q: requeue %q: %w", q.job, messageID, err)
	}
	if q.mq == nil {
		return fmt.Errorf("worker: job %q: requeue %q: needs Options.MQ", q.job, messageID)
	}
	msg := requeueMessage(rec, q.clock.Now())
	if err := q.mq.Publish(ctx, rec.Queue, msg); err != nil {
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
