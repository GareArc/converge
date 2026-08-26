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

type ShelvedMessage struct {
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

var ErrNotShelved = errors.New("worker: not shelved")

const reasonUndecodableRecord = "undecodable-record"

type Shelf struct {
	kv        converge.KV
	mq        converge.MQ
	clock     converge.Clock
	namespace string
	job       string
}

func ShelfFrom(rt *converge.Runtime, job string) (*Shelf, error) {
	w, err := wiring.OpsFor(rt)
	if err != nil {
		return nil, err
	}
	if job == "" {
		return nil, errors.New("worker: ShelfFrom needs a job name")
	}
	if w.Clock == nil {
		return nil, fmt.Errorf("worker: job %q: Shelf needs a runtime clock", job)
	}
	return &Shelf{kv: w.KV, mq: w.MQ, clock: w.Clock, namespace: w.Namespace, job: job}, nil
}

func (s *Shelf) requireKV() error {
	if s.kv == nil {
		return fmt.Errorf("worker: job %q: Shelf needs Options.KV", s.job)
	}
	return nil
}

func (s *Shelf) prefix() string { return keys.WorkerShelfPrefix(s.namespace, s.job) }

func (s *Shelf) key(messageID string) string { return keys.WorkerShelf(s.namespace, s.job, messageID) }

func toShelved(id string, raw []byte) ShelvedMessage {
	var rec ShelvedMessage
	if err := json.Unmarshal(raw, &rec); err != nil {
		return ShelvedMessage{MessageID: id, Reason: reasonUndecodableRecord, Error: err.Error()}
	}
	return rec
}

func (s *Shelf) List(ctx context.Context) ([]ShelvedMessage, error) {
	if err := s.requireKV(); err != nil {
		return nil, err
	}
	prefix := s.prefix()
	var out []ShelvedMessage
	seen := map[string]struct{}{}
	cursor := ""
	for {
		keys, next, err := s.kv.Scan(ctx, prefix, cursor)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			raw, ok, err := s.kv.Get(ctx, key)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			out = append(out, toShelved(strings.TrimPrefix(key, prefix), raw))
		}
		if next == "" {
			break
		}
		cursor = next
	}
	return out, nil
}

func (s *Shelf) Get(ctx context.Context, messageID string) (ShelvedMessage, error) {
	if err := s.requireKV(); err != nil {
		return ShelvedMessage{}, err
	}
	raw, ok, err := s.kv.Get(ctx, s.key(messageID))
	if err != nil {
		return ShelvedMessage{}, err
	}
	if !ok {
		return ShelvedMessage{}, ErrNotShelved
	}
	return toShelved(messageID, raw), nil
}

func (s *Shelf) Requeue(ctx context.Context, messageID string) error {
	if err := s.requireKV(); err != nil {
		return err
	}
	key := s.key(messageID)
	raw, ok, err := s.kv.Get(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotShelved
	}
	var rec ShelvedMessage
	if err := json.Unmarshal(raw, &rec); err != nil {
		return fmt.Errorf("worker: job %q: requeue %q: %w", s.job, messageID, err)
	}
	if s.mq == nil {
		return fmt.Errorf("worker: job %q: requeue %q: needs Options.MQ", s.job, messageID)
	}
	msg := requeueMessage(rec, s.clock.Now())
	if err := s.mq.Publish(ctx, rec.Queue, msg); err != nil {
		return err
	}
	if err := s.kv.Delete(ctx, key); err != nil {
		return fmt.Errorf("worker: job %q: requeue %q: republished but record not purged: %w", s.job, messageID, err)
	}
	return nil
}

func (s *Shelf) Purge(ctx context.Context, messageID string) error {
	if err := s.requireKV(); err != nil {
		return err
	}
	return s.kv.Delete(ctx, s.key(messageID))
}

func (s *Shelf) PurgeAll(ctx context.Context) error {
	if err := s.requireKV(); err != nil {
		return err
	}
	prefix := s.prefix()
	cursor := ""
	for {
		keys, next, err := s.kv.Scan(ctx, prefix, cursor)
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := s.kv.Delete(ctx, key); err != nil {
				return err
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}
	return nil
}
