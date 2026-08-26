package worker

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/GareArc/converge"
)

type envelope struct {
	d converge.Delivery
	m converge.Message
}

func newEnvelope(d converge.Delivery, m converge.Message) envelope {
	return envelope{d: d, m: m}
}

func (e envelope) attempt() (int, bool) {
	raw, ok := e.m.Headers[converge.HeaderAttempt]
	if !ok {
		return e.d.Attempt(), true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 || n > math.MaxInt/2 {
		return e.d.Attempt(), false
	}
	return n + e.d.Attempt(), true
}

func (e envelope) messageID() string {
	if id := e.m.Headers[converge.HeaderMessageID]; id != "" {
		return id
	}
	sum := sha256.New()
	sum.Write([]byte(e.m.Kind))
	sum.Write(e.m.Payload)
	return "anon-" + hex.EncodeToString(sum.Sum(nil)[:16])
}

func (e envelope) enqueuedAt() time.Time {
	if ts, err := time.Parse(time.RFC3339Nano, e.m.Headers[converge.HeaderEnqueuedAt]); err == nil {
		return ts
	}
	return e.d.EnqueuedAt()
}

func (e envelope) schemaVersion() string { return e.m.Headers[converge.HeaderSchemaVersion] }

func (e envelope) snoozes() int {
	n, _ := strconv.Atoi(e.m.Headers[converge.HeaderSnoozes])
	return n
}

func (e envelope) fold() map[string]string {
	h := maps.Clone(e.m.Headers)
	if h == nil {
		h = map[string]string{}
	}
	a, _ := e.attempt()
	h[converge.HeaderAttempt] = strconv.Itoa(a - 1)
	return h
}

func (e envelope) forSnooze() converge.Message {
	h := e.fold()
	h[converge.HeaderSnoozes] = strconv.Itoa(e.snoozes() + 1)
	return converge.Message{Kind: e.m.Kind, Headers: h, Payload: e.m.Payload}
}

func (e envelope) forNeutral() converge.Message {
	return converge.Message{Kind: e.m.Kind, Headers: e.fold(), Payload: e.m.Payload}
}

func seedMessage(kind string, version int, now time.Time, headers map[string]string, payload []byte) (converge.Message, error) {
	for k := range headers {
		if strings.HasPrefix(k, converge.HeaderPrefix) {
			return converge.Message{}, fmt.Errorf("header %q uses the reserved %q prefix", k, converge.HeaderPrefix)
		}
	}
	h := maps.Clone(headers)
	if h == nil {
		h = map[string]string{}
	}
	h[converge.HeaderMessageID] = newID()
	h[converge.HeaderSchemaVersion] = strconv.Itoa(version)
	h[converge.HeaderEnqueuedAt] = now.UTC().Format(time.RFC3339Nano)
	h[converge.HeaderAttempt] = "0"
	return converge.Message{Kind: kind, Headers: h, Payload: payload}, nil
}

func requeueMessage(rec ShelvedMessage, now time.Time) converge.Message {
	h := maps.Clone(rec.Headers)
	if h == nil {
		h = map[string]string{}
	}
	h[converge.HeaderAttempt] = "0"
	delete(h, converge.HeaderSnoozes)
	h[converge.HeaderEnqueuedAt] = now.UTC().Format(time.RFC3339Nano)
	return converge.Message{Kind: rec.Task, Headers: h, Payload: rec.Payload}
}

func newID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
