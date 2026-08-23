package reconcile

import (
	"context"
	"strconv"
	"strings"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/keys"
)

type parkStore interface {
	enabled() bool
	read(ctx context.Context, id ID) (Version, bool)
	mark(ctx context.Context, id ID, v Version)
	clear(ctx context.Context, id ID)
	scan(ctx context.Context, visit func(ID))
}

func (e *engine) newParkStore(deps converge.JobDeps) parkStore {
	if deps.KV == nil || e.cfg.runMode == converge.OnAllReplicas {
		return noopParkStore{}
	}
	return &kvParkStore{
		kv:        deps.KV,
		namespace: deps.Namespace,
		job:       e.cfg.name,
		retry:     e.pauseOnInfraError,
	}
}

type kvParkStore struct {
	kv        converge.KV
	namespace string
	job       string
	retry     func(context.Context) bool
}

func (s *kvParkStore) enabled() bool { return true }

func (s *kvParkStore) key(id ID) string {
	return keys.ReconcileParked(s.namespace, s.job, string(id))
}

func (s *kvParkStore) read(ctx context.Context, id ID) (Version, bool) {
	for {
		val, ok, err := s.kv.Get(ctx, s.key(id))
		if err == nil {
			if !ok {
				return 0, false
			}
			n, perr := strconv.ParseUint(string(val), 10, 64)
			if perr != nil {
				return 0, false
			}
			return Version(n), true
		}
		if !s.retry(ctx) {
			return 0, false
		}
	}
}

func (s *kvParkStore) mark(ctx context.Context, id ID, v Version) {
	val := []byte(strconv.FormatUint(uint64(v), 10))
	for {
		if err := s.kv.Set(ctx, s.key(id), val, 0); err == nil {
			return
		}
		if !s.retry(ctx) {
			return
		}
	}
}

func (s *kvParkStore) clear(ctx context.Context, id ID) {
	s.kv.Delete(ctx, s.key(id))
}

func (s *kvParkStore) scan(ctx context.Context, visit func(ID)) {
	prefix := keys.ReconcileParkedPrefix(s.namespace, s.job)
	cursor := ""
	for {
		found, next, err := s.kv.Scan(ctx, prefix, cursor)
		if err != nil {
			if !s.retry(ctx) {
				return
			}
			continue
		}
		for _, k := range found {
			visit(ID(strings.TrimPrefix(k, prefix)))
		}
		if next == "" {
			return
		}
		cursor = next
	}
}

type noopParkStore struct{}

func (noopParkStore) enabled() bool { return false }

func (noopParkStore) read(context.Context, ID) (Version, bool) { return 0, false }

func (noopParkStore) mark(context.Context, ID, Version) {}

func (noopParkStore) clear(context.Context, ID) {}

func (noopParkStore) scan(context.Context, func(ID)) {}
