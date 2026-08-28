package versions

import (
	"context"
	"fmt"
	"sync"

	"github.com/GareArc/converge/reconcile"
)

type Source struct {
	mu     sync.Mutex
	latest map[reconcile.ID]reconcile.Version
}

func Fixed(latest map[string]reconcile.Version) *Source {
	s := &Source{latest: make(map[reconcile.ID]reconcile.Version, len(latest))}
	for id, ver := range latest {
		s.latest[reconcile.ID(id)] = ver
	}
	return s
}

func (s *Source) Latest(ctx context.Context, id reconcile.ID) (reconcile.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ver, ok := s.latest[id]
	if !ok {
		return 0, fmt.Errorf("convergetest/versions: no version recorded for id %q", id)
	}
	return ver, nil
}

func (s *Source) Bump(id string) reconcile.Version {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest[reconcile.ID(id)]++
	return s.latest[reconcile.ID(id)]
}
