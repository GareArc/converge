package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/GareArc/converge"
)

type Version uint64

type VersionSource interface {
	Latest(ctx context.Context, id ID) (Version, error)
}

type Tracker struct {
	kv        converge.KV
	namespace string
	err       error
}

func NewTracker(kv converge.KV, namespace string) *Tracker {
	t := &Tracker{kv: kv, namespace: namespace}
	if kv == nil {
		t.err = errors.New("reconcile: Tracker needs a KV")
	} else if namespace == "" {
		t.err = errors.New("reconcile: Tracker needs a namespace")
	} else if strings.Contains(namespace, "/") {
		t.err = errors.New(`reconcile: Tracker namespace must not contain "/"`)
	}
	return t
}

func (t *Tracker) key(id ID) string {
	return "converge/tracker/" + t.namespace + "/" + string(id)
}

func (t *Tracker) read(ctx context.Context, id ID) (Version, []byte, error) {
	raw, ok, err := t.kv.Get(ctx, t.key(id))
	if err != nil {
		return 0, nil, err
	}
	if !ok {
		return 0, nil, nil
	}
	n, perr := strconv.ParseUint(string(raw), 10, 64)
	if perr != nil {
		return 0, nil, fmt.Errorf("reconcile: tracker %q: corrupt version %q for id %q", t.namespace, raw, string(id))
	}
	return Version(n), raw, nil
}

func (t *Tracker) MarkChanged(ctx context.Context, id ID) (Version, error) {
	if t.err != nil {
		return 0, t.err
	}
	for {
		cur, raw, err := t.read(ctx, id)
		if err != nil {
			return 0, err
		}
		next := cur + 1
		ok, err := t.kv.SetCAS(ctx, t.key(id), raw, []byte(strconv.FormatUint(uint64(next), 10)))
		if err != nil {
			return 0, err
		}
		if ok {
			return next, nil
		}
	}
}

func (t *Tracker) Latest(ctx context.Context, id ID) (Version, error) {
	if t.err != nil {
		return 0, t.err
	}
	v, _, err := t.read(ctx, id)
	return v, err
}

func (t *Tracker) MarkApplied(ctx context.Context, id ID, v Version) error {
	if t.err != nil {
		return t.err
	}
	latest, _, err := t.read(ctx, id)
	if err != nil {
		return err
	}
	if latest > v {
		return ErrOutdated
	}
	return nil
}

func (t *Tracker) Forget(ctx context.Context, id ID) error {
	if t.err != nil {
		return t.err
	}
	return t.kv.Delete(ctx, t.key(id))
}
