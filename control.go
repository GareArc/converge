package converge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/GareArc/converge/internal/ctl"
)

const (
	controlResponseTTL          = 60 * time.Second
	controlPollInterval         = 50 * time.Millisecond
	defaultControlTimeout       = 2 * time.Second
	controlConsumeRetryInterval = time.Second
	controlPausedValue          = "1"
)

var controlVerbs = map[string]func(ctx context.Context, j job, id string) error{
	ctl.OpPoke: func(_ context.Context, j job, id string) error { return j.Poke(id) },
	ctl.OpHint: func(_ context.Context, j job, id string) error { return j.Hint(id) },
	ctl.OpRunPass: func(ctx context.Context, j job, _ string) error {
		return j.RunPassNow(ctx)
	},
	ctl.OpPause:  func(_ context.Context, j job, _ string) error { j.SetPaused(true); return nil },
	ctl.OpResume: func(_ context.Context, j job, _ string) error { j.SetPaused(false); return nil },
}

func (rt *Runtime) applyPausedFlags(ctx context.Context, jobs []job) error {
	for _, j := range jobs {
		_, ok, err := rt.opts.KV.Get(ctx, ctl.PausedKey(rt.opts.Namespace, j.Name()))
		if err != nil {
			return err
		}
		if ok {
			j.SetPaused(true)
		}
	}
	return nil
}

func (rt *Runtime) startControlListener(ctx context.Context) {
	bc, ok := rt.opts.MQ.(BroadcastConsumer)
	if !ok || rt.opts.KV == nil {
		return
	}
	go rt.consumeControl(ctx, bc)
}

func (rt *Runtime) consumeControl(ctx context.Context, bc BroadcastConsumer) {
	queue := ctl.Queue(rt.opts.Namespace)
	deliver := func(d Delivery) { rt.handleControlDelivery(ctx, d) }
	for {
		bc.ConsumeBroadcast(ctx, queue, deliver)
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-rt.opts.Clock.After(controlConsumeRetryInterval):
		}
	}
}

func (rt *Runtime) handleControlDelivery(ctx context.Context, d Delivery) {
	defer d.Ack(ctx)
	var cmd ctl.Command
	if err := json.Unmarshal(d.Message().Payload, &cmd); err != nil || cmd.OpID == "" {
		return
	}
	resp := rt.executeControlCommand(ctx, cmd.Op, cmd.Job, cmd.ID)
	rt.writeControlResponse(ctx, cmd.OpID, resp)
}

func (rt *Runtime) executeControlCommand(ctx context.Context, op, jobName, id string) ctl.Response {
	resp := ctl.Response{Replica: rt.replica, At: rt.opts.Clock.Now()}
	verb, ok := controlVerbs[op]
	if !ok {
		resp.Err = fmt.Sprintf("converge: unknown control op %q", op)
		return resp
	}
	rt.mu.Lock()
	j, ok := rt.jobs[jobName]
	rt.mu.Unlock()
	if !ok {
		resp.Err = fmt.Sprintf("converge: unknown job %q", jobName)
		return resp
	}
	err := verb(ctx, j, id)
	resp.Acted = err == nil
	if err != nil {
		resp.Err = err.Error()
	}
	return resp
}

func (rt *Runtime) writeControlResponse(ctx context.Context, opID string, resp ctl.Response) {
	if rt.opts.KV == nil {
		return
	}
	data, err := json.Marshal(resp)
	if err != nil {
		return
	}
	rt.opts.KV.Set(ctx, ctl.ResKey(rt.opts.Namespace, opID, rt.replica), data, controlResponseTTL)
}

func newControlOpID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

var controlEarlyReturnOps = map[string]bool{
	ctl.OpPoke:    true,
	ctl.OpHint:    true,
	ctl.OpRunPass: true,
}

func (rt *Runtime) controlDispatch(ctx context.Context, req ctl.Request) ([]ctl.Response, error) {
	timeout := req.Timeout
	if timeout == 0 {
		timeout = defaultControlTimeout
	}

	if req.Op == ctl.OpPause || req.Op == ctl.OpResume {
		if err := rt.writeControlPausedFlag(ctx, req); err != nil {
			return nil, err
		}
	}

	_, hasBroadcast := rt.opts.MQ.(BroadcastConsumer)
	if !hasBroadcast || rt.opts.KV == nil {
		return []ctl.Response{rt.executeControlCommand(ctx, req.Op, req.Job, req.ID)}, nil
	}
	return rt.dispatchControlBroadcast(ctx, req, timeout)
}

func (rt *Runtime) writeControlPausedFlag(ctx context.Context, req ctl.Request) error {
	if rt.opts.KV == nil {
		return nil
	}
	key := ctl.PausedKey(rt.opts.Namespace, req.Job)
	if req.Op == ctl.OpPause {
		return rt.opts.KV.Set(ctx, key, []byte(controlPausedValue), 0)
	}
	return rt.opts.KV.Delete(ctx, key)
}

func (rt *Runtime) dispatchControlBroadcast(ctx context.Context, req ctl.Request, timeout time.Duration) ([]ctl.Response, error) {
	opID, err := newControlOpID()
	if err != nil {
		return nil, err
	}
	cmd := ctl.Command{Op: req.Op, Job: req.Job, ID: req.ID, OpID: opID}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	if err := rt.opts.MQ.Publish(ctx, ctl.Queue(rt.opts.Namespace), Message{Payload: payload}); err != nil {
		return nil, err
	}

	prefix := ctl.ResPrefix(rt.opts.Namespace, opID)
	deadline := rt.opts.Clock.Now().Add(timeout)
	collected := map[string]ctl.Response{}
	early := controlEarlyReturnOps[req.Op]

	for {
		rt.collectControlResponses(ctx, prefix, collected)
		if early && anyActed(collected) {
			return sortControlResponses(collected), nil
		}
		if !rt.opts.Clock.Now().Before(deadline) {
			return sortControlResponses(collected), nil
		}
		select {
		case <-ctx.Done():
			return sortControlResponses(collected), ctx.Err()
		case <-rt.opts.Clock.After(controlPollInterval):
		}
	}
}

func anyActed(collected map[string]ctl.Response) bool {
	for _, r := range collected {
		if r.Acted {
			return true
		}
	}
	return false
}

func (rt *Runtime) collectControlResponses(ctx context.Context, prefix string, collected map[string]ctl.Response) {
	cursor := ""
	for {
		keys, next, err := rt.opts.KV.Scan(ctx, prefix, cursor)
		if err != nil {
			return
		}
		for _, key := range keys {
			raw, ok, err := rt.opts.KV.Get(ctx, key)
			if err != nil || !ok {
				continue
			}
			var resp ctl.Response
			if err := json.Unmarshal(raw, &resp); err != nil {
				continue
			}
			collected[resp.Replica] = resp
		}
		if next == "" {
			return
		}
		cursor = next
	}
}

func sortControlResponses(collected map[string]ctl.Response) []ctl.Response {
	out := make([]ctl.Response, 0, len(collected))
	for _, r := range collected {
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b ctl.Response) int { return strings.Compare(a.Replica, b.Replica) })
	return out
}
