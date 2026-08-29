package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/worker"
)

const (
	demoWindow = 2 * time.Second
	namespace  = "media"
)

type TranscodeJob struct {
	UploadID string `json:"upload_id"`
	Profile  string `json:"profile"`
}

type uploadStore struct {
	mu       sync.Mutex
	finished map[string]bool
	waited   map[string]int
}

func newUploadStore(finished map[string]bool) *uploadStore {
	return &uploadStore{finished: finished, waited: map[string]int{}}
}

func (s *uploadStore) complete(_ context.Context, id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished[id] {
		return true
	}
	s.waited[id]++
	return false
}

func (s *uploadStore) waiting() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	lines := make([]string, 0, len(s.waited))
	for id, n := range s.waited {
		lines = append(lines, fmt.Sprintf("%s still uploading, checked %d time(s)", id, n))
	}
	slices.Sort(lines)
	return lines
}

type transcoder struct {
	mu      sync.Mutex
	outputs []string
}

func (t *transcoder) run(_ context.Context, j TranscodeJob) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.outputs = append(t.outputs, j.UploadID+"@"+j.Profile)
	return nil
}

func (t *transcoder) rendered() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := slices.Clone(t.outputs)
	slices.Sort(out)
	return out
}

var transcode = worker.NewTask[TranscodeJob]("transcode", worker.TaskOpts{})

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	mq := inmem.NewMQ()

	rt, err := converge.New(converge.Options{
		Namespace: namespace,
		MQ:        mq,
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	uploads := newUploadStore(map[string]bool{"up-4001": true})
	ffmpeg := &transcoder{}

	err = worker.Handle(rt, transcode, func(ctx context.Context, j TranscodeJob) error {
		if !uploads.complete(ctx, j.UploadID) {
			return worker.Snooze{In: 30 * time.Second}
		}
		return ffmpeg.run(ctx, j)
	}, worker.HandleOpts{Concurrency: 1, Timeout: 30 * time.Minute})
	if err != nil {
		return err
	}

	p, err := transcode.NewProducer(rt.Scope())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()

	for _, j := range []TranscodeJob{
		{UploadID: "up-4001", Profile: "1080p"},
		{UploadID: "up-4002", Profile: "1080p"},
	} {
		if err := p.Enqueue(ctx, j, worker.EnqueueOpts{}); err != nil {
			return err
		}
	}

	if err := rt.Run(ctx); err != nil {
		return err
	}

	fmt.Printf("transcoded: %v\n", ffmpeg.rendered())
	for _, line := range uploads.waiting() {
		fmt.Println(line)
	}
	return nil
}
