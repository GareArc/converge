package reconcile

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/GareArc/converge"
)

func okSpec() Spec {
	return Spec{
		Name:       "job",
		Reconciler: Func(func(context.Context, ID) error { return nil }),
		Triggers:   []Trigger{Schedule(SingleID(), Every(time.Hour))},
	}
}

func TestSpecValidationMatrix(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Spec)
		wantErr string
	}{
		{"valid", func(s *Spec) {}, ""},
		{"empty name", func(s *Spec) { s.Name = "" }, "Name"},
		{"nil reconciler", func(s *Spec) { s.Reconciler = nil }, "Reconciler"},
		{"negative concurrency", func(s *Spec) { s.Concurrency = -1 }, "Concurrency"},
		{"negative dead letter", func(s *Spec) { s.DeadLetterAfter = -1 }, "DeadLetterAfter"},
		{"negative rate", func(s *Spec) { s.RateLimit = converge.Rate{Events: -1, Per: time.Second} }, "RateLimit"},
		{"half rate", func(s *Spec) { s.RateLimit = converge.Rate{Events: 5} }, "RateLimit"},
		{"split across replicas", func(s *Spec) { s.RunMode = converge.SplitAcrossReplicas }, "SplitAcrossReplicas"},
		{"all replicas with versions", func(s *Spec) {
			s.RunMode = converge.OnAllReplicas
			s.Versions = fakeVersions{}
		}, "Versions"},
		{"all replicas with dead letter", func(s *Spec) {
			s.RunMode = converge.OnAllReplicas
			s.DeadLetterAfter = 3
		}, "DeadLetterAfter"},
		{"all replicas with rate limit", func(s *Spec) {
			s.RunMode = converge.OnAllReplicas
			s.RateLimit = converge.Rate{Events: 1, Per: time.Second}
		}, "RateLimit"},
		{"all replicas with paged source", func(s *Spec) {
			s.RunMode = converge.OnAllReplicas
			s.Triggers = []Trigger{Schedule(IDsByPage(func(context.Context, string) ([]ID, string, error) {
				return nil, "", nil
			}), Every(time.Hour))}
		}, "IDsByPage"},
		{"all replicas with explicit group delivery", func(s *Spec) {
			s.RunMode = converge.OnAllReplicas
			s.AllowUnscheduled = true
			s.Triggers = []Trigger{OnMessage("q", RawID(), OnMessageOpts{Delivery: converge.Group})}
		}, "Broadcast"},
		{"nil trigger", func(s *Spec) { s.Triggers = append(s.Triggers, nil) }, "nil"},
		{"zero id source", func(s *Spec) { s.Triggers = []Trigger{Schedule(IDSource{}, Every(time.Hour))} }, "IDSource"},
		{"bad cadence", func(s *Spec) { s.Triggers = []Trigger{Schedule(SingleID(), Every(-time.Second))} }, "positive"},
		{"zero cadence", func(s *Spec) { s.Triggers = []Trigger{Schedule(SingleID(), Cadence{})} }, "Cadence"},
		{"versions not wired", func(s *Spec) { s.Versions = fakeVersions{} }, "not supported"},
		{"bad cron", func(s *Spec) { s.Triggers = []Trigger{Schedule(SingleID(), Cron("@daily", CronOpts{}))} }, "descriptors"},
		{"message trigger without queue", func(s *Spec) {
			s.AllowUnscheduled = true
			s.Triggers = []Trigger{OnMessage("", RawID(), OnMessageOpts{})}
		}, "queue"},
		{"message trigger without id func", func(s *Spec) {
			s.AllowUnscheduled = true
			s.Triggers = []Trigger{OnMessage("q", nil, OnMessageOpts{})}
		}, "IDFunc"},
		{"no periodic trigger", func(s *Spec) {
			s.Triggers = []Trigger{OnMessage("q", RawID(), OnMessageOpts{})}
		}, "AllowUnscheduled"},
		{"no periodic trigger allowed", func(s *Spec) {
			s.AllowUnscheduled = true
			s.Triggers = []Trigger{OnMessage("q", RawID(), OnMessageOpts{})}
		}, ""},
		{"no triggers at all", func(s *Spec) { s.Triggers = nil }, "AllowUnscheduled"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := okSpec()
			c.mutate(&s)
			_, err := newEngine(s)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("error %v, want it to mention %q", err, c.wantErr)
			}
		})
	}
}

type fakeVersions struct{}

func (fakeVersions) Latest(context.Context, ID) (Version, error) { return 0, nil }

func TestNewEngineAppliesDefaults(t *testing.T) {
	e, err := newEngine(okSpec())
	if err != nil {
		t.Fatal(err)
	}
	if e.cfg.concurrency != 1 || e.cfg.runMode != converge.OnOneReplica {
		t.Fatalf("defaults = %+v", e.cfg)
	}
	if !e.cfg.single {
		t.Fatal("SingleID schedule must mark the job single")
	}
	multi := okSpec()
	multi.Triggers = []Trigger{Schedule(IDs(func(context.Context) ([]ID, error) { return nil, nil }), Every(time.Hour))}
	e2, err := newEngine(multi)
	if err != nil {
		t.Fatal(err)
	}
	if e2.cfg.single {
		t.Fatal("list-source job must not be single")
	}
}

type customPeriodic struct{}

func (customPeriodic) Run(ctx context.Context, wake func(ID)) error { <-ctx.Done(); return ctx.Err() }

func (customPeriodic) NextAfter(t time.Time) time.Time { return t.Add(time.Hour) }

func TestCustomPeriodicTriggerSatisfiesScheduleRequirement(t *testing.T) {
	s := okSpec()
	s.Triggers = []Trigger{customPeriodic{}}
	if _, err := newEngine(s); err != nil {
		t.Fatal(err)
	}
}

func TestPausedSpecDropsWakes(t *testing.T) {
	te := startEngine(t, config{paused: true}, func(ctx context.Context, id ID) error { return nil })
	te.e.hint("a")
	await(t, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			wd, ok := e.(converge.WakeDiscarded)
			return ok && wd.Reason == converge.DiscardPaused
		}) == 1
	})
}
