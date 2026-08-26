package reconcile

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
)

func okSpec() Spec {
	return Spec{
		Name:      "job",
		Reconcile: func(context.Context, ID) error { return nil },
		Triggers:  []Trigger{Schedule(SingleID(), Every(time.Hour))},
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
		{"slash in name", func(s *Spec) { s.Name = "a/b" }, "must not contain"},
		{"nil reconcile func", func(s *Spec) { s.Reconcile = nil }, "Reconcile"},
		{"negative concurrency", func(s *Spec) { s.Concurrency = -1 }, "Concurrency"},
		{"nil trigger", func(s *Spec) { s.Triggers = append(s.Triggers, nil) }, "nil"},
		{"zero id source", func(s *Spec) { s.Triggers = []Trigger{Schedule(IDSource{}, Every(time.Hour))} }, "IDSource"},
		{"bad cadence", func(s *Spec) { s.Triggers = []Trigger{Schedule(SingleID(), Every(-time.Second))} }, "positive"},
		{"zero cadence", func(s *Spec) { s.Triggers = []Trigger{Schedule(SingleID(), Cadence{})} }, "Cadence"},
		{"bad cron", func(s *Spec) { s.Triggers = []Trigger{Schedule(SingleID(), Cron("@daily", CronOpts{}))} }, "descriptors"},
		{"message trigger without queue", func(s *Spec) {
			s.Triggers = append(s.Triggers, OnMessage("", RawID(), OnMessageOpts{}))
		}, "queue"},
		{"message trigger without id func", func(s *Spec) {
			s.Triggers = append(s.Triggers, OnMessage("q", nil, OnMessageOpts{}))
		}, "IDFunc"},
		{"no periodic trigger", func(s *Spec) {
			s.Triggers = []Trigger{OnMessage("q", RawID(), OnMessageOpts{})}
		}, "Schedule trigger"},
		{"no triggers at all", func(s *Spec) { s.Triggers = nil }, "Schedule trigger"},
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

func TestCompetingIsRejectedOnReconcile(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	err := Register(rt, Spec{
		Name:      "nope",
		Reconcile: func(context.Context, ID) error { return nil },
		Triggers:  []Trigger{Schedule(SingleID(), Every(time.Minute))},
		RunMode:   converge.Competing,
	})
	if err == nil {
		t.Fatal("Competing accepted on the reconcile surface")
	}
	if !strings.Contains(err.Error(), "Competing is a worker mode") {
		t.Fatalf("error %q does not name the rule", err)
	}
}

func TestOnAllReplicasAcceptsVersionsAndPagedIDs(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	if err := Register(rt, Spec{
		Name:      "broadcast-paged",
		Reconcile: func(context.Context, ID) error { return nil },
		Triggers: []Trigger{Schedule(
			IDsByPage(func(context.Context, string) ([]ID, string, error) { return nil, "", nil }),
			Every(time.Minute))},
		RunMode:  converge.OnAllReplicas,
		Versions: fixedVersions{},
	}); err != nil {
		t.Fatalf("OnAllReplicas with paged IDs and Versions: %v", err)
	}
}

type fixedVersions struct{}

func (fixedVersions) Latest(context.Context, ID) (Version, error) {
	return 1, nil
}

func TestSpecRequiresASchedule(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	err := Register(rt, Spec{
		Name:      "notifications-only",
		Reconcile: func(context.Context, ID) error { return nil },
		Triggers:  []Trigger{OnMessage("q", RawID(), OnMessageOpts{})},
	})
	if err == nil {
		t.Fatal("a job with no Schedule trigger was accepted")
	}
}

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

func TestEngineInfoRendersScheduleSettings(t *testing.T) {
	s := okSpec()
	s.Concurrency = 2
	e, err := newEngine(s)
	if err != nil {
		t.Fatal(err)
	}
	info := e.Info()
	if info.Job != "job" || info.Surface != converge.SurfaceReconcile || info.RunMode != converge.OnOneReplica {
		t.Fatalf("identity = %+v", info)
	}
	if info.Queue != "" {
		t.Fatalf("Queue = %q, want empty", info.Queue)
	}
	want := map[string]string{
		"concurrency": "2",
		"schedule":    "every 1h",
		"triggers":    "schedule",
	}
	if !reflect.DeepEqual(info.Settings, want) {
		t.Fatalf("Settings = %+v, want %+v", info.Settings, want)
	}
}

func TestEngineInfoRendersCronExpression(t *testing.T) {
	s := okSpec()
	s.Triggers = []Trigger{Schedule(SingleID(), Cron("*/5 * * * *", CronOpts{}))}
	e, err := newEngine(s)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Info().Settings["schedule"]; got != "cron */5 * * * *" {
		t.Fatalf("schedule = %q, want %q", got, "cron */5 * * * *")
	}
}

func TestEngineInfoRendersTriggerComposition(t *testing.T) {
	s := okSpec()
	s.Triggers = []Trigger{
		Schedule(SingleID(), Every(time.Hour)),
		OnMessage("deploy-hints", RawID(), OnMessageOpts{}),
	}
	e, err := newEngine(s)
	if err != nil {
		t.Fatal(err)
	}
	want := "schedule + on-message deploy-hints"
	if got := e.Info().Settings["triggers"]; got != want {
		t.Fatalf("triggers = %q, want %q", got, want)
	}
}

func TestEngineInfoRendersUnknownTriggerAsCustom(t *testing.T) {
	s := okSpec()
	s.Triggers = []Trigger{customPeriodic{}}
	e, err := newEngine(s)
	if err != nil {
		t.Fatal(err)
	}
	info := e.Info()
	if got := info.Settings["triggers"]; got != "custom" {
		t.Fatalf("triggers = %q, want %q", got, "custom")
	}
	if _, ok := info.Settings["schedule"]; ok {
		t.Fatalf("schedule key must be omitted with no Schedule trigger, got %+v", info.Settings)
	}
}

func TestEngineInfoRendersOptionalSettings(t *testing.T) {
	s := okSpec()
	s.Versions = fakeVersions{}
	e, err := newEngine(s)
	if err != nil {
		t.Fatal(err)
	}
	info := e.Info()
	if got := info.Settings["versions"]; got != "custom" {
		t.Fatalf("versions = %q, want custom", got)
	}
}

func TestEngineInfoOmitsZeroSettings(t *testing.T) {
	e, err := newEngine(okSpec())
	if err != nil {
		t.Fatal(err)
	}
	info := e.Info()
	if _, ok := info.Settings["versions"]; ok {
		t.Fatalf(`Settings["versions"] must be omitted for zero values, got %+v`, info.Settings)
	}
}

func TestEngineInfoRendersPinnedDisplayFormats(t *testing.T) {
	loc := time.FixedZone("UTC+2", 2*60*60)
	cases := []struct {
		name   string
		mutate func(*Spec)
		key    string
		want   string
	}{
		{
			name:   "VersionSource renders as custom",
			mutate: func(s *Spec) { s.Versions = fakeVersions{} },
			key:    "versions",
			want:   "custom",
		},
		{
			name: "cron non-UTC location renders a loc suffix",
			mutate: func(s *Spec) {
				s.Triggers = []Trigger{Schedule(SingleID(), Cron("*/5 * * * *", CronOpts{Location: loc}))}
			},
			key:  "schedule",
			want: "cron */5 * * * * (loc: " + loc.String() + ")",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := okSpec()
			c.mutate(&s)
			e, err := newEngine(s)
			if err != nil {
				t.Fatal(err)
			}
			if got := e.Info().Settings[c.key]; got != c.want {
				t.Fatalf("Settings[%q] = %q, want %q", c.key, got, c.want)
			}
		})
	}
}
