// Package debughttp exposes read-only HTTP introspection over a Runtime's
// jobs and failing IDs. Its handler has no mutating routes.
package debughttp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/wiring"
	"github.com/GareArc/converge/worker"
)

const debugListCap = 100

type jobView struct {
	Job              string            `json:"job"`
	Surface          string            `json:"surface"`
	RunMode          string            `json:"run_mode"`
	State            string            `json:"state"`
	Queue            string            `json:"queue"`
	Settings         map[string]string `json:"settings"`
	LeaseHeld        bool              `json:"lease_held"`
	InFlight         int               `json:"in_flight"`
	Backlog          int               `json:"backlog"`
	BacklogKnown     bool              `json:"backlog_known"`
	BacklogAt        string            `json:"backlog_at"`
	Failing          int               `json:"failing"`
	Shelved          int               `json:"shelved"`
	ShelvedKnown     bool              `json:"shelved_known"`
	ShelvedAt        string            `json:"shelved_at"`
	LastSuccess      string            `json:"last_success"`
	LastError        string            `json:"last_error"`
	LastErrorAt      string            `json:"last_error_at"`
	ConsecutiveFails int               `json:"consecutive_fails"`

	FailingIDs       []failingIDView         `json:"failing_ids,omitempty"`
	FailingTruncated bool                    `json:"failing_truncated,omitempty"`
	ShelvedMessages  []worker.ShelvedMessage `json:"shelved_messages,omitempty"`
	ShelvedTruncated bool                    `json:"shelved_truncated,omitempty"`
}

type failingIDView struct {
	ID       string `json:"id"`
	Failures int    `json:"failures"`
	Error    string `json:"error,omitempty"`
	NextTry  string `json:"next_try,omitempty"`
}

type jobsResponse struct {
	Jobs []jobView `json:"jobs"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func ReadOnlyHandler(rt *converge.Runtime) http.Handler {
	if rt == nil {
		panic("debughttp: ReadOnlyHandler: rt must not be nil")
	}
	mux := http.NewServeMux()
	registerReadOnlyRoutes(mux, rt)
	return mux
}

func registerReadOnlyRoutes(mux *http.ServeMux, rt *converge.Runtime) {
	mux.HandleFunc("GET /debug/jobs", listJobsHandler(rt))
	mux.HandleFunc("GET /debug/jobs/{$}", listJobsHandler(rt))
	mux.HandleFunc("GET /debug/jobs/{job}", singleJobHandler(rt))
}

func singleJobHandler(rt *converge.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		job := r.PathValue("job")
		info, ok, err := lookupJob(rt, job)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !ok {
			writeError(w, http.StatusNotFound, fmt.Sprintf("unknown job %q", job))
			return
		}
		stats := statsByJob(rt.Stats())
		view := mergeJobView(info, stats[job])
		if err := attachFailingAndShelved(r.Context(), rt, info, &view); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, view)
	}
}

func listJobsHandler(rt *converge.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		infos, err := inspectJobs(rt)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		stats := statsByJob(rt.Stats())
		views := make([]jobView, 0, len(infos))
		for _, info := range infos {
			views = append(views, mergeJobView(info, stats[info.Job]))
		}
		writeJSON(w, http.StatusOK, jobsResponse{Jobs: views})
	}
}

func inspectJobs(rt *converge.Runtime) ([]converge.JobInfo, error) {
	return wiring.Jobs(rt)
}

func lookupJob(rt *converge.Runtime, job string) (converge.JobInfo, bool, error) {
	infos, err := inspectJobs(rt)
	if err != nil {
		return converge.JobInfo{}, false, err
	}
	for _, info := range infos {
		if info.Job == job {
			return info, true, nil
		}
	}
	return converge.JobInfo{}, false, nil
}

func statsByJob(stats []converge.JobStats) map[string]converge.JobStats {
	out := make(map[string]converge.JobStats, len(stats))
	for _, s := range stats {
		out[s.Job] = s
	}
	return out
}

func mergeJobView(info converge.JobInfo, stats converge.JobStats) jobView {
	return jobView{
		Job:              info.Job,
		Surface:          info.Surface.String(),
		RunMode:          info.RunMode.String(),
		State:            stats.State.String(),
		Queue:            info.Queue,
		Settings:         info.Settings,
		LeaseHeld:        stats.LeaseHeld,
		InFlight:         stats.InFlight,
		Backlog:          stats.Backlog,
		BacklogKnown:     stats.BacklogKnown,
		BacklogAt:        formatTime(stats.BacklogAt),
		Failing:          stats.Failing,
		Shelved:          stats.Shelved,
		ShelvedKnown:     stats.ShelvedKnown,
		ShelvedAt:        formatTime(stats.ShelvedAt),
		LastSuccess:      formatTime(stats.LastSuccess),
		LastError:        errString(stats.LastError),
		LastErrorAt:      formatTime(stats.LastErrorAt),
		ConsecutiveFails: stats.ConsecutiveFails,
	}
}

func attachFailingAndShelved(ctx context.Context, rt *converge.Runtime, info converge.JobInfo, view *jobView) error {
	switch info.Surface {
	case converge.SurfaceReconcile:
		ids, err := wiring.FailingIDs(rt, info.Job)
		if err != nil {
			return err
		}
		view.FailingIDs, view.FailingTruncated = capFailingIDs(ids)
	case converge.SurfaceWorker:
		shelf, err := worker.ShelfFrom(rt, info.Job)
		if err != nil {
			return err
		}
		list, err := shelf.List(ctx)
		if err != nil {
			return err
		}
		view.ShelvedMessages, view.ShelvedTruncated = capShelved(list)
	}
	return nil
}

func capFailingIDs(ids []converge.FailingID) ([]failingIDView, bool) {
	truncated := len(ids) > debugListCap
	if truncated {
		ids = ids[:debugListCap]
	}
	out := make([]failingIDView, 0, len(ids))
	for _, id := range ids {
		out = append(out, failingIDView{
			ID:       id.ID,
			Failures: id.Failures,
			Error:    errString(id.Err),
			NextTry:  formatTime(id.NextTry),
		})
	}
	return out, truncated
}

func capShelved(list []worker.ShelvedMessage) ([]worker.ShelvedMessage, bool) {
	truncated := len(list) > debugListCap
	if truncated {
		list = list[:debugListCap]
	}
	return list, truncated
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
