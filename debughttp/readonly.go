package debughttp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/hook"
)

type jobView struct {
	Job              string            `json:"job"`
	Surface          string            `json:"surface"`
	RunMode          string            `json:"run_mode"`
	Queue            string            `json:"queue"`
	Paused           bool              `json:"paused"`
	Settings         map[string]string `json:"settings"`
	QueueDepth       int               `json:"queue_depth"`
	Parked           int               `json:"parked"`
	LastSuccess      string            `json:"last_success"`
	ConsecutiveFails int               `json:"consecutive_fails"`
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
	mux.HandleFunc("GET /debug/jobs", func(w http.ResponseWriter, r *http.Request) {
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
	})
	mux.HandleFunc("GET /debug/jobs/{job}", func(w http.ResponseWriter, r *http.Request) {
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
		writeJSON(w, http.StatusOK, mergeJobView(info, stats[job]))
	})
}

func inspectJobs(rt *converge.Runtime) ([]converge.JobInfo, error) {
	raw, err := hook.Inspect(rt)
	if err != nil {
		return nil, err
	}
	infos, ok := raw.([]converge.JobInfo)
	if !ok {
		return nil, fmt.Errorf("debughttp: inspect: unexpected type %T", raw)
	}
	return infos, nil
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
		Queue:            info.Queue,
		Paused:           info.Paused,
		Settings:         info.Settings,
		QueueDepth:       stats.QueueDepth,
		Parked:           stats.Parked,
		LastSuccess:      formatLastSuccess(stats.LastSuccess),
		ConsecutiveFails: stats.ConsecutiveFails,
	}
}

func formatLastSuccess(t time.Time) string {
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
