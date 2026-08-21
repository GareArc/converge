package debughttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/ctl"
	"github.com/GareArc/converge/internal/hook"
	"github.com/GareArc/converge/worker"
)

type OpsOpts struct {
	Timeout      time.Duration
	ShowPayloads bool
}

type verbResponse struct {
	Op        string         `json:"op"`
	Job       string         `json:"job"`
	ID        string         `json:"id,omitempty"`
	Responses []ctl.Response `json:"responses"`
}

type deadLetterView struct {
	worker.DeadLetter
	Payload json.RawMessage `json:"payload,omitempty"`
}

type dlqListResponse struct {
	DeadLetters []deadLetterView `json:"dead_letters"`
}

type requeueResponse struct {
	Requeued string `json:"requeued"`
}

type purgeResponse struct {
	Purged string `json:"purged"`
}

type purgeAllResponse struct {
	PurgedCount int `json:"purged_count"`
}

func OpsHandler(rt *converge.Runtime, o OpsOpts) http.Handler {
	if rt == nil {
		panic("debughttp: OpsHandler: rt must not be nil")
	}
	mux := http.NewServeMux()
	registerReadOnlyRoutes(mux, rt)
	registerOpsRoutes(mux, rt, o)
	return mux
}

func registerOpsRoutes(mux *http.ServeMux, rt *converge.Runtime, o OpsOpts) {
	mux.HandleFunc("POST /debug/jobs/{job}/poke", opsVerbHandler(rt, o, ctl.OpPoke, true))
	mux.HandleFunc("POST /debug/jobs/{job}/run-pass", opsVerbHandler(rt, o, ctl.OpRunPass, false))
	mux.HandleFunc("POST /debug/jobs/{job}/pause", opsVerbHandler(rt, o, ctl.OpPause, false))
	mux.HandleFunc("POST /debug/jobs/{job}/resume", opsVerbHandler(rt, o, ctl.OpResume, false))
	mux.HandleFunc("GET /debug/jobs/{job}/dlq", dlqListHandler(rt, o))
	mux.HandleFunc("POST /debug/jobs/{job}/dlq/{id}/requeue", dlqRequeueHandler(rt))
	mux.HandleFunc("DELETE /debug/jobs/{job}/dlq/{id}", dlqPurgeOneHandler(rt))
	mux.HandleFunc("DELETE /debug/jobs/{job}/dlq", dlqPurgeAllHandler(rt))
}

func opsVerbHandler(rt *converge.Runtime, o OpsOpts, op string, withID bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		job := r.PathValue("job")
		if _, ok, err := lookupJob(rt, job); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		} else if !ok {
			writeError(w, http.StatusNotFound, fmt.Sprintf("unknown job %q", job))
			return
		}
		id := ""
		if withID {
			id = r.FormValue("id")
		}
		resp, err := hook.ControlDispatch(rt, r.Context(), ctl.Request{Op: op, Job: job, ID: id, Timeout: o.Timeout})
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, verbResponse{Op: op, Job: job, ID: id, Responses: resp})
	}
}

func ensureWorkerJob(w http.ResponseWriter, rt *converge.Runtime, job string) bool {
	info, ok, err := lookupJob(rt, job)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return false
	}
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("unknown job %q", job))
		return false
	}
	if info.Surface != converge.SurfaceWorker {
		writeError(w, http.StatusNotFound, fmt.Sprintf("job %q is a %s job, not a worker job", job, info.Surface.String()))
		return false
	}
	return true
}

func newDeadLetterView(dl worker.DeadLetter, showPayload bool) (deadLetterView, error) {
	v := deadLetterView{DeadLetter: dl}
	if !showPayload {
		return v, nil
	}
	raw, err := json.Marshal(dl.Payload)
	if err != nil {
		return deadLetterView{}, err
	}
	v.Payload = raw
	return v, nil
}

func dlqListHandler(rt *converge.Runtime, o OpsOpts) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		job := r.PathValue("job")
		if !ensureWorkerJob(w, rt, job) {
			return
		}
		dlq, err := worker.DLQFrom(rt, job)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		list, err := dlq.List(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		views := make([]deadLetterView, 0, len(list))
		for _, dl := range list {
			v, err := newDeadLetterView(dl, o.ShowPayloads)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			views = append(views, v)
		}
		writeJSON(w, http.StatusOK, dlqListResponse{DeadLetters: views})
	}
}

func dlqRequeueHandler(rt *converge.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		job := r.PathValue("job")
		if !ensureWorkerJob(w, rt, job) {
			return
		}
		id := r.PathValue("id")
		dlq, err := worker.DLQFrom(rt, job)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if err := dlq.Requeue(r.Context(), id); err != nil {
			if errors.Is(err, worker.ErrDeadLetterNotFound) {
				writeError(w, http.StatusNotFound, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, requeueResponse{Requeued: id})
	}
}

func dlqPurgeOneHandler(rt *converge.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		job := r.PathValue("job")
		if !ensureWorkerJob(w, rt, job) {
			return
		}
		id := r.PathValue("id")
		dlq, err := worker.DLQFrom(rt, job)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if err := dlq.Purge(r.Context(), id); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, purgeResponse{Purged: id})
	}
}

func dlqPurgeAllHandler(rt *converge.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		job := r.PathValue("job")
		if !ensureWorkerJob(w, rt, job) {
			return
		}
		dlq, err := worker.DLQFrom(rt, job)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		n, err := dlq.PurgeAll(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, purgeAllResponse{PurgedCount: n})
	}
}
