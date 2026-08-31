package webhooks

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/GareArc/converge/worker"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type handlers struct {
	store    *Store
	producer *worker.Producer[Delivery]
	shelf    *worker.Shelf
}

type publishRequest struct {
	Event string `json:"event"`
}

func (h *handlers) publish(c *gin.Context) {
	var req publishRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.Event == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "event is required"})
		return
	}
	ctx := c.Request.Context()
	subs, err := h.store.ActiveSubscribers(ctx)
	if err != nil {
		fail(c, "list active subscribers failed", err)
		return
	}
	eventID := uuid.NewString()
	queued := make([]string, 0, len(subs))
	for _, sub := range subs {
		d := Delivery{
			ID:           fmt.Sprintf("%s:%s", eventID, sub.ID),
			EventID:      eventID,
			SubscriberID: sub.ID,
			URL:          sub.URL,
			Event:        req.Event,
		}
		if err := h.store.Queue(ctx, d); err != nil {
			slog.Default().Error("webhook delivery queue failed", "id", d.ID, "error", err)
			continue
		}
		if err := h.producer.Enqueue(ctx, d, worker.EnqueueOpts{}); err != nil {
			slog.Default().Error("webhook delivery enqueue failed", "id", d.ID, "error", err)
			continue
		}
		queued = append(queued, d.ID)
	}
	c.JSON(http.StatusAccepted, gin.H{"event_id": eventID, "queued": queued})
}

func (h *handlers) listShelved(c *gin.Context) {
	shelved, err := h.shelf.List(c.Request.Context())
	if err != nil {
		fail(c, "list shelved webhook deliveries failed", err)
		return
	}
	if shelved == nil {
		shelved = []worker.ShelvedMessage{}
	}
	c.JSON(http.StatusOK, gin.H{"shelved": shelved})
}

func (h *handlers) requeue(c *gin.Context) {
	if err := h.shelf.Requeue(c.Request.Context(), c.Param("id")); err != nil {
		if errors.Is(err, worker.ErrNotShelved) {
			c.JSON(http.StatusNotFound, gin.H{"error": "no such shelved message"})
			return
		}
		fail(c, "webhook requeue failed", err)
		return
	}
	c.Status(http.StatusNoContent)
}

func fail(c *gin.Context, logMsg string, err error) {
	slog.Default().Error(logMsg, "error", err)
	c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
}
