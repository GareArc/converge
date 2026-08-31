package webhooks

import (
	"fmt"
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
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
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if err := h.producer.Enqueue(ctx, d, worker.EnqueueOpts{}); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		queued = append(queued, d.ID)
	}
	c.JSON(http.StatusAccepted, gin.H{"event_id": eventID, "queued": queued})
}

func (h *handlers) listShelved(c *gin.Context) {
	shelved, err := h.shelf.List(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"shelved": shelved})
}

func (h *handlers) requeue(c *gin.Context) {
	if err := h.shelf.Requeue(c.Request.Context(), c.Param("id")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}
