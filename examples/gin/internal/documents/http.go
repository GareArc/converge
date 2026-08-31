package documents

import (
	"log/slog"
	"net/http"

	"github.com/GareArc/converge/reconcile"
	"github.com/gin-gonic/gin"
)

type handlers struct {
	store    *Store
	notifier *reconcile.Notifier
}

type upsertRequest struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

func (h *handlers) upsert(c *gin.Context) {
	var req upsertRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.Title == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "title is required"})
		return
	}
	id := c.Param("id")
	ctx := c.Request.Context()
	if err := h.store.Upsert(ctx, Document{ID: id, Title: req.Title, Body: req.Body}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if err := h.notifier.Notify(ctx, reconcile.ID(id)); err != nil {
		slog.Default().Error("document upsert notification failed", "id", id, "error", err)
	}
	c.JSON(http.StatusAccepted, gin.H{"id": id})
}

func (h *handlers) search(c *gin.Context) {
	found, err := h.store.Search(c.Request.Context(), c.Query("q"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if found == nil {
		found = []Document{}
	}
	c.JSON(http.StatusOK, gin.H{"results": found})
}
