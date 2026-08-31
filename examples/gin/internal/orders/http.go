package orders

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

type createRequest struct {
	ID string `json:"id"`
}

func (h *handlers) create(c *gin.Context) {
	var req createRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.ID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required"})
		return
	}
	if err := h.store.Create(c.Request.Context(), req.ID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"id": req.ID, "status": StatusPending})
}

func (h *handlers) pay(c *gin.Context) {
	id := c.Param("id")
	ctx := c.Request.Context()
	paid, err := h.store.Pay(ctx, id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !paid {
		_, found, err := h.store.Get(ctx, id)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if !found {
			c.JSON(http.StatusNotFound, gin.H{"error": "no such order"})
			return
		}
		c.JSON(http.StatusConflict, gin.H{"error": "order is not pending"})
		return
	}
	if err := h.notifier.Notify(ctx, reconcile.ID(id)); err != nil {
		slog.Default().Error("order paid notification failed", "id", id, "error", err)
	}
	c.JSON(http.StatusOK, gin.H{"id": id, "status": StatusPaid})
}

func (h *handlers) get(c *gin.Context) {
	o, found, err := h.store.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "no such order"})
		return
	}
	c.JSON(http.StatusOK, o)
}
