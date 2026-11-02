package management

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/fillorder"
)

func (h *Handler) PreviewFillOrder(c *gin.Context) { h.fillOrderOperation(c, false) }
func (h *Handler) MoveFillOrder(c *gin.Context)    { h.fillOrderOperation(c, true) }
func (h *Handler) fillOrderOperation(c *gin.Context, commit bool) {
	manager := h.coreAuthRuntimeManager()
	if manager == nil || !manager.FillOrderSupported() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "fill order unavailable"})
		return
	}
	var req fillorder.Move
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid fill order move"})
		return
	}
	var p fillorder.Preview
	var err error
	if commit {
		p, err = manager.MoveFillOrder(c.Request.Context(), req)
	} else {
		p, err = manager.PreviewFillOrder(req)
	}
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, fillorder.ErrConflict):
			status = http.StatusConflict
		case errors.Is(err, fillorder.ErrNotFound):
			status = http.StatusNotFound
		case errors.Is(err, fillorder.ErrInvalidMove):
			status = http.StatusBadRequest
		}
		c.JSON(status, gin.H{"error": err.Error(), "revision": manager.FillOrderRevision()})
		return
	}
	describe := func(r *fillorder.Record) any {
		if r == nil {
			return nil
		}
		a, ok := manager.GetByID(r.Key)
		if !ok {
			return nil
		}
		return gin.H{"record_id": r.ID, "name": a.FileName, "auth_index": a.Index, "priority": r.Priority, "fill_order": manager.FillOrderSummary(a)}
	}
	c.JSON(http.StatusOK, gin.H{"record": describe(&p.Record), "before": describe(p.Before), "after": describe(p.After), "revision": p.Revision})
}
