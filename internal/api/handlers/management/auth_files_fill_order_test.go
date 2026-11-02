package management

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFillOrderManagementPreviewMoveConflictAndPaging(t *testing.T) {
	h, m, dir := newAuthFilesPaginationTestHandler(t, true)
	if err := m.ConfigureFillOrder(""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.CloseFillOrder() })
	for _, name := range []string{"a.json", "b.json", "c.json"} {
		registerAuthFilesPaginationTestAuth(t, m, dir, name, "codex", name == "b.json", "", 5, true, "plus")
	}
	if err := m.InitializeFillOrder(); err != nil {
		t.Fatal(err)
	}
	registerAuthFilesPaginationTestAuth(t, m, dir, "0-new.json", "codex", false, "", 5, true, "plus")
	r := gin.New()
	r.GET("/auth-files", h.ListAuthFiles)
	r.POST("/preview", h.PreviewFillOrder)
	r.POST("/move", h.MoveFillOrder)
	c, _ := m.GetByID("c.json")
	record := m.FillOrderSummary(c)
	rev := m.FillOrderRevision()
	body, _ := json.Marshal(map[string]any{"record_id": record["record_id"], "revision": rev, "direction": "up", "steps": 2})
	post := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		return w
	}
	preview := post("/preview")
	if preview.Code != 200 || !strings.Contains(preview.Body.String(), "a.json") || m.FillOrderRevision() != rev {
		t.Fatal(preview.Code, preview.Body.String())
	}
	result := post("/move")
	if result.Code != 200 {
		t.Fatal(result.Code, result.Body.String())
	}
	conflict := post("/move")
	if conflict.Code != 409 {
		t.Fatal(conflict.Code)
	}
	var page struct {
		Files []map[string]any `json:"files"`
	}
	decodeAuthFilesPaginationResponse(t, performAuthFilesPaginationRequest(r, "/auth-files?paged=true&page=1&page_size=2&sort=fill-order"), &page)
	if strings.Join(authFilesPaginationNames(page.Files), ",") != "c.json,a.json" {
		t.Fatal(page.Files)
	}
	if strings.Contains(preview.Body.String(), "identity") || strings.Contains(preview.Body.String(), "token") {
		t.Fatal("preview leaked private identity")
	}
}
