package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementasset"
)

func TestGetControlPanelUpdateDisabledSkipsRemoteCheck(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	h := &Handler{cfg: &config.Config{RemoteManagement: config.RemoteManagement{
		DisableControlPanel:    true,
		DisableAutoUpdatePanel: true,
		PanelGitHubRepository:  server.URL + "/repos/test/panel/releases/latest",
	}}}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/v0/management/control-panel/update", nil)

	h.GetControlPanelUpdate(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
	var status managementasset.ManagementHTMLStatus
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &status); errDecode != nil {
		t.Fatalf("decode status: %v", errDecode)
	}
	if !status.Disabled {
		t.Fatal("disabled = false, want true")
	}
	if !status.AutoUpdateDisabled {
		t.Fatal("auto_update_disabled = false, want true")
	}
}
