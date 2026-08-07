package management

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementasset"
)

func (h *Handler) GetControlPanelUpdate(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "config unavailable"})
		return
	}
	if cfg.RemoteManagement.DisableControlPanel {
		c.JSON(http.StatusOK, managementasset.ManagementHTMLStatus{
			Disabled:           true,
			AutoUpdateDisabled: cfg.RemoteManagement.DisableAutoUpdatePanel,
			CheckedAt:          time.Now().UTC(),
		})
		return
	}
	status := managementasset.CheckManagementHTMLStatus(
		c.Request.Context(),
		managementasset.StaticDir(h.configFilePath),
		cfg.ProxyURL,
		cfg.RemoteManagement.PanelGitHubRepository,
	)
	status.Disabled = cfg.RemoteManagement.DisableControlPanel
	status.AutoUpdateDisabled = cfg.RemoteManagement.DisableAutoUpdatePanel
	c.JSON(http.StatusOK, status)
}

func (h *Handler) PostControlPanelUpdate(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "config unavailable"})
		return
	}
	if cfg.RemoteManagement.DisableControlPanel {
		c.JSON(http.StatusOK, managementasset.ManagementHTMLStatus{
			Disabled:           true,
			AutoUpdateDisabled: cfg.RemoteManagement.DisableAutoUpdatePanel,
			CheckedAt:          time.Now().UTC(),
			Error:              "control panel disabled",
		})
		return
	}
	status := managementasset.UpdateManagementHTML(
		c.Request.Context(),
		managementasset.StaticDir(h.configFilePath),
		cfg.ProxyURL,
		cfg.RemoteManagement.PanelGitHubRepository,
		true,
	)
	status.AutoUpdateDisabled = cfg.RemoteManagement.DisableAutoUpdatePanel
	c.JSON(http.StatusOK, status)
}
