package management

import (
	"context"
	"errors"
	"net/http"
	"os"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

func chatGPTWebManualReloginPersistenceResponse(auth *coreauth.Auth, current bool, err error, response gin.H) (int, bool) {
	outcome, explicit := coreauth.SaveOutcomeFromError(err)
	if !explicit && !coreauth.IsRequestAuthPersistenceError(err) {
		return 0, false
	}
	status := http.StatusServiceUnavailable
	response["status"] = "failed"
	response["error_category"] = "persist_uncertain"
	response["error"] = "credential persistence outcome is uncertain"
	response["failure_stage"] = "credential_persist"
	response["persistence_outcome"] = "unknown"
	response["persistence_reason"] = chatGPTWebPersistenceFailureReason(err)
	if explicit {
		switch outcome {
		case coreauth.SaveOutcomeCommitted:
			response["persistence_outcome"] = "committed"
			// Durable storage alone does not prove the new credential was installed.
			if current {
				status = http.StatusOK
				response["status"] = "ok"
				response["warning"] = "credential was saved with a cleanup warning"
				delete(response, "error_category")
				delete(response, "error")
			} else {
				response["error"] = "credential was saved but runtime installation is unconfirmed"
			}
		case coreauth.SaveOutcomeUncertain:
			response["persistence_outcome"] = "uncertain"
		case coreauth.SaveOutcomeRolledBack:
			response["persistence_outcome"] = "rolled_back"
			status = http.StatusInternalServerError
			response["error_category"] = "persist_failed"
			response["error"] = "failed to save chatgpt web credential"
			if errors.Is(err, authfileguard.ErrPersistGenerationStale) {
				status = http.StatusConflict
				response["error_category"] = "credential_changed"
				response["error"] = "credential file changed while re-login was running; reload it before retrying"
			}
		}
	}
	fields := log.Fields{
		"provider": "chatgpt-web", "stage": "credential_persist", "status": status,
		"persistence_outcome": response["persistence_outcome"],
		"persistence_reason":  response["persistence_reason"],
	}
	if auth != nil {
		fields["auth_index"] = auth.EnsureIndex()
	}
	// Store errors can contain paths, auth payloads, or backend credentials.
	log.WithFields(fields).Warn("chatgpt web manual re-login credential persistence did not complete cleanly")
	return status, true
}

func chatGPTWebPersistenceFailureReason(err error) string {
	switch {
	case errors.Is(err, authfileguard.ErrPersistGenerationStale):
		return "source_generation_changed"
	case errors.Is(err, os.ErrPermission):
		return "permission_denied"
	case errors.Is(err, syscall.ENOSPC), errors.Is(err, syscall.EDQUOT):
		return "storage_full"
	case errors.Is(err, syscall.EROFS):
		return "read_only_filesystem"
	case errors.Is(err, os.ErrNotExist):
		return "storage_path_missing"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "storage_error"
	}
}
