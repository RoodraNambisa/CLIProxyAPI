package helps

import (
	"encoding/json"
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

type XAIHeaderPreviewInput struct {
	Name          string            `json:"name"`
	Model         string            `json:"model"`
	Purpose       string            `json:"purpose"`
	Protocol      string            `json:"protocol"`
	Stream        bool              `json:"stream"`
	Config        *config.XAIConfig `json:"config,omitempty"`
	ClientHeaders http.Header       `json:"client_headers,omitempty"`
	RequestBody   json.RawMessage   `json:"request_body,omitempty"`
}
