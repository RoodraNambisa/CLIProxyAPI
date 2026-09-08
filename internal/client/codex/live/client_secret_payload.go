package live

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

var errUnsupportedRealtimeSession = errors.New("only realtime sessions are supported by Codex OAuth")

var errClientSecretBodyTooLarge = errors.New("realtime client secret request exceeds 64 KiB")

type clientSecretRequest struct {
	session  json.RawMessage
	model    string
	lifetime time.Duration
}

func readClientSecretBody(reader io.Reader) ([]byte, error) {
	if reader == nil {
		return nil, nil
	}
	body, errRead := io.ReadAll(io.LimitReader(reader, clientSecretMaxBodySize+1))
	if len(body) > clientSecretMaxBodySize {
		return nil, errClientSecretBodyTooLarge
	}
	return body, errRead
}

func parseClientSecretRequest(body []byte, legacy bool) (clientSecretRequest, error) {
	if len(body) > clientSecretMaxBodySize {
		return clientSecretRequest{}, errClientSecretBodyTooLarge
	}
	result := clientSecretRequest{lifetime: clientSecretDefaultLifetime}
	session := json.RawMessage(body)
	if !legacy {
		var payload struct {
			Session      json.RawMessage `json:"session"`
			ExpiresAfter *struct {
				Anchor  string `json:"anchor"`
				Seconds int64  `json:"seconds"`
			} `json:"expires_after"`
		}
		if len(bytes.TrimSpace(body)) > 0 {
			if errJSON := json.Unmarshal(body, &payload); errJSON != nil {
				return result, errors.New("invalid realtime client secret request")
			}
		}
		session = payload.Session
		if expiry := payload.ExpiresAfter; expiry != nil {
			if expiry.Anchor != "" && expiry.Anchor != "created_at" {
				return result, errors.New("expires_after.anchor must be created_at")
			}
			if expiry.Seconds < int64(clientSecretMinimumLifetime/time.Second) || expiry.Seconds > int64(clientSecretMaximumLifetime/time.Second) {
				return result, errors.New("expires_after.seconds must be between 10 and 7200")
			}
			result.lifetime = time.Duration(expiry.Seconds) * time.Second
		}
	}
	trimmed := bytes.TrimSpace(session)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		session = json.RawMessage(`{}`)
	}
	object, errObject := callJSONObject(session, "session")
	if errObject != nil {
		return result, errObject
	}
	kind, model := "", ""
	if raw, exists := object["type"]; exists {
		if errJSON := json.Unmarshal(raw, &kind); errJSON != nil {
			return result, errors.New("realtime session type must be a string")
		}
	}
	if strings.TrimSpace(kind) == "" {
		kind = "realtime"
	}
	if kind != "realtime" {
		return result, errUnsupportedRealtimeSession
	}
	if raw, exists := object["model"]; exists {
		if errJSON := json.Unmarshal(raw, &model); errJSON != nil {
			return result, errors.New("realtime session model must be a string")
		}
	}
	model = strings.TrimSpace(model)
	if model == "" {
		model = registry.CodexRealtimeModelID
	}
	object["type"], _ = json.Marshal(kind)
	object["model"], _ = json.Marshal(model)
	// Server-managed identity and credentials are never copied from a previous
	// session response into a new capability or sent to the upstream.
	for _, field := range []string{"id", "object", "expires_at", "client_secret"} {
		delete(object, field)
	}
	result.session, _ = json.Marshal(object)
	if len(result.session) > clientSecretMaxBodySize {
		return result, errClientSecretBodyTooLarge
	}
	result.model = model
	return result, nil
}

func clientSecretSessionResponse(a ClientSecretAuthorization) (json.RawMessage, error) {
	object, errObject := callJSONObject(a.session, "session")
	if errObject != nil {
		return nil, errObject
	}
	object["id"], _ = json.Marshal(a.principal)
	object["object"] = json.RawMessage(`"realtime.session"`)
	object["expires_at"] = json.RawMessage(fmt.Sprint(a.expiresAt.Unix()))
	delete(object, "client_secret")
	return json.Marshal(object)
}

// Realtime fixes the model at connection creation. Only mutable native session
// settings belong in the initialization event sent before client relay starts.
func clientSecretSessionUpdate(a ClientSecretAuthorization) ([]byte, error) {
	object, errObject := callJSONObject(a.session, "session")
	if errObject != nil {
		return nil, errObject
	}
	for _, field := range []string{"model", "id", "object", "expires_at", "client_secret"} {
		delete(object, field)
	}
	session, errJSON := json.Marshal(object)
	if errJSON != nil {
		return nil, errJSON
	}
	return json.Marshal(struct {
		Type    string          `json:"type"`
		Session json.RawMessage `json:"session"`
	}{"session.update", session})
}
