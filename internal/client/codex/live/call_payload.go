package live

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

const maxCallBodySize = 16 << 20

var errCallBodyTooLarge = errors.New("realtime call body exceeds 16 MiB")

type callPayload struct {
	body        []byte
	contentType string
	model       string
}

func readCallBody(reader io.Reader) ([]byte, error) {
	if reader == nil {
		return nil, nil
	}
	body, errRead := io.ReadAll(io.LimitReader(reader, maxCallBodySize+1))
	if len(body) > maxCallBodySize {
		return body[:maxCallBodySize], errors.Join(errCallBodyTooLarge, errRead)
	}
	return body, errRead
}

// prepareCallPayload keeps native session fields opaque. It never runs a
// Responses translator or inserts cache/identity fields into the session.
func prepareCallPayload(body []byte, contentType string) (callPayload, error) {
	if len(body) > maxCallBodySize {
		return callPayload{}, errCallBodyTooLarge
	}
	if strings.TrimSpace(contentType) == "" {
		contentType = "application/json"
	}
	mediaType, params, errType := mime.ParseMediaType(contentType)
	if errType != nil {
		return callPayload{}, errors.New("invalid realtime call content type")
	}
	switch strings.ToLower(mediaType) {
	case "multipart/form-data":
		return prepareMultipartCall(body, params["boundary"])
	case "application/sdp", "text/plain":
		if len(bytes.TrimSpace(body)) == 0 {
			return callPayload{}, errors.New("realtime call requires an SDP offer")
		}
		return callPayload{body: body, contentType: contentType, model: registry.CodexLiveModelID}, nil
	case "application/json":
		payload, errObject := callJSONObject(body, "call")
		if errObject != nil {
			return callPayload{}, errObject
		}
		model, errModel := callModel(payload)
		if errModel != nil {
			return callPayload{}, errModel
		}
		return callPayload{body: body, contentType: "application/json", model: model}, nil
	default:
		return callPayload{}, errors.New("realtime call requires JSON, SDP or multipart form data")
	}
}

func callJSONObject(body []byte, name string) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if errJSON := json.Unmarshal(body, &object); errJSON != nil || object == nil {
		return nil, fmt.Errorf("realtime %s must be a JSON object", name)
	}
	return object, nil
}

func callModel(payload map[string]json.RawMessage) (string, error) {
	model := ""
	if raw, exists := payload["model"]; exists {
		if errJSON := json.Unmarshal(raw, &model); errJSON != nil {
			return "", errors.New("realtime model must be a string")
		}
	}
	if raw, exists := payload["session"]; exists {
		session, errObject := callJSONObject(raw, "session")
		if errObject != nil {
			return "", errObject
		}
		if rawModel, exists := session["model"]; exists {
			var sessionModel string
			if errJSON := json.Unmarshal(rawModel, &sessionModel); errJSON != nil {
				return "", errors.New("realtime session model must be a string")
			}
			if strings.TrimSpace(sessionModel) != "" {
				model = sessionModel
			}
		}
	}
	model = strings.TrimSpace(model)
	if model == "" {
		model = registry.CodexLiveModelID
	}
	return model, nil
}

func prepareMultipartCall(body []byte, boundary string) (callPayload, error) {
	if boundary == "" {
		return callPayload{}, errors.New("realtime multipart boundary is missing")
	}
	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	payload := make(map[string]json.RawMessage)
	for {
		part, errPart := reader.NextPart()
		if errors.Is(errPart, io.EOF) {
			break
		}
		if errPart != nil {
			return callPayload{}, errors.New("invalid realtime multipart body")
		}
		data, errRead := io.ReadAll(part)
		errClose := part.Close()
		if errRead != nil || errClose != nil {
			return callPayload{}, errors.New("failed to read realtime multipart field")
		}
		switch part.FormName() {
		case "sdp":
			payload["sdp"], _ = json.Marshal(string(data))
		case "session":
			if _, errObject := callJSONObject(data, "session"); errObject != nil {
				return callPayload{}, errObject
			}
			payload["session"] = data
		}
	}
	if _, exists := payload["sdp"]; !exists {
		return callPayload{}, errors.New("realtime multipart body requires an sdp field")
	}
	encoded, errJSON := json.Marshal(payload)
	if errJSON != nil {
		return callPayload{}, errJSON
	}
	return prepareCallPayload(encoded, "application/json")
}

// withModel applies only the model resolved by the credential lease. Both
// protocol mirrors are updated, without touching tools or arbitrary user JSON.
func (p callPayload) withModel(model string) (callPayload, error) {
	if model == registry.CodexRealtimeModelID {
		model = registry.CodexLiveModelID
	}
	var payload map[string]json.RawMessage
	if p.contentType == "application/json" {
		var errObject error
		payload, errObject = callJSONObject(p.body, "call")
		if errObject != nil {
			return callPayload{}, errObject
		}
	} else {
		sdp, _ := json.Marshal(string(p.body))
		payload = map[string]json.RawMessage{"sdp": sdp}
	}
	encodedModel, _ := json.Marshal(model)
	if _, exists := payload["model"]; exists {
		payload["model"] = encodedModel
	}
	if raw, exists := payload["session"]; exists {
		session, errObject := callJSONObject(raw, "session")
		if errObject != nil {
			return callPayload{}, errObject
		}
		session["model"] = encodedModel
		payload["session"], _ = json.Marshal(session)
	} else if _, exists := payload["model"]; !exists {
		payload["session"], _ = json.Marshal(map[string]json.RawMessage{"model": encodedModel})
	}
	body, errJSON := json.Marshal(payload)
	if len(body) > maxCallBodySize {
		return callPayload{}, errCallBodyTooLarge
	}
	return callPayload{body: body, contentType: "application/json", model: model}, errJSON
}

func (p callPayload) withClientSecret(grant ClientSecretAuthorization) (callPayload, error) {
	var payload map[string]json.RawMessage
	if p.contentType == "application/json" {
		var errObject error
		payload, errObject = callJSONObject(p.body, "call")
		if errObject != nil {
			return callPayload{}, errObject
		}
	} else {
		sdp, _ := json.Marshal(string(p.body))
		payload = map[string]json.RawMessage{"sdp": sdp}
	}
	payload["session"] = append(json.RawMessage(nil), grant.session...)
	if _, exists := payload["model"]; exists {
		payload["model"], _ = json.Marshal(grant.model)
	}
	body, errJSON := json.Marshal(payload)
	if len(body) > maxCallBodySize {
		return callPayload{}, errCallBodyTooLarge
	}
	return callPayload{body: body, contentType: "application/json", model: grant.model}, errJSON
}
