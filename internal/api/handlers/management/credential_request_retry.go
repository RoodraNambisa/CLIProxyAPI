package management

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

// Omission keeps the saved override; explicit null restores global inheritance.
type credentialRequestRetryPatch struct {
	value *int
	set   bool
}

func (p *credentialRequestRetryPatch) UnmarshalJSON(data []byte) error {
	p.set, p.value = true, nil
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil
	}
	var parsed int64
	if err := json.Unmarshal(data, &parsed); err != nil {
		return errors.New("request-retry must be an integer or null")
	}
	if parsed > int64(config.MaxCredentialRequestRetry) {
		return errors.New("request-retry exceeds the supported range")
	}
	value := int(max(int64(0), parsed))
	p.value = &value
	return nil
}
