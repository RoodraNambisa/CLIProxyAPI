package management

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/credentialweight"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

// A present null clears an override; omission leaves the saved value untouched.
type credentialWeightPatch struct {
	value *int
	set   bool
}

func (p *credentialWeightPatch) UnmarshalJSON(data []byte) error {
	p.set = true
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		p.value = nil
		return nil
	}
	var value int64
	if err := json.Unmarshal(data, &value); err != nil {
		return errors.New("weight must be an integer or null")
	}
	normalized, err := credentialweight.Normalize(value)
	if err != nil {
		return err
	}
	weight := int(normalized)
	p.value = &weight
	return nil
}

func configuredAuthFileWeight(auth *coreauth.Auth) (int64, bool) {
	if auth == nil {
		return 0, false
	}
	if value, exists := auth.Attributes[coreauth.AttributeWeight]; exists && strings.TrimSpace(value) != "" {
		weight, err := credentialweight.ParseString(value)
		return weight, err == nil
	}
	if value, exists := auth.Metadata[coreauth.AttributeWeight]; exists {
		weight, err := credentialweight.ParseValue(value)
		return weight, err == nil
	}
	return 0, false
}
