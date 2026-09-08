package management

import (
	"bytes"
	"encoding/json"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type authFileErrorRulesPatch struct {
	set   bool
	value any
}

func (patch *authFileErrorRulesPatch) UnmarshalJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	if err := coreauth.ValidateAuthRequestScopedErrors(&coreauth.Auth{
		Metadata: map[string]any{"request_scoped_errors": value},
	}); err != nil {
		return err
	}
	// Keep unknown rule fields and exact numbers in imported auth metadata.
	patch.set, patch.value = true, value
	return nil
}
