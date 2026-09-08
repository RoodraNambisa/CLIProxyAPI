package management

import (
	"encoding/json"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

type requestScopedErrorsPatch struct {
	set   bool
	value []config.RequestScopedErrorRule
}

func (patch *requestScopedErrorsPatch) UnmarshalJSON(data []byte) error {
	var rules []config.RequestScopedErrorRule
	if err := json.Unmarshal(data, &rules); err != nil {
		return err
	}
	if _, err := config.CompileRequestScopedErrors(rules); err != nil {
		return err
	}
	patch.set, patch.value = true, rules
	return nil
}
