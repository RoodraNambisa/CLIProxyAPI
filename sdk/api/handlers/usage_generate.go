package handlers

import (
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

func setGenerateMetadata(metadata map[string]any, payload []byte) {
	if metadata != nil {
		// Only an explicit JSON boolean false denotes a non-generating request.
		metadata[core.GenerateMetadataKey] = gjson.GetBytes(payload, "generate").Type != gjson.False
	}
}
