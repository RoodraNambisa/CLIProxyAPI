package registry

import (
	"slices"
	"strings"
)

// ChatGPTWebImageThinkingModel selects only advertised efforts from this client's
// cached catalog. The website uses different effort names across model families.
func (r *ModelRegistry) ChatGPTWebImageThinkingModel(clientID, carrier, mode string) (model, effort string) {
	var aliases []string
	switch mode {
	case "low":
		aliases = []string{"low", "min"}
	case "medium":
		aliases = []string{"medium", "standard"}
	case "high":
		aliases = []string{"high", "extended"}
	case "xhigh":
		aliases = []string{"extra_high", "xhigh", "max"}
	default:
		return "", ""
	}
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	selectedDefault := false
	for _, info := range r.clientModelInfos[clientID] {
		if info == nil {
			continue
		}
		candidate := strings.TrimSpace(info.UpstreamID)
		if candidate == "" {
			candidate = strings.TrimSpace(info.ID)
		}
		if candidate == "" || (carrier != "auto" && candidate != carrier && info.ID != carrier) {
			continue
		}
		for _, native := range aliases {
			if !slices.Contains(info.ChatGPTWebThinkingEfforts, native) {
				continue
			}
			preferred := info.ChatGPTWebThinkingDefault
			if model == "" || (preferred && !selectedDefault) || preferred == selectedDefault && candidate < model {
				model, effort, selectedDefault = candidate, native, preferred
			}
			break
		}
	}
	return model, effort
}
