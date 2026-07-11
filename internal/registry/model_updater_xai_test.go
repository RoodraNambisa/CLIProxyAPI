package registry

import "testing"

func TestDetectChangedProvidersIncludesXAI(t *testing.T) {
	current := getModels()
	oldData := *current
	newData := oldData
	newData.XAI = cloneModelInfos(oldData.XAI)
	newData.XAI[0].DisplayName += " updated"

	changed := detectChangedProviders(&oldData, &newData)
	for _, provider := range changed {
		if provider == "xai" {
			return
		}
	}
	t.Fatalf("detectChangedProviders() = %v, want xai", changed)
}

func TestValidateModelsCatalogRequiresXAI(t *testing.T) {
	data := *getModels()
	data.XAI = nil
	if err := validateModelsCatalog(&data); err == nil {
		t.Fatal("validateModelsCatalog() error = nil, want empty xai section error")
	}
}
