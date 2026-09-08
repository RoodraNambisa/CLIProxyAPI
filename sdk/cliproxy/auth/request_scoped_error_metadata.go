package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

var requestScopedRuleMetadataKeys = [...]string{"request_scoped_errors", "request-scoped-errors"}

type authRequestScopedErrorSnapshot struct {
	digest [32]byte
	rules  *config.CompiledRequestScopedErrors
}

// The caller owns auth until publication. Unchanged persistence snapshots reuse
// the compiled object; neither matching nor result persistence compiles regexes.
func prepareAuthRequestScopedErrors(auth *Auth) error {
	if auth == nil {
		return nil
	}
	var source map[string]any
	for _, key := range requestScopedRuleMetadataKeys {
		if value, exists := auth.Metadata[key]; exists {
			if source == nil {
				source = make(map[string]any, 2)
			}
			source[key] = value
		}
	}
	if source == nil {
		if auth.requestScopedErrorRules != nil {
			auth.requestScopedErrorRules = nil
		}
		return nil
	}
	encoded, err := json.Marshal(source)
	if err != nil {
		return fmt.Errorf("request-scoped-errors contains invalid rule data")
	}
	digest := sha256.Sum256(encoded)
	if current := auth.requestScopedErrorRules; current != nil && current.digest == digest {
		return nil
	}
	rules, err := compileAuthRequestScopedErrors(auth)
	if err != nil {
		return err
	}
	auth.requestScopedErrorRules = &authRequestScopedErrorSnapshot{digest: digest, rules: rules}
	return nil
}

func compileAuthRequestScopedErrors(auth *Auth) (*config.CompiledRequestScopedErrors, error) {
	if auth == nil {
		return nil, nil
	}
	var selected *config.CompiledRequestScopedErrors
	selectedSet := false
	for _, key := range requestScopedRuleMetadataKeys {
		raw, exists := auth.Metadata[key]
		if !exists {
			continue
		}
		var rules []config.RequestScopedErrorRule
		encoded, err := json.Marshal(raw)
		if err != nil {
			return nil, fmt.Errorf("%s contains invalid rule data", key)
		}
		if err := json.Unmarshal(encoded, &rules); err != nil {
			return nil, fmt.Errorf("%s contains invalid rule types", key)
		}
		compiled, err := config.CompileRequestScopedErrors(rules)
		if err != nil {
			return nil, err
		}
		if !selectedSet {
			selected, selectedSet = compiled, true
		}
	}
	return selected, nil
}

// ValidateAuthRequestScopedErrors checks both metadata aliases before any
// persistence. The canonical underscore spelling wins, including explicit null.
func ValidateAuthRequestScopedErrors(auth *Auth) error {
	_, err := compileAuthRequestScopedErrors(auth)
	return err
}

func cloneRequestScopedRuleMetadata(value any) any {
	if rules, ok := value.([]config.RequestScopedErrorRule); ok {
		cloned := slices.Clone(rules)
		for index := range cloned {
			cloned[index].Match = slices.Clone(cloned[index].Match)
			cloned[index].MatchRegexr = slices.Clone(cloned[index].MatchRegexr)
		}
		return cloned
	}
	// Preserve unknown rule fields and exact JSON numbers in imported metadata.
	encoded, err := json.Marshal(value)
	if err != nil {
		return value
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var cloned any
	if err := decoder.Decode(&cloned); err != nil {
		return value
	}
	return cloned
}

// Error rules are local configuration; a token refresh must not learn them.
func carryForwardConfiguredRequestScopedErrors(current, next *Auth) {
	if current == nil || next == nil {
		return
	}
	for _, key := range requestScopedRuleMetadataKeys {
		if value, exists := current.Metadata[key]; exists {
			if next.Metadata == nil {
				next.Metadata = make(map[string]any)
			}
			next.Metadata[key] = cloneRequestScopedRuleMetadata(value)
		} else {
			delete(next.Metadata, key)
		}
	}
	next.requestScopedErrorRules = current.requestScopedErrorRules
}

func requestScopedErrorConfigurationChanged(baseline, current *Auth) bool {
	if baseline == nil || current == nil {
		return false
	}
	for _, key := range requestScopedRuleMetadataKeys {
		before, beforeSet := baseline.Metadata[key]
		now, nowSet := current.Metadata[key]
		if beforeSet != nowSet || !reflect.DeepEqual(before, now) {
			return true
		}
	}
	return false
}
