package auth

import (
	"bytes"
	"encoding/json"
	"reflect"
)

// Merge JSON metadata against the refresh baseline. Concurrent current edits
// win conflicts; independent refresh changes and explicit deletions are retained.
func mergeCodexRefreshMetadata(base, current, refreshed map[string]any) map[string]any {
	keys := make(map[string]struct{}, len(base)+len(current)+len(refreshed))
	for key := range base {
		keys[key] = struct{}{}
	}
	for key := range current {
		keys[key] = struct{}{}
	}
	for key := range refreshed {
		keys[key] = struct{}{}
	}
	if len(keys) == 0 {
		if current == nil && refreshed == nil {
			return nil
		}
		return map[string]any{}
	}
	merged := make(map[string]any, len(keys))
	for key := range keys {
		before, beforeOK := base[key]
		now, nowOK := current[key]
		after, afterOK := refreshed[key]
		value, present := mergeCodexMetadataValue(before, beforeOK, now, nowOK, after, afterOK)
		if present {
			merged[key] = value
		}
	}
	return merged
}

func mergeCodexMetadataValue(base any, baseOK bool, current any, currentOK bool, refreshed any, refreshedOK bool) (any, bool) {
	if codexMetadataValuesEqual(base, baseOK, current, currentOK) {
		return cloneCodexMetadataValue(refreshed), refreshedOK
	}
	if codexMetadataValuesEqual(base, baseOK, refreshed, refreshedOK) {
		return cloneCodexMetadataValue(current), currentOK
	}
	baseMap, baseObject := base.(map[string]any)
	currentMap, currentObject := current.(map[string]any)
	refreshedMap, refreshedObject := refreshed.(map[string]any)
	if currentOK && refreshedOK && currentObject && refreshedObject && (baseObject || !baseOK) {
		return mergeCodexRefreshMetadata(baseMap, currentMap, refreshedMap), true
	}
	return cloneCodexMetadataValue(current), currentOK
}

func codexMetadataValuesEqual(left any, leftOK bool, right any, rightOK bool) bool {
	if leftOK != rightOK {
		return false
	}
	if !leftOK || reflect.DeepEqual(left, right) {
		return true
	}
	// Runtime metadata and decoded JSON can represent the same number or array
	// using different Go types. That alone is not a concurrent semantic edit.
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && bytes.Equal(a, b)
}

func cloneCodexMetadataValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		if typed == nil {
			return typed
		}
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[key] = cloneCodexMetadataValue(item)
		}
		return result
	case []any:
		if typed == nil {
			return typed
		}
		result := make([]any, len(typed))
		for index, item := range typed {
			result[index] = cloneCodexMetadataValue(item)
		}
		return result
	case map[string]string:
		if typed == nil {
			return typed
		}
		result := make(map[string]string, len(typed))
		for key, item := range typed {
			result[key] = item
		}
		return result
	case []string:
		if typed == nil {
			return typed
		}
		return append([]string{}, typed...)
	default:
		return value
	}
}
