package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"

	"gopkg.in/yaml.v3"
)

// APIKeyPriorityLimit keeps management JSON integers exact in JavaScript clients.
const APIKeyPriorityLimit = 9007199254740991

type APIKeyPriorityList []int

func (values *APIKeyPriorityList) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		*values = nil
		return nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(data, &items); err != nil {
		return errors.New("API key priorities must be an array of integers")
	}
	next := make(APIKeyPriorityList, 0, len(items))
	for _, item := range items {
		var value int64
		if bytes.Equal(bytes.TrimSpace(item), []byte("null")) || json.Unmarshal(item, &value) != nil || !validAPIKeyPriority(value) {
			return errors.New("API key priority must be an integer between -9007199254740991 and 9007199254740991")
		}
		next = append(next, int(value))
	}
	*values = next
	return nil
}

func (values *APIKeyPriorityList) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	if node == nil {
		return errors.New("invalid API key priority alias")
	}
	if node.Tag == "!!null" {
		*values = nil
		return nil
	}
	if node.Kind != yaml.SequenceNode {
		return errors.New("API key priorities must be an array of integers")
	}
	next := make(APIKeyPriorityList, 0, len(node.Content))
	for _, item := range node.Content {
		if item.Kind == yaml.AliasNode {
			item = item.Alias
		}
		var value int64
		if item.Tag != "!!int" || item.Decode(&value) != nil || !validAPIKeyPriority(value) {
			return errors.New("API key priority must be an integer between -9007199254740991 and 9007199254740991")
		}
		next = append(next, int(value))
	}
	*values = next
	return nil
}

func validAPIKeyPriority(value int64) bool {
	return value >= -APIKeyPriorityLimit && value <= APIKeyPriorityLimit && int64(int(value)) == value
}

// ValidateAPIKeyPriorities also protects callers that construct SDK config directly.
func (cfg *Config) ValidateAPIKeyPriorities() error {
	if cfg == nil {
		return nil
	}
	for _, group := range cfg.APIKeyGroups {
		for _, list := range []APIKeyPriorityList{group.AllowedPriorities, group.ExcludedPriorities} {
			for _, value := range list {
				if !validAPIKeyPriority(int64(value)) {
					return errors.New("API key priority exceeds the supported integer range")
				}
			}
		}
	}
	return nil
}

func normalizeAPIKeyPriorities(values APIKeyPriorityList) (APIKeyPriorityList, error) {
	for _, value := range values {
		if !validAPIKeyPriority(int64(value)) {
			return nil, errors.New("API key priority exceeds the supported integer range")
		}
	}
	next := slices.Clone(values)
	slices.Sort(next)
	return slices.Compact(next), nil
}
