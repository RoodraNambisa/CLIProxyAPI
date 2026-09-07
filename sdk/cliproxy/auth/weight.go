package auth

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/credentialweight"
)

const AttributeWeight = "weight"

// ValidateAuthWeight checks both sources even when attributes take precedence.
// Invalid updates must not reach persistence or replace an installed instance.
func ValidateAuthWeight(auth *Auth) error {
	if auth == nil {
		return nil
	}
	if value, exists := auth.Attributes[AttributeWeight]; exists {
		if _, err := credentialweight.ParseString(value); err != nil {
			return fmt.Errorf("invalid attributes weight: %w", err)
		}
	}
	if value, exists := auth.Metadata[AttributeWeight]; exists {
		if _, err := credentialweight.ParseValue(value); err != nil {
			return fmt.Errorf("invalid metadata weight: %w", err)
		}
	}
	return nil
}

// Weight is local configuration, never a field learned during token refresh.
// Preserve both roles so a refreshed attribute cannot shadow a concurrent edit.
func carryForwardConfiguredAuthWeight(current, next *Auth) {
	if current == nil || next == nil {
		return
	}
	if value, exists := current.Attributes[AttributeWeight]; exists {
		if next.Attributes == nil {
			next.Attributes = make(map[string]string)
		}
		next.Attributes[AttributeWeight] = value
	} else {
		delete(next.Attributes, AttributeWeight)
	}
	if value, exists := current.Metadata[AttributeWeight]; exists {
		if next.Metadata == nil {
			next.Metadata = make(map[string]any)
		}
		next.Metadata[AttributeWeight] = value
	} else {
		delete(next.Metadata, AttributeWeight)
	}
}

func authWeightConfigurationChanged(baseline, current *Auth) bool {
	if baseline == nil || current == nil {
		return false
	}
	oldAttribute, oldAttributeSet := baseline.Attributes[AttributeWeight]
	attribute, attributeSet := current.Attributes[AttributeWeight]
	oldValue, oldValueSet := baseline.Metadata[AttributeWeight]
	value, valueSet := current.Metadata[AttributeWeight]
	return oldAttributeSet != attributeSet || oldAttribute != attribute ||
		oldValueSet != valueSet || !reflect.DeepEqual(oldValue, value)
}

func authWeight(auth *Auth) int64 {
	if auth == nil {
		return credentialweight.Default
	}
	if value, exists := auth.Attributes[AttributeWeight]; exists && strings.TrimSpace(value) != "" {
		weight, err := credentialweight.ParseString(value)
		if err != nil {
			return 0
		}
		return weight
	}
	if value, exists := auth.Metadata[AttributeWeight]; exists {
		weight, err := credentialweight.ParseValue(value)
		if err != nil {
			return 0
		}
		return weight
	}
	return credentialweight.Default
}

func (m *Manager) weightedEligibleAuths(auths []*Auth, contexts ...context.Context) []*Auth {
	var eligible []*Auth
	for index, candidate := range auths {
		excluded := candidate != nil && m.routingStrategyForPriority(authPriority(candidate), contexts...) == schedulerStrategyWeightedRoundRobin && authWeight(candidate) <= 0
		if excluded {
			if eligible == nil {
				eligible = make([]*Auth, 0, len(auths))
				eligible = append(eligible, auths[:index]...)
			}
		} else if eligible != nil {
			eligible = append(eligible, candidate)
		}
	}
	if eligible == nil {
		return auths
	}
	return eligible
}

// ApplyAuthWeightMetadata imports a validated file weight into scheduling
// attributes. The caller owns the mutable Auth and its attribute map.
func ApplyAuthWeightMetadata(auth *Auth, metadata map[string]any) error {
	if err := ValidateAuthWeight(auth); err != nil {
		return err
	}
	if auth == nil {
		return nil
	}
	value, exists := metadata[AttributeWeight]
	if !exists {
		return nil
	}
	weight, err := credentialweight.ParseValue(value)
	if err != nil {
		return fmt.Errorf("invalid metadata weight: %w", err)
	}
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes[AttributeWeight] = strconv.FormatInt(weight, 10)
	return nil
}
