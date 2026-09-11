package chatgptweb

import (
	"fmt"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
)

type conversationCompatibilityState struct {
	bytes int
	sizes map[string]int
}

func (vm *conversationTurnstileVM) initializeCompatibilityProperties() error {
	return vm.challengeEnvironment.Compatibility.ForEachProperty(func(prop sentinelcompat.Property) error {
		value := prop.Value
		switch prop.Type {
		case "undefined":
			value = conversationTurnstileUndefined
		case "null":
			value = nil
		}
		if err := vm.reserveRuntimeBytes(prop.BudgetBytes()); err != nil {
			return err
		}
		vm.environment[prop.Path] = value
		return nil
	})
}

func (vm *conversationTurnstileVM) compatibilityFailure(operation string) error {
	return vm.compatibilityError(SentinelCompatibilityUnsupportedValue, "compatibility_"+operation, fmt.Errorf("compatibility property requires SDK"))
}

func (vm *conversationTurnstileVM) markCompatibilityUsed() {
	if !vm.compatibilityUsed {
		vm.compatibilityUsed = true
		if vm.challengeEnvironment.compatibilityUsage != nil {
			vm.challengeEnvironment.compatibilityUsage.Store(true)
		}
	}
}

func (vm *conversationTurnstileVM) setCompatibilityWindowProperty(key string, value any) (bool, error) {
	policy := vm.challengeEnvironment.Compatibility
	if policy == nil || (vm.programKind != SentinelProgramObserverCollect && vm.programKind != SentinelProgramObserverSnapshot) || !policy.AllowsWrite(key) {
		return false, nil
	}
	vm.markCompatibilityUsed()
	kind := ""
	plain := value
	switch v := value.(type) {
	case conversationTurnstileUndefinedValue:
		kind = "undefined"
		plain = nil
	case nil, conversationTurnstileExplicitNullValue:
		kind = "null"
		plain = nil
		value = nil
	case bool:
		kind = "boolean"
	case string:
		kind = "string"
	case conversationTurnstileJSString:
		if len(v.units)*2 > sentinelcompat.MaxStringBytes {
			return false, vm.compatibilityFailure("value_limit")
		}
		// Keep UTF-16 code units intact, including isolated surrogates.
		kind = "utf16"
	default:
		if n, ok := conversationTurnstileNumberPrimitive(value); ok {
			kind = "number"
			plain = n
		} else {
			return false, vm.compatibilityFailure("value_type")
		}
	}
	size := 16
	if kind == "utf16" {
		size += len(value.(conversationTurnstileJSString).units) * 2
	} else {
		_, n, err := sentinelcompat.NormalizeValue(kind, plain)
		if err != nil {
			return false, vm.compatibilityFailure("value_type_or_limit")
		}
		size = n
	}
	path := "window." + key
	if err := vm.chargeRuntimeWork(len(path) + size); err != nil {
		return false, err
	}
	state := vm.compatibilityState
	if state == nil {
		if err := vm.reserveRuntimeBytes(256); err != nil {
			return false, err
		}
		state = &conversationCompatibilityState{bytes: 256, sizes: make(map[string]int)}
	}
	previous, exists := state.sizes[path]
	if !exists && len(state.sizes)+policy.PropertyCount() >= sentinelcompat.MaxProperties {
		return false, vm.compatibilityFailure("field_limit")
	}
	size += len(path) + 64
	if state.bytes-previous+size+policy.PropertyBytes() > sentinelcompat.MaxStateBytes {
		return false, vm.compatibilityFailure("state_limit")
	}
	if err := vm.reserveRuntimeBytes(size); err != nil {
		return false, err
	}
	if v, ok := value.(conversationTurnstileJSString); ok {
		value = conversationTurnstileJSString{units: append([]uint16(nil), v.units...)}
	}
	state.bytes += size - previous
	state.sizes[path] = size
	vm.compatibilityState = state
	vm.environment[path] = value
	return true, nil
}

func (vm *conversationTurnstileVM) compatibilityHasTarget(target string) bool {
	if vm.challengeEnvironment.Compatibility == nil {
		return false
	}
	if target == "window" && vm.compatibilityState != nil && len(vm.compatibilityState.sizes) > 0 {
		return true
	}
	found := false
	_ = vm.challengeEnvironment.Compatibility.ForEachProperty(func(p sentinelcompat.Property) error {
		if p.Path[:strings.LastIndexByte(p.Path, '.')] == target {
			found = true
		}
		return nil
	})
	if found {
		vm.markCompatibilityUsed()
	}
	return found
}
