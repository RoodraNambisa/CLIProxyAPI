package chatgptweb

import "fmt"

// WeakSet entries are retained only for this bounded VM lifetime. Keeping their
// identities alive prevents Go address reuse from changing membership checks.
type conversationTurnstileWeakSet struct {
	entries map[string]any
}

func (vm *conversationTurnstileVM) bindFunction(target any, args []any) (any, error) {
	depth := 1
	if callable, ok := target.(conversationTurnstileCallable); ok {
		depth += callable.bindDepth
	}
	if depth > conversationTurnstileMaxQueueDepth {
		return nil, conversationTurnstileFatalError{message: "conversation turnstile function binding depth limit exceeded"}
	}
	// Modeled callables already retain their receiver; Reflect.set ignores thisArg.
	count := max(0, len(args)-1)
	if err := vm.reserveArrayElements(count, 16); err != nil {
		return nil, err
	}
	if err := vm.reserveRuntimeBytes(128); err != nil {
		return nil, err
	}
	bound := make([]any, count)
	if count > 0 {
		copy(bound, args[1:])
	}
	callable := newConversationTurnstileCallable(func(callArgs []any) (any, error) {
		count := len(bound) + len(callArgs)
		if err := vm.reserveArrayElements(count, 16); err != nil {
			return nil, err
		}
		combined := make([]any, count)
		copy(combined, bound)
		copy(combined[len(bound):], callArgs)
		return vm.call(target, combined)
	})
	callable.bindDepth = depth
	return callable, nil
}

func (vm *conversationTurnstileVM) reflectConstruct(args []any) (any, error) {
	target, ok := conversationTurnstileArgument(args, 0).(conversationTurnstileObjectRef)
	if !ok || target.path != "window.WeakSet" {
		return nil, vm.compatibilityError(SentinelCompatibilityUnsupportedValue, "Reflect.construct", fmt.Errorf("constructor requires SDK"))
	}
	if len(args) > 2 && !conversationTurnstileStrictEqual(target, args[2]) {
		return nil, vm.compatibilityError(SentinelCompatibilityUnsupportedValue, "Reflect.construct.newTarget", fmt.Errorf("custom constructor prototype requires SDK"))
	}
	arguments, ok := conversationTurnstileSlice(conversationTurnstileArgument(args, 1))
	if !ok {
		return nil, vm.compatibilityError(SentinelCompatibilityUnsupportedValue, "Reflect.construct.arguments", fmt.Errorf("constructor argument list requires SDK"))
	}
	if err := vm.reserveRuntimeBytes(128); err != nil {
		return nil, err
	}
	set := &conversationTurnstileWeakSet{entries: make(map[string]any)}
	iterable := conversationTurnstileArgument(arguments, 0)
	if iterable == nil || isConversationTurnstileUndefined(iterable) || iterable == conversationTurnstileExplicitNull {
		return set, nil
	}
	items, ok := conversationTurnstileSlice(iterable)
	if !ok {
		return nil, vm.compatibilityError(SentinelCompatibilityUnsupportedValue, "WeakSet.iterable", fmt.Errorf("iterator requires SDK"))
	}
	for _, value := range items {
		if _, err := vm.weakSetOperation(set, "add", value); err != nil {
			return nil, err
		}
	}
	return set, nil
}

func (vm *conversationTurnstileVM) weakSetProperty(set *conversationTurnstileWeakSet, key string) (any, error) {
	switch key {
	case "add", "has", "delete":
		return vm.browserCallable(func(args []any) (any, error) {
			return vm.weakSetOperation(set, key, conversationTurnstileArgument(args, 0))
		})
	case "toString":
		return vm.browserCallable(func([]any) (any, error) { return "[object WeakSet]", nil })
	default:
		return conversationTurnstileUndefined, nil
	}
}

func (vm *conversationTurnstileVM) weakSetOperation(set *conversationTurnstileWeakSet, operation string, value any) (any, error) {
	if err := vm.chargeRuntimeWork(1); err != nil {
		return nil, err
	}
	if set == nil {
		return nil, conversationTurnstileTypeError("incompatible WeakSet receiver")
	}
	if conversationTurnstilePrimitive(value) {
		if operation == "add" {
			return nil, conversationTurnstileTypeError("Invalid value used in weak set")
		}
		return false, nil
	}
	key := vm.mapKey(value)
	if vm.fatalErr != nil {
		return nil, vm.fatalErr
	}
	_, exists := set.entries[key]
	switch operation {
	case "add":
		if !exists {
			if err := vm.reserveRuntimeBytes(len(key) + 64); err != nil {
				return nil, err
			}
			set.entries[key] = value
		}
		return set, nil
	case "delete":
		delete(set.entries, key)
	}
	return exists, nil
}
