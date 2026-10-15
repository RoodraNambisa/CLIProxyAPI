package chatgptweb

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestGoConversationTurnstileStorageEstimateIsAwaitable(t *testing.T) {
	vm := &conversationTurnstileVM{ctx: t.Context(), memoryBudget: &conversationTurnstileMemoryBudget{}}
	storage, err := vm.property(conversationTurnstileObjectRef{path: "window.navigator"}, "storage")
	if err != nil || isConversationTurnstileUndefined(storage) {
		t.Fatal("navigator.storage is unavailable")
	}
	estimate, err := vm.property(storage, "estimate")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		promise, errCall := vm.call(estimate, nil)
		if errCall != nil {
			t.Fatal(errCall)
		}
		then, errThen := vm.property(promise, "then")
		if errThen != nil || isConversationTurnstileUndefined(then) {
			t.Fatal("estimate must return a thenable")
		}
		resolved, settled, errResolve := vm.resolveThenable(promise, 0)
		if errResolve != nil || !settled {
			t.Fatalf("estimate did not settle: %v", errResolve)
		}
		for _, key := range []string{"quota", "usage"} {
			value, errProperty := vm.property(resolved, key)
			if errProperty != nil || value != float64(0) {
				t.Fatalf("ephemeral storage %s = %v, %v", key, value, errProperty)
			}
		}
		resolved.(*conversationTurnstileOrderedMap).set("quota", float64(99))
	}
}

func TestSentinelSDKStorageEstimateMatchesEphemeralRuntime(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	request, sdk := sentinelRuntimeTestRequests(t, false)
	source := strings.Replace(sentinelRuntimeTestSDK, `return "sdk-turnstile-token"`, `const result=navigator.storage.estimate();return JSON.stringify({tag:Object.prototype.toString.call(navigator.storage),thenable:typeof result.then==="function",estimate:await result})`, 1)
	sdk.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(source), "application/javascript", target, nil
	}
	result, err := manager.SolveTurnstile(t.Context(), request, sdk, nil)
	if err != nil {
		t.Fatal(err)
	}
	var value struct {
		Tag      string             `json:"tag"`
		Thenable bool               `json:"thenable"`
		Estimate map[string]float64 `json:"estimate"`
	}
	if err = json.Unmarshal([]byte(result), &value); err != nil {
		t.Fatal(err)
	}
	if value.Tag != "[object StorageManager]" || !value.Thenable || len(value.Estimate) != 2 || value.Estimate["quota"] != 0 || value.Estimate["usage"] != 0 {
		t.Fatalf("unexpected storage estimate: %+v", value)
	}
}

func TestConversationTurnstileCallResultAwaitsThenable(t *testing.T) {
	vm := &conversationTurnstileVM{ctx: t.Context(), values: map[string]any{}, memoryBudget: &conversationTurnstileMemoryBudget{}}
	thenable := newConversationTurnstileOrderedMap()
	thenable.set("then", newConversationTurnstileCallable(func(args []any) (any, error) { return vm.call(args[0], []any{"ready"}) }))
	vm.set(40, newConversationTurnstileCallable(func([]any) (any, error) { return thenable, nil }))
	if _, err := vm.opCallResult([]any{41, 40}); err != nil {
		t.Fatal(err)
	}
	if vm.get(41) != "ready" {
		t.Fatal("opcode 17 stored the promise instead of its resolved value")
	}
}

func TestConversationTurnstileCallResultHandlesRejectedAndPendingPromises(t *testing.T) {
	for _, pending := range []bool{false, true} {
		vm := &conversationTurnstileVM{ctx: t.Context(), values: map[string]any{}, memoryBudget: &conversationTurnstileMemoryBudget{}}
		thenable := newConversationTurnstileOrderedMap()
		thenable.set("then", newConversationTurnstileCallable(func(args []any) (any, error) {
			if pending {
				return conversationTurnstileUndefined, nil
			}
			return vm.call(args[1], []any{"estimate rejected"})
		}))
		vm.set(40, newConversationTurnstileCallable(func([]any) (any, error) { return thenable, nil }))
		if _, err := vm.opCallResult([]any{41, 40}); err != nil {
			t.Fatal(err)
		}
		if pending {
			if !vm.deferred || !isConversationTurnstileUndefined(vm.get(41)) {
				t.Fatal("pending promise was treated as completed")
			}
		} else if vm.get(41) != "estimate rejected" {
			t.Fatal("promise rejection was not preserved as the opcode result")
		}
	}
}
