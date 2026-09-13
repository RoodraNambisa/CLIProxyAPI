package chatgptweb

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
)

func newCollectionTestVM(t *testing.T) *conversationTurnstileVM {
	t.Helper()
	return newCompatibilityVM(t, sentinelcompat.Config{})
}

func newTestWeakSet(t *testing.T, vm *conversationTurnstileVM, items ...any) *conversationTurnstileWeakSet {
	t.Helper()
	value, err := vm.call(conversationTurnstileObjectRef{path: "window.Reflect.construct"}, []any{
		conversationTurnstileObjectRef{path: "window.WeakSet"},
		conversationTurnstileArrayValue([]any{conversationTurnstileArrayValue(items)}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return value.(*conversationTurnstileWeakSet)
}

func TestConversationTurnstileWeakSetIdentityAndMethods(t *testing.T) {
	vm := newCollectionTestVM(t)
	object := newConversationTurnstileOrderedMap()
	other := newConversationTurnstileOrderedMap()
	object.set("value", 1)
	other.set("value", 1)
	set := newTestWeakSet(t, vm, object, object)
	if len(set.entries) != 1 {
		t.Fatal("duplicate identity created another entry")
	}
	for _, tc := range []struct {
		method string
		value  any
		want   any
	}{
		{"has", object, true}, {"has", other, false},
		{"has", nil, false}, {"delete", "not-an-object", false},
		{"add", other, set}, {"has", other, true},
		{"delete", object, true}, {"delete", object, false}, {"has", object, false},
	} {
		method, err := vm.bindProperty(set, tc.method)
		if err != nil {
			t.Fatal(err)
		}
		got, err := vm.call(method, []any{tc.value})
		if err != nil || !conversationTurnstileStrictEqual(got, tc.want) {
			t.Fatalf("WeakSet.%s(%T) = %v, %v", tc.method, tc.value, got, err)
		}
	}
	for _, value := range []any{nil, conversationTurnstileUndefined, conversationTurnstileExplicitNull, false, 1, "x"} {
		_, err := vm.weakSetOperation(set, "add", value)
		var jsErr conversationTurnstileJSError
		if !errors.As(err, &jsErr) || jsErr.name != "TypeError" {
			t.Fatalf("primitive add %T: %v", value, err)
		}
	}
	if got, err := vm.property(set, "size"); err != nil || !isConversationTurnstileUndefined(got) {
		t.Fatal("WeakSet exposed a size property")
	}
	if got := conversationTurnstileString(set); got != "[object WeakSet]" {
		t.Fatalf("WeakSet string = %q", got)
	}
	if got, err := marshalConversationTurnstileJSON(set); err != nil || string(got) != "{}" {
		t.Fatalf("WeakSet JSON = %q, %v", got, err)
	}
	if conversationTurnstileStrictEqual(set, newTestWeakSet(t, vm)) {
		t.Fatal("separate sets share identity")
	}
	selfSet := newTestWeakSet(t, vm, set)
	if got, err := vm.weakSetOperation(selfSet, "has", set); err != nil || got != true {
		t.Fatalf("WeakSet key identity lost: %v, %v", got, err)
	}
}

func TestConversationTurnstileBindPreservesArgumentsAndReceiver(t *testing.T) {
	vm := newCollectionTestVM(t)
	object := newConversationTurnstileOrderedMap()
	bind, err := vm.property(conversationTurnstileObjectRef{path: "window.Reflect.set"}, "bind")
	if err != nil || !conversationTurnstileCallableValue(bind) {
		t.Fatalf("Reflect.set.bind = %v, %v", bind, err)
	}
	args := []any{nil, object, "value"}
	bound, err := vm.call(bind, args)
	if err != nil {
		t.Fatal(err)
	}
	args[1] = newConversationTurnstileOrderedMap()
	args[2] = "changed-after-bind"
	if got, err := vm.call(bound, []any{7}); err != nil || got != true {
		t.Fatalf("bound set = %v, %v", got, err)
	}
	if got, _ := object.get("value"); got != 7 {
		t.Fatal("bound arguments were not copied or object identity was lost")
	}
	bindAgain, err := vm.bindProperty(bound, "bind")
	if err != nil {
		t.Fatal(err)
	}
	rebound, err := vm.call(bindAgain, []any{nil, 9})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := vm.call(rebound, []any{12}); err != nil || got != false {
		t.Fatalf("primitive receiver = %v, %v", got, err)
	}
	if got, _ := object.get("value"); got != 7 {
		t.Fatal("primitive receiver changed the bound target")
	}
	set := newTestWeakSet(t, vm, object)
	has, err := vm.bindProperty(set, "has")
	if err != nil {
		t.Fatal(err)
	}
	hasBind, err := vm.bindProperty(has, "bind")
	if err != nil {
		t.Fatal(err)
	}
	hasObject, err := vm.call(hasBind, []any{set, object})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := vm.call(hasObject, nil); err != nil || got != true {
		t.Fatalf("bound WeakSet.has = %v, %v", got, err)
	}
	if got := solveCompatibilitySDKFixture(t, nil, `(()=>{const object={};const bound=Reflect.set.bind(null,object,"value");const a=bound(7);bound.bind(null,9)(12);const set=new WeakSet([object]);return [a,object.value,set.has.bind(set,object)()]})()`); got != `[true,7,true]` {
		t.Fatalf("SDK binding semantics differ: %s", got)
	}
	if got, err := vm.call(rebound, nil); err != nil || got != true {
		t.Fatalf("rebound set = %v, %v", got, err)
	}
	if got, _ := object.get("value"); got != 9 {
		t.Fatal("rebinding lost its added argument")
	}
	receiver := newConversationTurnstileOrderedMap()
	if got, err := vm.call(rebound, []any{receiver}); err != nil || got != true {
		t.Fatalf("object receiver = %v, %v", got, err)
	}
	if got, _ := receiver.get("value"); got != 9 {
		t.Fatal("Reflect.set did not write to its receiver")
	}
}

func TestConversationTurnstileNewOperationsRetainLimitsAndFallback(t *testing.T) {
	vm := newCollectionTestVM(t)
	set := newTestWeakSet(t, vm)
	bound, err := vm.bindFunction(conversationTurnstileObjectRef{path: "window.Reflect.set"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for range conversationTurnstileMaxQueueDepth - 1 {
		bound, err = vm.bindFunction(bound, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := vm.bindFunction(bound, nil); err == nil {
		t.Fatal("function binding depth is unbounded")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	vm.ctx = ctx
	if _, err := vm.call(bound, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("bound invocation ignored cancellation: %v", err)
	}
	if _, err := vm.weakSetOperation(set, "has", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("WeakSet ignored cancellation: %v", err)
	}
	vm.ctx = t.Context()
	vm.memoryBudget.used = conversationTurnstileMaxRuntimeBytes
	if _, err := vm.weakSetOperation(set, "add", newConversationTurnstileOrderedMap()); err == nil || len(set.entries) != 0 {
		t.Fatal("WeakSet exceeded its memory budget")
	}
	if _, err := vm.bindFunction(conversationTurnstileObjectRef{path: "window.Reflect.set"}, nil); err == nil {
		t.Fatal("binding exceeded its memory budget")
	}
	vm = newCollectionTestVM(t)
	weakSet := conversationTurnstileObjectRef{path: "window.WeakSet"}
	for _, args := range [][]any{
		{conversationTurnstileObjectRef{path: "window.Date"}, []any{}},
		{weakSet, []any{}, conversationTurnstileObjectRef{path: "window.Object"}},
		{weakSet, newConversationTurnstileOrderedMap()},
		{weakSet, []any{newConversationTurnstileOrderedMap()}},
	} {
		_, err := vm.reflectConstruct(args)
		var compat *SentinelCompatibilityError
		if !errors.As(err, &compat) {
			t.Fatalf("unmodeled constructor semantics did not fall back: %v", err)
		}
	}
	if _, err := vm.call(weakSet, nil); err == nil {
		t.Fatal("WeakSet constructor accepted a call without new")
	}
}

func TestConversationTurnstileNumberIsSafeInteger(t *testing.T) {
	vm := newCollectionTestVM(t)
	method, err := vm.property(conversationTurnstileObjectRef{path: "window.Number"}, "isSafeInteger")
	if err != nil || !conversationTurnstileCallableValue(method) {
		t.Fatalf("Number.isSafeInteger = %v, %v", method, err)
	}
	for _, tc := range []struct {
		value any
		want  bool
	}{
		{0, true}, {-1, true}, {9007199254740991.0, true}, {-9007199254740991.0, true},
		{9007199254740992.0, false}, {-9007199254740992.0, false}, {1.5, false},
		{math.NaN(), false}, {math.Inf(1), false}, {"1", false}, {false, false},
		{nil, false}, {conversationTurnstileUndefined, false}, {json.Number("10"), true},
		{&conversationTurnstileBoxedPrimitive{value: 1}, false},
	} {
		if got, err := vm.call(method, []any{tc.value}); err != nil || got != tc.want {
			t.Fatalf("Number.isSafeInteger(%v) = %v, %v; want %v", tc.value, got, err, tc.want)
		}
	}
	if got, err := vm.call(method, nil); err != nil || got != false {
		t.Fatalf("Number.isSafeInteger() = %v, %v", got, err)
	}
	if got := solveCompatibilitySDKFixture(t, nil, `[0,-1,9007199254740991,-9007199254740991,9007199254740992,1.5,NaN,Infinity,"1",false,null,undefined,new Number(1)].map(Number.isSafeInteger)`); got != `[true,true,true,true,false,false,false,false,false,false,false,false,false]` {
		t.Fatalf("SDK safe-integer semantics differ: %s", got)
	}
}

func TestConversationTurnstileObserverStateRemainsRequestLocal(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		vm := newCompatibilityVM(t, sentinelcompat.Config{Enabled: enabled})
		other := newCompatibilityVM(t, sentinelcompat.Config{Enabled: enabled})
		window := conversationTurnstileObjectRef{path: "window"}
		values := map[string]any{
			"__oai_so_hpm": newConversationTurnstileCallable(func([]any) (any, error) { return 1, nil }),
			"__oai_so_pm":  newConversationTurnstileOrderedMap(),
			"__oai_so_wd":  0,
		}
		for key, value := range values {
			if ok, err := vm.reflectSetObjectRefProperty(window, key, value); err != nil || !ok {
				t.Fatalf("collector state %s rejected: %v", key, err)
			}
			if got, err := vm.property(window, key); err != nil || !conversationTurnstileStrictEqual(got, value) {
				t.Fatalf("collector state %s not preserved: %v", key, err)
			}
			if got, err := other.property(window, key); err != nil || !isConversationTurnstileUndefined(got) {
				t.Fatalf("collector state %s leaked to another request: %v", key, err)
			}
		}
		if ok, err := vm.reflectSetObjectRefProperty(window, "__oai_so_unknown_callback", values["__oai_so_hpm"]); ok || (enabled && err == nil) {
			t.Fatal("unknown callbacks bypassed primitive-only compatibility policy")
		}
		for _, key := range []string{"fetch", "document", "__proto__"} {
			if ok, err := vm.reflectSetObjectRefProperty(window, key, 0); ok || err != nil {
				t.Fatalf("protected host property %s was modified: %v", key, err)
			}
		}
	}
}
