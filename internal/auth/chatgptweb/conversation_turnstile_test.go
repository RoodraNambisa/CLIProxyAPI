package chatgptweb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestBuildConversationTurnstileTokenExecutesChallenge(t *testing.T) {
	requirementsToken := "requirements-token"
	dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{2, 40, "turn"},
		[]any{2, 41, "stile"},
		[]any{5, 40, 41},
		[]any{7, 3, 40},
	})
	token, err := BuildConversationTurnstileToken(
		context.Background(), dx, requirementsToken, zeroReader{},
		func() time.Time { return time.Unix(1_700_000_000, 0) },
	)
	if err != nil {
		t.Fatalf("BuildConversationTurnstileToken() error = %v", err)
	}
	want := base64.StdEncoding.EncodeToString([]byte("turnstile"))
	if token != want {
		t.Fatalf("token = %q, want %q", token, want)
	}
}

func TestBuildConversationTurnstileTokenUsesFirstSettlement(t *testing.T) {
	requirementsToken := "requirements-token"
	dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{2, 40, "first"},
		[]any{7, 3, 40},
		[]any{2, 41, "second"},
		[]any{7, 3, 41},
		[]any{7, 4, 41},
	})
	token, err := BuildConversationTurnstileToken(context.Background(), dx, requirementsToken, zeroReader{}, time.Now)
	if err != nil {
		t.Fatalf("BuildConversationTurnstileToken() error = %v", err)
	}
	want := base64.StdEncoding.EncodeToString([]byte("first"))
	if token != want {
		t.Fatalf("token = %q, want %q", token, want)
	}
}

func TestBuildConversationTurnstileTokenStopsAfterSettlement(t *testing.T) {
	requirementsToken := "requirements-token"
	dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{2, 40, "ok"},
		[]any{7, 3, 40},
		[]any{7, 99},
	})
	token, err := BuildConversationTurnstileToken(context.Background(), dx, requirementsToken, zeroReader{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if want := base64.StdEncoding.EncodeToString([]byte("ok")); token != want {
		t.Fatalf("token = %q, want %q", token, want)
	}
}

func TestConversationTurnstileNativeConstructorsAndArrayMethods(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		values:          make(map[string]any),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 100},
	}

	stringValue, err := vm.call(conversationTurnstileObjectRef{path: "window.String"}, []any{123})
	if err != nil || stringValue != "123" {
		t.Fatalf("String(123) = %#v, %v", stringValue, err)
	}
	arrayValue, err := vm.call(conversationTurnstileObjectRef{path: "window.Array"}, []any{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	arrayItems, ok := conversationTurnstileSlice(arrayValue)
	if !ok || !reflect.DeepEqual(arrayItems, []any{"a", "b"}) {
		t.Fatalf("Array(a, b) = %#v", arrayValue)
	}
	lengthValue, err := vm.call(conversationTurnstileObjectRef{path: "window.Array"}, []any{3})
	if err != nil {
		t.Fatal(err)
	}
	lengthItems, ok := conversationTurnstileSlice(lengthValue)
	if !ok || len(lengthItems) != 3 {
		t.Fatalf("Array(3) = %#v", lengthValue)
	}
	objectValue, err := vm.call(conversationTurnstileObjectRef{path: "window.Object"}, []any{conversationTurnstileUndefined})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok = objectValue.(*conversationTurnstileOrderedMap); !ok {
		t.Fatalf("Object(undefined) = %#v", objectValue)
	}

	arrayObject := conversationTurnstileObjectRef{path: "window.Array"}
	isArray, err := vm.property(arrayObject, "isArray")
	if err != nil {
		t.Fatal(err)
	}
	isArrayValue, err := vm.call(isArray, []any{arrayValue})
	if err != nil || isArrayValue != true {
		t.Fatalf("Array.isArray(array) = %#v, %v", isArrayValue, err)
	}
	from, err := vm.property(arrayObject, "from")
	if err != nil {
		t.Fatal(err)
	}
	fromValue, err := vm.call(from, []any{"A😀"})
	if err != nil {
		t.Fatal(err)
	}
	fromItems, ok := conversationTurnstileSlice(fromValue)
	if !ok || len(fromItems) != 2 || conversationTurnstileString(fromItems[0]) != "A" || conversationTurnstileString(fromItems[1]) != "😀" {
		t.Fatalf("Array.from(A😀) = %#v", fromValue)
	}
}

func TestConversationTurnstileSparseArrayAndArrayFromSemantics(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		values:          make(map[string]any),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 100},
	}
	sparseValue, err := vm.call(conversationTurnstileObjectRef{path: "window.Array"}, []any{3})
	if err != nil {
		t.Fatal(err)
	}
	if length, err := vm.property(sparseValue, "length"); err != nil || length != 3 {
		t.Fatalf("Array(3).length = %#v, %v", length, err)
	}
	keysValue, err := vm.call(conversationTurnstileObjectRef{path: "window.Object.keys"}, []any{sparseValue})
	if err != nil {
		t.Fatal(err)
	}
	keys, _ := conversationTurnstileSlice(keysValue)
	if len(keys) != 0 {
		t.Fatalf("Object.keys(Array(3)) = %#v", keys)
	}
	denseValue, err := vm.call(conversationTurnstileObjectRef{path: "window.Array.from"}, []any{sparseValue})
	if err != nil {
		t.Fatal(err)
	}
	denseKeysValue, err := vm.call(conversationTurnstileObjectRef{path: "window.Object.keys"}, []any{denseValue})
	if err != nil {
		t.Fatal(err)
	}
	denseKeys, _ := conversationTurnstileSlice(denseKeysValue)
	if want := []any{"0", "1", "2"}; !reflect.DeepEqual(denseKeys, want) {
		t.Fatalf("Object.keys(Array.from(Array(3))) = %#v, want %#v", denseKeys, want)
	}

	for _, value := range []any{nil, conversationTurnstileExplicitNull, conversationTurnstileUndefined} {
		if _, err = vm.call(conversationTurnstileObjectRef{path: "window.Array.from"}, []any{value}); err == nil || !strings.HasPrefix(err.Error(), "TypeError:") {
			t.Fatalf("Array.from(%#v) error = %v", value, err)
		}
	}
	arrayLike := newConversationTurnstileOrderedMap()
	arrayLike.set("0", "a")
	arrayLike.set("2", "c")
	arrayLike.set("length", 3)
	mapFn := newConversationTurnstileCallable(func(args []any) (any, error) {
		return fmt.Sprintf("%s%d", conversationTurnstileString(args[0]), args[1]), nil
	})
	mappedValue, err := vm.call(conversationTurnstileObjectRef{path: "window.Array.from"}, []any{arrayLike, mapFn})
	if err != nil {
		t.Fatal(err)
	}
	mapped, _ := conversationTurnstileSlice(mappedValue)
	if want := []any{"a0", "undefined1", "c2"}; !reflect.DeepEqual(mapped, want) {
		t.Fatalf("Array.from(arrayLike, mapFn) = %#v, want %#v", mapped, want)
	}
}

func TestConversationTurnstileArrayFromReservesBeforeStringElementAllocation(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		memoryBudget:    &conversationTurnstileMemoryBudget{used: conversationTurnstileMaxRuntimeBytes - 4096},
		executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 10},
	}
	_, err := vm.arrayFromItems(strings.Repeat("a", 1024))
	var fatal conversationTurnstileFatalError
	if !errors.As(err, &fatal) || !strings.Contains(err.Error(), "runtime allocation exceeds") {
		t.Fatalf("Array.from string allocation error = %v", err)
	}

	vm = &conversationTurnstileVM{
		ctx:          context.Background(),
		memoryBudget: &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{
			maxSteps:    10,
			runtimeWork: conversationTurnstileMaxRuntimeWork - 2500,
		},
	}
	_, err = vm.arrayFromItems(conversationTurnstileJSString{units: make([]uint16, 1024)})
	if !errors.As(err, &fatal) || !strings.Contains(err.Error(), "runtime work exceeds") {
		t.Fatalf("Array.from string work error = %v", err)
	}
}

func TestConversationTurnstileObjectBoxingAndStringToPrimitive(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		values:          make(map[string]any),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 100},
	}
	boxed, err := vm.call(conversationTurnstileObjectRef{path: "window.Object"}, []any{1})
	if err != nil {
		t.Fatal(err)
	}
	if conversationTurnstileStrictEqual(boxed, 1) {
		t.Fatal("Object(1) is strictly equal to 1")
	}
	if value, err := vm.call(conversationTurnstileObjectRef{path: "window.String"}, []any{boxed}); err != nil || value != "1" {
		t.Fatalf("String(Object(1)) = %#v, %v", value, err)
	}
	boxedString, err := vm.call(conversationTurnstileObjectRef{path: "window.Object"}, []any{"ab"})
	if err != nil {
		t.Fatal(err)
	}
	keysValue, err := vm.call(conversationTurnstileObjectRef{path: "window.Object.keys"}, []any{boxedString})
	if err != nil {
		t.Fatal(err)
	}
	keys, _ := conversationTurnstileSlice(keysValue)
	if want := []any{"0", "1"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("Object.keys(Object(ab)) = %#v, want %#v", keys, want)
	}

	custom := newConversationTurnstileOrderedMap()
	custom.set("toString", newConversationTurnstileCallable(func([]any) (any, error) { return "custom", nil }))
	if value, err := vm.call(conversationTurnstileObjectRef{path: "window.String"}, []any{custom}); err != nil || value != "custom" {
		t.Fatalf("String(custom toString) = %#v, %v", value, err)
	}
	fallback := newConversationTurnstileOrderedMap()
	fallback.set("toString", newConversationTurnstileCallable(func([]any) (any, error) { return newConversationTurnstileOrderedMap(), nil }))
	fallback.set("valueOf", newConversationTurnstileCallable(func([]any) (any, error) { return 7, nil }))
	if value, err := vm.call(conversationTurnstileObjectRef{path: "window.String"}, []any{fallback}); err != nil || value != "7" {
		t.Fatalf("String(valueOf fallback) = %#v, %v", value, err)
	}
}

func TestConversationTurnstilePropertyKeyUsesStringHint(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		values:          make(map[string]any),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 100},
	}
	key := newConversationTurnstileOrderedMap()
	toStringCalls := 0
	key.set("toString", newConversationTurnstileCallable(func([]any) (any, error) {
		toStringCalls++
		return "selected", nil
	}))
	key.set("valueOf", newConversationTurnstileCallable(func([]any) (any, error) {
		return "wrong", nil
	}))

	object := newConversationTurnstileOrderedMap()
	object.set("selected", "value")
	if value, err := vm.property(object, key); err != nil || value != "value" {
		t.Fatalf("object[propertyKey] = %#v, error = %v", value, err)
	}
	if toStringCalls != 1 {
		t.Fatalf("property key toString calls = %d, want 1", toStringCalls)
	}
	if value, err := vm.bindProperty(object, key); err != nil || value != "value" {
		t.Fatalf("bound object[propertyKey] = %#v, error = %v", value, err)
	}
	if toStringCalls != 2 {
		t.Fatalf("bound property key toString calls = %d, want 2", toStringCalls)
	}

	plain := map[string]any{}
	if value, err := vm.call(conversationTurnstileObjectRef{path: "window.Reflect.set"}, []any{plain, key, "set"}); err != nil || value != true {
		t.Fatalf("Reflect.set() = %#v, error = %v", value, err)
	}
	if plain["selected"] != "set" || toStringCalls != 3 {
		t.Fatalf("Reflect.set property = %#v, toString calls = %d", plain, toStringCalls)
	}
}

func TestConversationTurnstileSubroutineCatchesPropertyTypeError(t *testing.T) {
	requirementsToken := "requirements"
	dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{30, 40, 50, []any{
			[]any{2, 60, nil},
			[]any{2, 61, "x"},
			[]any{6, 62, 60, 61},
		}},
		[]any{17, 41, 40},
		[]any{7, 3, 41},
	})
	token, err := BuildConversationTurnstileToken(context.Background(), dx, requirementsToken, zeroReader{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if want := base64.StdEncoding.EncodeToString([]byte("TypeError: Cannot read properties of null (reading 'x')")); token != want {
		t.Fatalf("caught token = %q, want %q", token, want)
	}
}

func TestConversationTurnstileNestedQueueAndSubroutineResults(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		values:          make(map[string]any),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 100},
	}
	vm.set(40, "keep")
	if _, err := vm.opNestedQueue([]any{40, []any{}}); err != nil {
		t.Fatalf("opNestedQueue() error = %v", err)
	}
	if got := vm.get(40); got != "keep" {
		t.Fatalf("nested queue result = %#v", got)
	}
	vm.set(60, newConversationTurnstileCallable(func([]any) (any, error) { return nil, errors.New("nested failure") }))
	if _, err := vm.opNestedQueue([]any{61, []any{[]any{60}}}); err != nil {
		t.Fatalf("opNestedQueue(failure) error = %v", err)
	}
	if got := vm.get(61); got != "Error: nested failure" {
		t.Fatalf("nested queue caught error = %#v", got)
	}
	vm.set(50, 42)
	if _, err := vm.opSubroutine([]any{41, 50, []any{}}); err != nil {
		t.Fatalf("opSubroutine() error = %v", err)
	}
	value, err := vm.call(vm.get(41), nil)
	if err != nil || value != 42 {
		t.Fatalf("subroutine result = %#v, %v", value, err)
	}
}

func TestBuildConversationTurnstileTokenSubroutineCaptureModes(t *testing.T) {
	requirementsToken := "requirements-token"
	dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{2, 60, "outer"},
		[]any{2, 61, "captured"},
		[]any{30, 40, 50, []any{[]any{8, 50, 60}}},
		[]any{17, 41, 40, 61},
		[]any{30, 42, 52, []any{62}, []any{[]any{8, 52, 62}}},
		[]any{17, 43, 42, 61},
		[]any{5, 41, 43},
		[]any{7, 3, 41},
	})
	token, err := BuildConversationTurnstileToken(
		context.Background(), dx, requirementsToken, zeroReader{}, time.Now,
	)
	if err != nil {
		t.Fatalf("BuildConversationTurnstileToken() error = %v", err)
	}
	want := base64.StdEncoding.EncodeToString([]byte("outercaptured"))
	if token != want {
		t.Fatalf("token = %q, want %q", token, want)
	}
}

func TestBuildConversationTurnstileTokenMatchesSentinelSDKFixtures(t *testing.T) {
	requirementsToken := "fixture-requirements-token"
	persona := DefaultPersona()
	environment := ConversationTurnstileEnvironment{
		Persona: persona,
		ScriptSources: []string{
			"https://sentinel.openai.com/backend-api/sentinel/sdk.js",
			"https://sentinel.openai.com/sentinel/20260219f9f6/sdk.js",
		},
	}
	tests := []struct {
		name        string
		program     []any
		environment ConversationTurnstileEnvironment
		want        string
	}{
		{
			name: "process map stringify",
			program: []any{
				[]any{12, 40},
				[]any{15, 41, 40},
				[]any{7, 3, 41},
			},
			want: "e30=",
		},
		{
			name: "ordered object stringify",
			program: []any{
				[]any{2, 40, "Object"},
				[]any{6, 41, 10, 40},
				[]any{2, 42, "create"},
				[]any{24, 43, 41, 42},
				[]any{2, 54, nil},
				[]any{17, 44, 43, 54},
				[]any{2, 45, "Reflect"},
				[]any{6, 46, 10, 45},
				[]any{2, 47, "set"},
				[]any{24, 48, 46, 47},
				[]any{2, 49, "b"},
				[]any{2, 50, 1},
				[]any{7, 48, 44, 49, 50},
				[]any{2, 51, "a"},
				[]any{2, 52, 2},
				[]any{7, 48, 44, 51, 52},
				[]any{15, 53, 44},
				[]any{7, 3, 53},
			},
			want: "eyJiIjoxLCJhIjoyfQ==",
		},
		{
			name: "parsed object order",
			program: []any{
				[]any{2, 40, `{"b":1,"a":2}`},
				[]any{14, 41, 40},
				[]any{15, 42, 41},
				[]any{7, 3, 42},
			},
			want: "eyJiIjoxLCJhIjoyfQ==",
		},
		{
			name: "navigator persona",
			program: []any{
				[]any{2, 40, "navigator"},
				[]any{6, 41, 10, 40},
				[]any{2, 42, "userAgent"},
				[]any{6, 43, 41, 42},
				[]any{7, 3, 43},
			},
			environment: environment,
			want:        base64.StdEncoding.EncodeToString([]byte(persona.UserAgent)),
		},
		{
			name: "document location object",
			program: []any{
				[]any{2, 40, "document"},
				[]any{6, 41, 10, 40},
				[]any{2, 42, "location"},
				[]any{6, 43, 41, 42},
				[]any{2, 44, "href"},
				[]any{6, 45, 43, 44},
				[]any{7, 3, 45},
			},
			environment: ConversationTurnstileEnvironment{Location: "https://chatgpt.com/backend-api/sentinel/frame.html?flow=login"},
			want:        base64.StdEncoding.EncodeToString([]byte("https://chatgpt.com/backend-api/sentinel/frame.html?flow=login")),
		},
		{
			name: "script source lookup",
			program: []any{
				[]any{2, 40, "sentinel/20260219"},
				[]any{11, 41, 40},
				[]any{7, 3, 41},
			},
			environment: environment,
			want:        "c2VudGluZWwvMjAyNjAyMTk=",
		},
		{
			name: "custom local storage keys",
			program: []any{
				[]any{2, 40, "Object"},
				[]any{6, 41, 10, 40},
				[]any{2, 42, "keys"},
				[]any{24, 43, 41, 42},
				[]any{2, 44, "localStorage"},
				[]any{6, 45, 10, 44},
				[]any{17, 46, 43, 45},
				[]any{15, 47, 46},
				[]any{7, 3, 47},
			},
			environment: ConversationTurnstileEnvironment{LocalStorageKeys: []string{"custom"}},
			want:        "WyJjdXN0b20iXQ==",
		},
		{
			name: "division",
			program: []any{
				[]any{2, 40, 6},
				[]any{2, 41, 3},
				[]any{35, 42, 40, 41},
				[]any{7, 3, 42},
			},
			want: "Mg==",
		},
		{
			name: "numeric addition",
			program: []any{
				[]any{2, 40, 1},
				[]any{2, 41, 2},
				[]any{5, 40, 41},
				[]any{7, 3, 40},
			},
			want: "Mw==",
		},
		{
			name: "boolean addition",
			program: []any{
				[]any{2, 40, true},
				[]any{2, 41, 2},
				[]any{5, 40, 41},
				[]any{7, 3, 40},
			},
			want: "Mw==",
		},
		{
			name: "null addition",
			program: []any{
				[]any{2, 40, nil},
				[]any{2, 41, 2},
				[]any{5, 40, 41},
				[]any{7, 3, 40},
			},
			want: "Mg==",
		},
		{
			name: "object addition",
			program: []any{
				[]any{2, 40, "Object"},
				[]any{6, 41, 10, 40},
				[]any{2, 42, "create"},
				[]any{24, 43, 41, 42},
				[]any{2, 44, nil},
				[]any{17, 45, 43, 44},
				[]any{2, 46, 1},
				[]any{5, 45, 46},
				[]any{7, 3, 45},
			},
			want: base64.StdEncoding.EncodeToString([]byte("[object Object]1")),
		},
		{
			name: "strict numeric equality",
			program: []any{
				[]any{2, 40, 1},
				[]any{2, 41, 1},
				[]any{2, 42, 1},
				[]any{33, 43, 41, 42},
				[]any{20, 40, 43, 3, "equal"},
			},
			want: base64.StdEncoding.EncodeToString([]byte("equal")),
		},
		{
			name: "strict object identity",
			program: []any{
				[]any{2, 47, "distinct"},
				[]any{7, 3, 47},
				[]any{2, 40, "Object"},
				[]any{6, 41, 10, 40},
				[]any{2, 42, "create"},
				[]any{24, 43, 41, 42},
				[]any{2, 44, nil},
				[]any{17, 45, 43, 44},
				[]any{17, 46, 43, 44},
				[]any{20, 45, 46, 3, "wrong"},
			},
			want: base64.StdEncoding.EncodeToString([]byte("distinct")),
		},
	}

	// Expected tokens were captured from Sentinel SDK 20260219f9f6 using the
	// same compact programs. Normal tests do not execute JavaScript or Node.
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dx := encodeConversationTurnstileProgram(t, requirementsToken, test.program)
			token, err := BuildConversationTurnstileTokenWithEnvironment(
				context.Background(), dx, requirementsToken, test.environment, zeroReader{},
				func() time.Time { return time.Unix(1_700_000_000, 0) },
			)
			if err != nil {
				t.Fatalf("BuildConversationTurnstileTokenWithEnvironment() error = %v", err)
			}
			if token != test.want {
				t.Fatalf("token = %q, want %q", token, test.want)
			}
		})
	}
}

func TestNormalizeConversationTurnstileEnvironmentSplitsLocation(t *testing.T) {
	values, _, _ := normalizeConversationTurnstileEnvironment(ConversationTurnstileEnvironment{
		Location: "https://chatgpt.com/backend-api/sentinel/frame.html?flow=login",
	}, time.Unix(1_700_000_000, 0))
	if values["window.location.origin"] != "https://chatgpt.com" {
		t.Fatalf("location origin = %#v", values["window.location.origin"])
	}
	if values["window.location.pathname"] != "/backend-api/sentinel/frame.html" {
		t.Fatalf("location pathname = %#v", values["window.location.pathname"])
	}
	if values["window.location.search"] != "?flow=login" {
		t.Fatalf("location search = %#v", values["window.location.search"])
	}
	location, ok := values["window.document.location"].(*conversationTurnstileLocationRef)
	if !ok || location.href != "https://chatgpt.com/backend-api/sentinel/frame.html?flow=login" {
		t.Fatalf("document location = %#v", values["window.document.location"])
	}
	if values["window.location"] != values["window.document.location"] {
		t.Fatal("window.location and document.location do not share identity")
	}
}

func TestConversationTurnstileStringOperationsUseUTF16Units(t *testing.T) {
	value := "A😀B"
	if got := len(conversationTurnstileUTF16Units(value)); got != 4 {
		t.Fatalf("UTF-16 length = %d, want 4", got)
	}
	if got := conversationTurnstileUTF16Index(value, "B"); got != 3 {
		t.Fatalf("UTF-16 index = %d, want 3", got)
	}
	vm := &conversationTurnstileVM{memoryBudget: &conversationTurnstileMemoryBudget{}}
	bound, err := vm.bindProperty("😀", "charCodeAt")
	if err != nil {
		t.Fatal(err)
	}
	charCodeAt, ok := bound.(conversationTurnstileCallable)
	if !ok {
		t.Fatal("charCodeAt binding is missing")
	}
	high, err := charCodeAt.invoke([]any{0})
	if err != nil || high != int(0xd83d) {
		t.Fatalf("charCodeAt(0) = %#v, %v", high, err)
	}
	low, err := charCodeAt.invoke([]any{1})
	if err != nil || low != int(0xde00) {
		t.Fatalf("charCodeAt(1) = %#v, %v", low, err)
	}
	fromCharCode, err := vm.call(conversationTurnstileObjectRef{path: "window.String.fromCharCode"}, []any{0xd83d, 0xde00})
	if err != nil || conversationTurnstileString(fromCharCode) != "😀" {
		t.Fatalf("fromCharCode() = %#v, %v", fromCharCode, err)
	}
	isolated, err := vm.call(conversationTurnstileObjectRef{path: "window.String.fromCharCode"}, []any{0xd800})
	if err != nil {
		t.Fatalf("fromCharCode(isolated surrogate) error = %v", err)
	}
	bound, err = vm.bindProperty(isolated, "charCodeAt")
	if err != nil {
		t.Fatal(err)
	}
	isolatedCharCodeAt, ok := bound.(conversationTurnstileCallable)
	if !ok {
		t.Fatal("isolated surrogate charCodeAt binding is missing")
	}
	unit, err := isolatedCharCodeAt.invoke([]any{0})
	if err != nil || unit != int(0xd800) {
		t.Fatalf("isolated surrogate charCodeAt(0) = %#v, %v", unit, err)
	}
}

func TestConversationTurnstileCharCodeAtUsesToIntegerOrInfinity(t *testing.T) {
	vm := &conversationTurnstileVM{memoryBudget: &conversationTurnstileMemoryBudget{}}
	bound, err := vm.bindProperty("AB", "charCodeAt")
	if err != nil {
		t.Fatal(err)
	}
	charCodeAt := bound.(conversationTurnstileCallable)
	for _, test := range []struct {
		name    string
		value   any
		want    int
		wantNaN bool
	}{
		{name: "undefined", value: conversationTurnstileUndefined, want: int('A')},
		{name: "NaN", value: math.NaN(), want: int('A')},
		{name: "negative zero", value: math.Copysign(0, -1), want: int('A')},
		{name: "negative fraction", value: -0.9, want: int('A')},
		{name: "positive fraction", value: 1.9, want: int('B')},
		{name: "negative index", value: -1.9, wantNaN: true},
		{name: "positive infinity", value: math.Inf(1), wantNaN: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := charCodeAt.invoke([]any{test.value})
			if err != nil {
				t.Fatalf("charCodeAt() error = %v", err)
			}
			if test.wantNaN {
				if number, ok := got.(float64); !ok || !math.IsNaN(number) {
					t.Fatalf("charCodeAt() = %#v, want NaN", got)
				}
				return
			}
			if got != test.want {
				t.Fatalf("charCodeAt() = %#v, want %d", got, test.want)
			}
		})
	}
}

func TestConversationTurnstileJSStringPreservesSurrogatesAcrossJSONAndMapKeys(t *testing.T) {
	isolated := conversationTurnstileJSString{units: []uint16{0xd800}}
	payload, err := marshalConversationTurnstileJSON(isolated)
	if err != nil {
		t.Fatalf("marshalConversationTurnstileJSON() error = %v", err)
	}
	if string(payload) != `"\ud800"` {
		t.Fatalf("isolated surrogate JSON = %q", payload)
	}
	vm := &conversationTurnstileVM{values: make(map[string]any)}
	vm.set(isolated, "isolated")
	if got := vm.get(isolated); got != "isolated" {
		t.Fatalf("isolated surrogate map value = %#v", got)
	}
	if got := vm.get(conversationTurnstileJSString{units: []uint16{0xfffd}}); !isConversationTurnstileUndefined(got) {
		t.Fatalf("replacement character collided with isolated surrogate: %#v", got)
	}
	if conversationTurnstileMapKey("😀") != conversationTurnstileMapKey(conversationTurnstileJSString{units: []uint16{0xd83d, 0xde00}}) {
		t.Fatal("equivalent UTF-8 and UTF-16 strings produced different map keys")
	}
}

func TestDecodeConversationTurnstileJSONPreservesIsolatedSurrogates(t *testing.T) {
	decoded, err := decodeConversationTurnstileJSON([]byte(`{"\ud800":"\udfff"}`))
	if err != nil {
		t.Fatalf("decodeConversationTurnstileJSON() error = %v", err)
	}
	object, ok := decoded.(*conversationTurnstileOrderedMap)
	if !ok || len(object.keys) != 1 {
		t.Fatalf("decoded object = %#v", decoded)
	}
	key, ok := object.keys[0].(conversationTurnstileJSString)
	if !ok || !reflect.DeepEqual(key.units, []uint16{0xd800}) {
		t.Fatalf("decoded key = %#v", object.keys[0])
	}
	value, exists := object.get(conversationTurnstileJSString{units: []uint16{0xd800}})
	stringValue, ok := value.(conversationTurnstileJSString)
	if !exists || !ok || !reflect.DeepEqual(stringValue.units, []uint16{0xdfff}) {
		t.Fatalf("decoded value = %#v, exists %t", value, exists)
	}
	payload, err := marshalConversationTurnstileJSON(object)
	if err != nil || string(payload) != `{"\ud800":"\udfff"}` {
		t.Fatalf("round-trip JSON = %q, %v", payload, err)
	}
}

func TestConversationTurnstileReflectSetPreservesIsolatedSurrogateKey(t *testing.T) {
	vm := &conversationTurnstileVM{memoryBudget: &conversationTurnstileMemoryBudget{}}
	object := newConversationTurnstileOrderedMap()
	if _, err := vm.call(conversationTurnstileObjectRef{path: "window.Reflect.set"}, []any{object, conversationTurnstileJSString{units: []uint16{0xd800}}, "value"}); err != nil {
		t.Fatalf("Reflect.set() error = %v", err)
	}
	payload, err := marshalConversationTurnstileJSON(object)
	if err != nil || string(payload) != `{"\ud800":"value"}` {
		t.Fatalf("Reflect.set JSON = %q, %v", payload, err)
	}
}

func TestConversationTurnstileProcessMapUsesObjectIdentity(t *testing.T) {
	vm := &conversationTurnstileVM{values: make(map[string]any), memoryBudget: &conversationTurnstileMemoryBudget{}}
	firstObject := newConversationTurnstileOrderedMap()
	secondObject := newConversationTurnstileOrderedMap()
	vm.set(firstObject, "first-object")
	vm.set(secondObject, "second-object")
	firstArray := make([]any, 0, 1)
	secondArray := make([]any, 0, 1)
	vm.set(firstArray, "first-array")
	vm.set(secondArray, "second-array")
	vm.set(math.Copysign(0, -1), "zero")
	if vm.fatalErr != nil {
		t.Fatalf("map setup error = %v", vm.fatalErr)
	}
	for name, test := range map[string]struct {
		key  any
		want string
	}{
		"first object":  {key: firstObject, want: "first-object"},
		"second object": {key: secondObject, want: "second-object"},
		"first array":   {key: firstArray, want: "first-array"},
		"second array":  {key: secondArray, want: "second-array"},
		"positive zero": {key: float64(0), want: "zero"},
	} {
		if got := vm.get(test.key); got != test.want {
			t.Fatalf("%s = %#v, want %q", name, got, test.want)
		}
	}
}

func TestConversationTurnstileProcessMapOpcodeReturnsStableMap(t *testing.T) {
	vm := &conversationTurnstileVM{values: make(map[string]any), memoryBudget: &conversationTurnstileMemoryBudget{}}
	if _, err := vm.opProcessMap([]any{40}); err != nil {
		t.Fatal(err)
	}
	if _, err := vm.opProcessMap([]any{41}); err != nil {
		t.Fatal(err)
	}
	first, firstOK := vm.get(40).(*conversationTurnstileProcessMapRef)
	second, secondOK := vm.get(41).(*conversationTurnstileProcessMapRef)
	if !firstOK || !secondOK || first != second {
		t.Fatalf("process maps = %#v and %#v", vm.get(40), vm.get(41))
	}
	if got := first.method("size"); got != len(vm.values) {
		t.Fatalf("process map size = %#v, want %d", got, len(vm.values))
	}
	set := first.method("set").(conversationTurnstileCallable)
	if _, err := set.invoke(nil); err != nil {
		t.Fatal(err)
	}
	has := first.method("has").(conversationTurnstileCallable)
	if got, err := has.invoke(nil); err != nil || got != true {
		t.Fatalf("process map has(undefined) = %#v, %v", got, err)
	}
	get := first.method("get").(conversationTurnstileCallable)
	if got, err := get.invoke(nil); err != nil || !isConversationTurnstileUndefined(got) {
		t.Fatalf("process map get(undefined) = %#v, %v", got, err)
	}
	remove := first.method("delete").(conversationTurnstileCallable)
	if got, err := remove.invoke(nil); err != nil || got != true {
		t.Fatalf("process map delete(undefined) = %#v, %v", got, err)
	}
}

func TestConversationTurnstileStringWorkIsBounded(t *testing.T) {
	vm := &conversationTurnstileVM{memoryBudget: &conversationTurnstileMemoryBudget{}}
	value := strings.Repeat("a", 1<<20)
	var err error
	for err == nil {
		_, err = vm.property(value, "length")
	}
	if !strings.Contains(err.Error(), "runtime work exceeds") {
		t.Fatalf("runtime work error = %v", err)
	}
}

func TestConversationTurnstilePropertyMissingIsUndefined(t *testing.T) {
	vm := &conversationTurnstileVM{}
	ordered := newConversationTurnstileOrderedMap()
	ordered.set("null", nil)
	plain := map[string]any{"null": nil}
	for name, object := range map[string]any{"ordered": ordered, "plain": plain} {
		if got, err := vm.property(object, "missing"); err != nil || !isConversationTurnstileUndefined(got) {
			t.Fatalf("%s missing property = %#v", name, got)
		}
		if got, err := vm.property(object, "null"); err != nil || got != nil {
			t.Fatalf("%s null property = %#v", name, got)
		}
	}
	if got, err := vm.property("ordinary", "missing"); err != nil || !isConversationTurnstileUndefined(got) {
		t.Fatalf("string missing property = %#v", got)
	}
	if got, err := vm.property("😀", "1"); err != nil || !reflect.DeepEqual(got, conversationTurnstileJSString{units: []uint16{0xde00}}) {
		t.Fatalf("string UTF-16 index = %#v", got)
	}
	if got := conversationTurnstileString("window.Object"); got != "window.Object" {
		t.Fatalf("ordinary string conversion = %q", got)
	}
	if _, err := vm.call("window.Object.keys", []any{map[string]any{"key": true}}); err == nil {
		t.Fatal("ordinary string was treated as a callable object reference")
	}
	if got, err := vm.property("abc", "01"); err != nil || !isConversationTurnstileUndefined(got) {
		t.Fatalf("non-canonical string index = %#v", got)
	}
}

func TestConversationTurnstileStringBindingDefersConversion(t *testing.T) {
	vm := &conversationTurnstileVM{memoryBudget: &conversationTurnstileMemoryBudget{}}
	value := conversationTurnstileJSString{units: make([]uint16, 1<<20)}
	bound, err := vm.bindProperty(value, "charCodeAt")
	if err != nil {
		t.Fatal(err)
	}
	if vm.memoryBudget.used != len("charCodeAt") {
		t.Fatalf("binding allocated %d bytes, want only the property name", vm.memoryBudget.used)
	}
	callable, ok := bound.(conversationTurnstileCallable)
	if !ok {
		t.Fatal("charCodeAt binding is missing")
	}
	if _, err := callable.invoke([]any{0}); err != nil {
		t.Fatalf("charCodeAt invocation error = %v", err)
	}
}

func TestConversationTurnstileUTF16ScansConsumeRuntimeWork(t *testing.T) {
	value := conversationTurnstileJSString{units: make([]uint16, 1<<20)}
	search := conversationTurnstileJSString{units: []uint16{1}}
	for _, test := range []struct {
		name string
		run  func(*conversationTurnstileVM) error
	}{
		{
			name: "indexOf",
			run: func(vm *conversationTurnstileVM) error {
				_, err := vm.utf16Index(value, search, 0)
				return err
			},
		},
		{
			name: "lessThan",
			run: func(vm *conversationTurnstileVM) error {
				_, err := vm.lessThan(value, search)
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			vm := &conversationTurnstileVM{
				memoryBudget:    &conversationTurnstileMemoryBudget{},
				executionBudget: &conversationTurnstileExecutionBudget{runtimeWork: conversationTurnstileMaxRuntimeWork - 1024},
			}
			err := test.run(vm)
			if !strings.Contains(err.Error(), "runtime work exceeds") {
				t.Fatalf("scan error = %v", err)
			}
		})
	}
}

func TestConversationTurnstileStringConversionsConsumeRuntimeWork(t *testing.T) {
	large := conversationTurnstileJSString{units: make([]uint16, 1<<20)}
	for _, test := range []struct {
		name string
		run  func(*conversationTurnstileVM) error
	}{
		{
			name: "property path",
			run: func(vm *conversationTurnstileVM) error {
				_, err := vm.property(conversationTurnstileObjectRef{path: strings.Repeat("x", 1<<20)}, "field")
				return err
			},
		},
		{
			name: "parse float",
			run: func(vm *conversationTurnstileVM) error {
				_, _, err := vm.number(large)
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			vm := &conversationTurnstileVM{
				memoryBudget:    &conversationTurnstileMemoryBudget{},
				executionBudget: &conversationTurnstileExecutionBudget{runtimeWork: conversationTurnstileMaxRuntimeWork - 1024},
			}
			err := test.run(vm)
			if !strings.Contains(err.Error(), "runtime work exceeds") {
				t.Fatalf("conversion error = %v", err)
			}
		})
	}
}

func TestConversationTurnstileCompositeNumberConversionsShareRuntimeWork(t *testing.T) {
	const itemCount = 4096
	items := make([]any, itemCount)
	for index := range items {
		items[index] = "1234567890"
	}
	largeArray := conversationTurnstileArrayValue(items)
	convertedLength := itemCount*10 + itemCount - 1
	conversionWork := convertedLength * 2
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{runtimeWork: conversationTurnstileMaxRuntimeWork - conversionWork - conversionWork/2},
	}
	if _, _, err := vm.number(largeArray); err != nil {
		t.Fatalf("first array conversion error = %v", err)
	}
	if _, _, err := vm.number(largeArray); err == nil || !strings.Contains(err.Error(), "runtime work exceeds") {
		t.Fatalf("second array conversion error = %v, want shared runtime work limit", err)
	}

	objectVM := &conversationTurnstileVM{
		ctx:             context.Background(),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{runtimeWork: conversationTurnstileMaxRuntimeWork - 1},
	}
	if _, _, err := objectVM.number(newConversationTurnstileOrderedMap()); err == nil || !strings.Contains(err.Error(), "runtime work exceeds") {
		t.Fatalf("object conversion error = %v, want runtime work limit", err)
	}
}

func TestConversationTurnstileCompositeNumberConversionObservesCancellation(t *testing.T) {
	items := make([]any, 4096)
	for index := range items {
		items[index] = index
	}
	vm := &conversationTurnstileVM{
		ctx:             newConversationTurnstileCancelAfterContext(8),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{},
	}
	if _, _, err := vm.number(conversationTurnstileArrayValue(items)); !errors.Is(err, context.Canceled) {
		t.Fatalf("array conversion error = %v, want context.Canceled", err)
	}
}

func TestConversationTurnstileCompositeNumberSemantics(t *testing.T) {
	vm := &conversationTurnstileVM{ctx: context.Background(), memoryBudget: &conversationTurnstileMemoryBudget{}}
	tests := []struct {
		name   string
		value  any
		want   float64
		wantOK bool
	}{
		{name: "empty array", value: conversationTurnstileArrayValue(nil), want: 0, wantOK: true},
		{name: "single value array", value: conversationTurnstileArrayValue([]any{"42"}), want: 42, wantOK: true},
		{name: "multiple value array", value: conversationTurnstileArrayValue([]any{1, 2})},
		{name: "object", value: newConversationTurnstileOrderedMap()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok, err := vm.number(test.value)
			if err != nil {
				t.Fatalf("number() error = %v", err)
			}
			if ok != test.wantOK {
				t.Fatalf("number() ok = %t, want %t", ok, test.wantOK)
			}
			if got != test.want {
				t.Fatalf("number() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestConversationTurnstileRegexpMatchingUsesBoundedCancelableWork(t *testing.T) {
	matcher, err := regexp.Compile(strings.Repeat("a?", 1024) + "z")
	if err != nil {
		t.Fatal(err)
	}
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{},
	}
	if _, err = vm.regexpMatchIndices(matcher, strings.Repeat("a", 64<<10), false); err == nil || !strings.Contains(err.Error(), "runtime work exceeds") {
		t.Fatalf("regexp work error = %v", err)
	}

	vm = &conversationTurnstileVM{
		ctx:             newConversationTurnstileCancelAfterContext(2),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{},
	}
	matcher = regexp.MustCompile(`z$`)
	if _, err = vm.regexpMatchIndices(matcher, strings.Repeat("a", 4096), false); !errors.Is(err, context.Canceled) {
		t.Fatalf("regexp cancellation error = %v, want context.Canceled", err)
	}
}

func TestConversationTurnstileJSONStringifyBoundsDepthAndObservesCancellation(t *testing.T) {
	var nested any = "leaf"
	for range conversationTurnstileMaxJSONDepth + 1 {
		nested = conversationTurnstileArrayValue([]any{nested})
	}
	vm := &conversationTurnstileVM{
		ctx:          context.Background(),
		values:       make(map[string]any),
		memoryBudget: &conversationTurnstileMemoryBudget{},
	}
	vm.set(40, nested)
	if _, err := vm.opJSONStringify([]any{41, 40}); err == nil || !strings.Contains(err.Error(), "JSON exceeds depth") {
		t.Fatalf("JSON depth error = %v", err)
	}

	wide := make([]any, 4096)
	for index := range wide {
		wide[index] = float64(index)
	}
	vm = &conversationTurnstileVM{
		ctx:          context.Background(),
		values:       make(map[string]any),
		memoryBudget: &conversationTurnstileMemoryBudget{},
	}
	vm.set(40, conversationTurnstileArrayValue(wide))
	vm.ctx = newConversationTurnstileCancelAfterContext(16)
	if _, err := vm.opJSONStringify([]any{41, 40}); !errors.Is(err, context.Canceled) {
		t.Fatalf("JSON cancellation error = %v, want context.Canceled", err)
	}
}

func TestConversationTurnstileLimitedBufferReservesBeforeWrite(t *testing.T) {
	wantErr := errors.New("allocation rejected")
	buffer := &conversationTurnstileLimitedBuffer{
		max: 1024,
		reserve: func(int) error {
			return wantErr
		},
	}
	if _, err := buffer.WriteString(strings.Repeat("x", 512)); !errors.Is(err, wantErr) {
		t.Fatalf("WriteString() error = %v, want %v", err, wantErr)
	}
	if buffer.Len() != 0 {
		t.Fatalf("buffer length = %d after rejected reservation", buffer.Len())
	}
}

func TestConversationTurnstileUTF16UTF8Length(t *testing.T) {
	for _, test := range []struct {
		units []uint16
		want  int
	}{
		{units: []uint16{'a'}, want: 1},
		{units: []uint16{0x07ff}, want: 2},
		{units: []uint16{0x0800}, want: 3},
		{units: []uint16{0xd83d, 0xde00}, want: 4},
		{units: []uint16{0xd83d}, want: 3},
	} {
		if got := conversationTurnstileUTF16UTF8Length(test.units); got != test.want {
			t.Fatalf("UTF-8 length for %v = %d, want %d", test.units, got, test.want)
		}
	}
}

func TestConversationTurnstileArithmeticUsesJavaScriptInvalidNumberSemantics(t *testing.T) {
	newVM := func() *conversationTurnstileVM {
		return &conversationTurnstileVM{ctx: context.Background(), values: make(map[string]any), memoryBudget: &conversationTurnstileMemoryBudget{}}
	}
	for _, test := range []struct {
		name string
		run  func(*conversationTurnstileVM) error
	}{
		{
			name: "invalid multiply",
			run: func(vm *conversationTurnstileVM) error {
				vm.set(40, conversationTurnstileUndefined)
				vm.set(41, float64(2))
				_, err := vm.opMultiply([]any{42, 40, 41})
				return err
			},
		},
		{
			name: "invalid subtract",
			run: func(vm *conversationTurnstileVM) error {
				vm.set(40, conversationTurnstileUndefined)
				vm.set(41, float64(2))
				_, err := vm.opRemove([]any{40, 41})
				vm.set(42, vm.get(40))
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			vm := newVM()
			if err := test.run(vm); err != nil {
				t.Fatal(err)
			}
			if value, ok := vm.get(42).(float64); !ok || !math.IsNaN(value) {
				t.Fatalf("result = %#v, want NaN", vm.get(42))
			}
		})
	}

	for _, test := range []struct {
		name  string
		left  float64
		right float64
		want  float64
	}{
		{name: "positive divide by zero", left: 1, right: 0, want: 0},
		{name: "negative divide by zero", left: -1, right: 0, want: 0},
		{name: "zero divide by zero", left: 0, right: 0, want: 0},
		{name: "ordinary division", left: 6, right: 2, want: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			vm := newVM()
			vm.set(40, test.left)
			vm.set(41, test.right)
			if _, err := vm.opDivide([]any{42, 40, 41}); err != nil {
				t.Fatal(err)
			}
			value, ok := vm.get(42).(float64)
			if !ok || value != test.want {
				t.Fatalf("division result = %#v, want %v", vm.get(42), test.want)
			}
		})
	}
}

func TestConversationTurnstileBase64UsesBrowserBinaryStrings(t *testing.T) {
	vm := &conversationTurnstileVM{values: make(map[string]any), memoryBudget: &conversationTurnstileMemoryBudget{}}
	vm.set(40, " /w ")
	if _, err := vm.opBase64Decode([]any{40}); err != nil {
		t.Fatalf("atob error = %v", err)
	}
	decoded, ok := vm.get(40).(conversationTurnstileJSString)
	if !ok || !reflect.DeepEqual(decoded.units, []uint16{0xff}) {
		t.Fatalf("atob value = %#v", vm.get(40))
	}
	if _, err := vm.opBase64Encode([]any{40}); err != nil {
		t.Fatalf("btoa error = %v", err)
	}
	if got := vm.get(40); got != "/w==" {
		t.Fatalf("btoa value = %#v", got)
	}
	if _, err := vm.base64EncodeBinaryString(conversationTurnstileJSString{units: []uint16{0x100}}); err == nil {
		t.Fatal("btoa accepted a code unit above 255")
	}
	if _, err := vm.opResult([]any{conversationTurnstileJSString{units: []uint16{0xff}}}); err != nil || vm.result != "/w==" {
		t.Fatalf("result = %q, %v", vm.result, err)
	}
}

func TestConversationTurnstileArrayMutationPreservesIdentity(t *testing.T) {
	vm := &conversationTurnstileVM{values: make(map[string]any), memoryBudget: &conversationTurnstileMemoryBudget{}}
	array := conversationTurnstileArrayValue([]any{"first"})
	vm.set(40, array)
	vm.set(41, array)
	vm.set(42, "second")
	vm.set(array, "mapped")
	if _, err := vm.opAppend([]any{40, 42}); err != nil {
		t.Fatalf("append error = %v", err)
	}
	alias, ok := vm.get(41).(*conversationTurnstileArray)
	if !ok || alias != array || !reflect.DeepEqual(alias.items, []any{"first", "second"}) {
		t.Fatalf("append alias = %#v", vm.get(41))
	}
	if got := vm.get(array); got != "mapped" {
		t.Fatalf("map identity after append = %#v", got)
	}
	vm.set(43, "missing")
	if _, err := vm.opRemove([]any{40, 43}); err != nil {
		t.Fatalf("remove missing error = %v", err)
	}
	if !reflect.DeepEqual(array.items, []any{"first"}) {
		t.Fatalf("remove missing = %#v, want SDK splice(-1, 1) behavior", array.items)
	}
	vm.set(42, "first")
	if _, err := vm.opRemove([]any{40, 42}); err != nil {
		t.Fatalf("remove error = %v", err)
	}
	if len(alias.items) != 0 {
		t.Fatalf("remove alias = %#v", alias.items)
	}
}

func TestConversationTurnstileArrayRemoveScanConsumesRuntimeWork(t *testing.T) {
	items := make([]any, 2048)
	for index := range items {
		items[index] = float64(index)
	}
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		values:          make(map[string]any),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{runtimeWork: conversationTurnstileMaxRuntimeWork - 1024},
	}
	vm.set(40, conversationTurnstileArrayValue(items))
	vm.set(41, float64(-1))
	_, err := vm.opRemove([]any{40, 41})
	if err == nil || !strings.Contains(err.Error(), "runtime work exceeds") {
		t.Fatalf("remove scan error = %v, want runtime work limit", err)
	}
}

func TestConversationTurnstileArrayRemoveScanObservesCancellation(t *testing.T) {
	items := make([]any, 2048)
	for index := range items {
		items[index] = float64(index)
	}
	vm := &conversationTurnstileVM{
		ctx:          context.Background(),
		values:       make(map[string]any),
		memoryBudget: &conversationTurnstileMemoryBudget{},
	}
	vm.set(40, conversationTurnstileArrayValue(items))
	vm.set(41, float64(-1))
	vm.ctx = newConversationTurnstileCancelAfterContext(3)
	if _, err := vm.opRemove([]any{40, 41}); !errors.Is(err, context.Canceled) {
		t.Fatalf("remove scan error = %v, want context.Canceled", err)
	}
}

func TestConversationTurnstileArrayRemoveChargesTailMove(t *testing.T) {
	items := make([]any, 2048)
	for index := range items {
		items[index] = float64(index)
	}
	array := conversationTurnstileArrayValue(items)
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		values:          make(map[string]any),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{runtimeWork: conversationTurnstileMaxRuntimeWork - 1024},
	}
	vm.set(40, array)
	vm.set(41, float64(0))
	_, err := vm.opRemove([]any{40, 41})
	if err == nil || !strings.Contains(err.Error(), "runtime work exceeds") {
		t.Fatalf("head removal error = %v, want runtime work limit", err)
	}
	if len(array.items) != len(items) || array.items[0] != float64(0) {
		t.Fatalf("failed removal mutated array: len=%d first=%#v", len(array.items), array.items[0])
	}
}

func TestConversationTurnstileRejectDoesNotExposeVMValues(t *testing.T) {
	const secret = "requirements-token-secret"
	_, err := (&conversationTurnstileVM{}).opReject([]any{secret})
	if err == nil || strings.Contains(err.Error(), secret) || err.Error() != "conversation turnstile challenge rejected" {
		t.Fatalf("opReject() error = %v", err)
	}
}

func TestConversationTurnstileJSONNumbersUseJavaScriptNumberSemantics(t *testing.T) {
	decoded, err := decodeConversationTurnstileJSON([]byte(`{"n":1e3,"negative_zero":-0,"large":9007199254740993,"overflow":1e400}`))
	if err != nil {
		t.Fatalf("decode error = %v", err)
	}
	object := decoded.(*conversationTurnstileOrderedMap)
	negativeZero, _ := object.get("negative_zero")
	if value, ok := negativeZero.(float64); !ok || !math.Signbit(value) {
		t.Fatalf("negative zero = %#v", negativeZero)
	}
	large, _ := object.get("large")
	if large != float64(9007199254740992) {
		t.Fatalf("large number = %#v", large)
	}
	overflow, _ := object.get("overflow")
	if value, ok := overflow.(float64); !ok || !math.IsInf(value, 1) {
		t.Fatalf("overflow = %#v", overflow)
	}
	payload, err := marshalConversationTurnstileJSON(object)
	if err != nil {
		t.Fatalf("marshal error = %v", err)
	}
	if got, want := string(payload), `{"n":1000,"negative_zero":0,"large":9007199254740992,"overflow":null}`; got != want {
		t.Fatalf("JSON = %s, want %s", got, want)
	}
}

func TestConversationTurnstileNumberSupportsJavaScriptRadixStrings(t *testing.T) {
	vm := &conversationTurnstileVM{memoryBudget: &conversationTurnstileMemoryBudget{}}
	for _, test := range []struct {
		input   string
		want    float64
		wantOK  bool
		wantNaN bool
	}{
		{input: "0x10", want: 16, wantOK: true},
		{input: "0B10", want: 2, wantOK: true},
		{input: "0o10", want: 8, wantOK: true},
		{input: "  0Xff  ", want: 255, wantOK: true},
		{input: "", want: 0, wantOK: true},
		{input: "Infinity", want: math.Inf(1), wantOK: true},
		{input: "NaN", wantOK: true, wantNaN: true},
		{input: "0b2", wantNaN: true},
	} {
		t.Run(test.input, func(t *testing.T) {
			got, ok, err := vm.number(test.input)
			if err != nil {
				t.Fatalf("number(%q) error = %v", test.input, err)
			}
			if ok != test.wantOK {
				t.Fatalf("number(%q) ok = %t, want %t", test.input, ok, test.wantOK)
			}
			if test.wantNaN {
				if !math.IsNaN(got) {
					t.Fatalf("number(%q) = %v, want NaN", test.input, got)
				}
				return
			}
			if got != test.want {
				t.Fatalf("number(%q) = %v, want %v", test.input, got, test.want)
			}
		})
	}
}

func TestConversationTurnstileScriptSourceMissingReturnsNull(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:           context.Background(),
		values:        make(map[string]any),
		scriptSources: []string{"https://example.com/app.js"},
		memoryBudget:  &conversationTurnstileMemoryBudget{},
	}
	vm.set(40, "sentinel/not-present")
	if _, err := vm.opScriptSource([]any{41, 40}); err != nil {
		t.Fatalf("opScriptSource() error = %v", err)
	}
	if got := vm.get(41); got != nil {
		t.Fatalf("missing script source = %#v, want null", got)
	}
	called := false
	vm.set(42, newConversationTurnstileCallable(func([]any) (any, error) {
		called = true
		return nil, nil
	}))
	if _, err := vm.opIfDefined([]any{41, 42}); err != nil {
		t.Fatalf("opIfDefined() error = %v", err)
	}
	if !called {
		t.Fatal("null script source was treated as undefined")
	}
}

func TestConversationTurnstileJSONStringifyTopLevelUnsupportedReturnsUndefined(t *testing.T) {
	vm := &conversationTurnstileVM{values: make(map[string]any), memoryBudget: &conversationTurnstileMemoryBudget{}}
	for _, value := range []any{
		conversationTurnstileUndefined,
		newConversationTurnstileCallable(conversationTurnstileNoop),
	} {
		vm.set(40, value)
		if _, err := vm.opJSONStringify([]any{41, 40}); err != nil {
			t.Fatalf("opJSONStringify(%T) error = %v", value, err)
		}
		if got := vm.get(41); !isConversationTurnstileUndefined(got) {
			t.Fatalf("opJSONStringify(%T) = %#v, want undefined", value, got)
		}
	}

	object := newConversationTurnstileOrderedMap()
	object.set("omitted", conversationTurnstileUndefined)
	object.set("kept", 1)
	vm.set(40, conversationTurnstileArrayValue([]any{conversationTurnstileUndefined, newConversationTurnstileCallable(conversationTurnstileNoop), object}))
	if _, err := vm.opJSONStringify([]any{41, 40}); err != nil {
		t.Fatalf("nested opJSONStringify() error = %v", err)
	}
	if got, want := vm.get(41), `[null,null,{"kept":1}]`; got != want {
		t.Fatalf("nested opJSONStringify() = %#v, want %s", got, want)
	}
}

func TestConversationTurnstileRunQueuePropagatesOrdinaryInstructionErrors(t *testing.T) {
	vm := &conversationTurnstileVM{ctx: context.Background(), values: make(map[string]any), executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 10}, memoryBudget: &conversationTurnstileMemoryBudget{}}
	vm.set(40, newConversationTurnstileCallable(func([]any) (any, error) { return nil, errors.New("uncaught") }))
	called := false
	vm.set(41, newConversationTurnstileCallable(func([]any) (any, error) {
		called = true
		return nil, nil
	}))
	vm.set(9, conversationTurnstileArrayValue([]any{
		conversationTurnstileArrayValue([]any{40}),
		conversationTurnstileArrayValue([]any{41}),
	}))
	if err := vm.runQueue(); err == nil || err.Error() != "uncaught" {
		t.Fatalf("runQueue error = %v, want uncaught", err)
	}
	if called {
		t.Fatal("runQueue continued after an uncaught instruction error")
	}

	vm = &conversationTurnstileVM{ctx: context.Background(), values: make(map[string]any), executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 10}, memoryBudget: &conversationTurnstileMemoryBudget{}}
	vm.set(40, newConversationTurnstileCallable(func([]any) (any, error) { return nil, errors.New("caught") }))
	if _, err := vm.opCallRaw([]any{41, 40}); err != nil {
		t.Fatalf("opCallRaw error = %v", err)
	}
	if got := vm.get(41); got != "Error: caught" {
		t.Fatalf("caught error = %#v", got)
	}
	if _, err := vm.opCallResult([]any{42, 40}); err != nil {
		t.Fatalf("opCallResult error = %v", err)
	}
	if got := vm.get(42); got != "Error: caught" {
		t.Fatalf("opCallResult caught error = %#v", got)
	}
	vm.set(40, "window.Math.random")
	if _, err := vm.opCallResult([]any{42, 40}); err != nil {
		t.Fatalf("opCallResult string target error = %v", err)
	}
	if got := vm.get(42); !strings.HasPrefix(fmt.Sprint(got), "TypeError:") {
		t.Fatalf("string callable error = %#v", got)
	}
	vm.set(40, newConversationTurnstileCallable(func([]any) (any, error) {
		return nil, conversationTurnstileFatalError{message: "resource limit"}
	}))
	if _, err := vm.opCallRaw([]any{41, 40}); err == nil || err.Error() != "resource limit" {
		t.Fatalf("opCallRaw fatal error = %v", err)
	}
}

func TestConversationTurnstileRunQueuePropagatesFatalInstructionErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "fatal", err: conversationTurnstileFatalError{message: "challenge rejected"}},
		{name: "canceled", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			vm := &conversationTurnstileVM{ctx: context.Background(), values: make(map[string]any), executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 10}, memoryBudget: &conversationTurnstileMemoryBudget{}}
			vm.set(40, newConversationTurnstileCallable(func([]any) (any, error) { return nil, test.err }))
			vm.set(9, conversationTurnstileArrayValue([]any{conversationTurnstileArrayValue([]any{40})}))
			if err := vm.runQueue(); err == nil || !errors.Is(err, test.err) && err.Error() != test.err.Error() {
				t.Fatalf("runQueue error = %v, want %v", err, test.err)
			}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	vm := &conversationTurnstileVM{ctx: ctx, values: make(map[string]any), executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 10}, memoryBudget: &conversationTurnstileMemoryBudget{}}
	vm.set(40, newConversationTurnstileCallable(func([]any) (any, error) {
		cancel()
		return nil, errors.New("ordinary error after cancellation")
	}))
	vm.set(9, conversationTurnstileArrayValue([]any{conversationTurnstileArrayValue([]any{40})}))
	if err := vm.runQueue(); !errors.Is(err, context.Canceled) {
		t.Fatalf("runQueue cancellation error = %v, want context.Canceled", err)
	}
}

func TestConversationTurnstileRunQueuePropagatesFatalMapKeyErrors(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx: context.Background(),
		values: map[string]any{
			conversationTurnstileMapKey(9): conversationTurnstileArrayValue([]any{}),
		},
		memoryBudget: &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{
			maxSteps:    10,
			runtimeWork: conversationTurnstileMaxRuntimeWork,
		},
	}
	err := vm.runQueue()
	var fatal conversationTurnstileFatalError
	if !errors.As(err, &fatal) || !strings.Contains(err.Error(), "runtime work exceeds") {
		t.Fatalf("runQueue map key error = %v", err)
	}
}

func TestConversationTurnstileJSONParsePreservesFatalDecodeErrors(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx: context.Background(),
		values: map[string]any{
			conversationTurnstileMapKey(40): `[]`,
		},
		memoryBudget: &conversationTurnstileMemoryBudget{
			used: conversationTurnstileMaxRuntimeBytes - 64,
		},
		executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 10},
	}
	_, err := vm.opJSONParse([]any{41, 40})
	var fatal conversationTurnstileFatalError
	if !errors.As(err, &fatal) || !strings.Contains(err.Error(), "runtime allocation exceeds") {
		t.Fatalf("opJSONParse fatal error = %v", err)
	}
	var jsError conversationTurnstileJSError
	if errors.As(err, &jsError) {
		t.Fatalf("fatal JSON decode error was converted to %v", jsError)
	}
}

func TestConversationTurnstileObjectCreatePreservesPrototypeSemantics(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 10},
	}
	nullPrototype, err := vm.call(conversationTurnstileObjectRef{path: "window.Object.create"}, []any{conversationTurnstileExplicitNull})
	if err != nil {
		t.Fatalf("Object.create(null) error = %v", err)
	}
	if _, err = vm.primitive(nullPrototype); err == nil || !strings.Contains(err.Error(), "Cannot convert object to primitive value") {
		t.Fatalf("null-prototype primitive error = %v", err)
	}
	sdkNullPrototype, err := vm.call(conversationTurnstileObjectRef{path: "window.Object.create"}, []any{nil})
	if err != nil {
		t.Fatalf("Sentinel Object.create(JSON null) error = %v", err)
	}
	if primitive, errPrimitive := vm.primitive(sdkNullPrototype); errPrimitive != nil || primitive != "[object Object]" {
		t.Fatalf("Sentinel JSON null prototype primitive = %#v, error = %v", primitive, errPrimitive)
	}

	prototype := newConversationTurnstileOrderedMap()
	prototype.set("inherited", "value")
	prototype.set("toString", newConversationTurnstileCallable(func([]any) (any, error) {
		return "custom", nil
	}))
	created, err := vm.call(conversationTurnstileObjectRef{path: "window.Object.create"}, []any{prototype})
	if err != nil {
		t.Fatalf("Object.create(prototype) error = %v", err)
	}
	if inherited, errProperty := vm.property(created, "inherited"); errProperty != nil || inherited != "value" {
		t.Fatalf("inherited property = %#v, error = %v", inherited, errProperty)
	}
	if primitive, errPrimitive := vm.primitive(created); errPrimitive != nil || primitive != "custom" {
		t.Fatalf("custom prototype primitive = %#v, error = %v", primitive, errPrimitive)
	}
}

func TestConversationTurnstileDefaultToPrimitivePrefersValueOf(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		values:          make(map[string]any),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 10},
	}
	object := newConversationTurnstileOrderedMap()
	object.set("valueOf", newConversationTurnstileCallable(func([]any) (any, error) { return 1, nil }))
	object.set("toString", newConversationTurnstileCallable(func([]any) (any, error) { return "2", nil }))
	vm.set(40, object)
	vm.set(41, 3)
	if _, err := vm.opAppend([]any{40, 41}); err != nil {
		t.Fatal(err)
	}
	if value := vm.get(40); value != float64(4) {
		t.Fatalf("object + 3 = %#v", value)
	}
	if less, err := vm.lessThan(object, 2); err != nil || !less {
		t.Fatalf("object < 2 = %t, error = %v", less, err)
	}
	if value, err := vm.call(conversationTurnstileObjectRef{path: "window.String"}, []any{object}); err != nil || value != "2" {
		t.Fatalf("String(object) = %#v, error = %v", value, err)
	}
}

func TestConversationTurnstileNumberUsesValueOf(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 10},
	}
	object := newConversationTurnstileOrderedMap()
	object.set("valueOf", newConversationTurnstileCallable(func([]any) (any, error) { return -7, nil }))
	value, err := vm.call(conversationTurnstileObjectRef{path: "window.Math.abs"}, []any{object})
	if err != nil || value != float64(7) {
		t.Fatalf("Math.abs(object) = %#v, error = %v", value, err)
	}
}

func TestConversationTurnstilePrototypeTraversalIsBounded(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 10},
	}
	deep := newConversationTurnstileOrderedMap()
	for range conversationTurnstileMaxPrototypeDepth {
		child := newConversationTurnstileOrderedMap()
		child.prototype = deep
		child.prototypeSet = true
		deep = child
	}
	_, err := vm.primitive(deep)
	var fatal conversationTurnstileFatalError
	if !errors.As(err, &fatal) || !strings.Contains(err.Error(), "prototype chain exceeds") {
		t.Fatalf("deep primitive error = %v", err)
	}
	_, err = vm.property(deep, "missing")
	if !errors.As(err, &fatal) || !strings.Contains(err.Error(), "prototype chain exceeds") {
		t.Fatalf("deep property error = %v", err)
	}
}

func TestConversationTurnstileIfElapsedSkipsNaN(t *testing.T) {
	for _, test := range []struct {
		name      string
		left      any
		right     any
		threshold any
		wantCalls int
	}{
		{name: "left NaN", left: math.NaN(), right: 0, threshold: 1},
		{name: "right NaN", left: 2, right: math.NaN(), threshold: 1},
		{name: "threshold NaN", left: 2, right: 0, threshold: math.NaN()},
		{name: "delta NaN", left: math.Inf(1), right: math.Inf(1), threshold: 1},
		{name: "finite exceeds", left: 2, right: 0, threshold: 1, wantCalls: 1},
		{name: "negative threshold", left: 0, right: 0, threshold: -1, wantCalls: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			vm := &conversationTurnstileVM{ctx: context.Background(), values: make(map[string]any), memoryBudget: &conversationTurnstileMemoryBudget{}}
			vm.set(40, test.left)
			vm.set(41, test.right)
			vm.set(42, test.threshold)
			calls := 0
			vm.set(43, newConversationTurnstileCallable(func([]any) (any, error) {
				calls++
				return nil, nil
			}))
			if _, err := vm.opIfElapsed([]any{40, 41, 42, 43}); err != nil {
				t.Fatalf("opIfElapsed error = %v", err)
			}
			if calls != test.wantCalls {
				t.Fatalf("callback calls = %d, want %d", calls, test.wantCalls)
			}
		})
	}
}

func TestConversationTurnstileFormatNumberUsesECMAScriptExponentBoundaries(t *testing.T) {
	for _, test := range []struct {
		value float64
		want  string
	}{
		{value: 1e20, want: "100000000000000000000"},
		{value: 1e21, want: "1e+21"},
		{value: 1e-6, want: "0.000001"},
		{value: 1e-7, want: "1e-7"},
	} {
		if got := conversationTurnstileFormatNumber(test.value); got != test.want {
			t.Errorf("format(%g) = %q, want %q", test.value, got, test.want)
		}
	}
}

func TestConversationTurnstileStringSearchAndIndexOfSemantics(t *testing.T) {
	vm := &conversationTurnstileVM{ctx: context.Background(), values: make(map[string]any), memoryBudget: &conversationTurnstileMemoryBudget{}}

	bound, err := vm.bindStringProperty("ababa", "indexOf")
	if err != nil {
		t.Fatal(err)
	}
	indexOf := bound.(conversationTurnstileCallable)
	for _, test := range []struct {
		args []any
		want int
	}{
		{args: []any{"ba", 2}, want: 3},
		{args: []any{"ba", -10}, want: 1},
		{args: []any{"", math.Inf(1)}, want: 5},
		{args: []any{"", 1e300}, want: 5},
	} {
		got, err := indexOf.invoke(test.args)
		if err != nil || got != test.want {
			t.Fatalf("indexOf(%#v) = %#v, %v, want %d", test.args, got, err, test.want)
		}
	}

	bound, err = vm.bindStringProperty("xundefined", "indexOf")
	if err != nil {
		t.Fatal(err)
	}
	undefinedIndexOf := bound.(conversationTurnstileCallable)
	if got, err := undefinedIndexOf.invoke(nil); err != nil || got != 1 {
		t.Fatalf("indexOf() = %#v, %v, want 1", got, err)
	}

	bound, err = vm.bindStringProperty("abc", "search")
	if err != nil {
		t.Fatal(err)
	}
	search := bound.(conversationTurnstileCallable)
	if got, err := search.invoke([]any{"b."}); err != nil || got != 1 {
		t.Fatalf("search(b.) = %#v, %v, want 1", got, err)
	}
	if got, err := search.invoke(nil); err != nil || got != 0 {
		t.Fatalf("search() = %#v, %v, want 0", got, err)
	}
	bound, err = vm.bindStringProperty("😀ab", "search")
	if err != nil {
		t.Fatal(err)
	}
	utf16Search := bound.(conversationTurnstileCallable)
	if got, err := utf16Search.invoke([]any{"a"}); err != nil || got != 2 {
		t.Fatalf("UTF-16 search = %#v, %v, want 2", got, err)
	}
	match, err := vm.bindStringProperty("abc", "match")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := vm.call(match, []any{"z"}); err != nil || got != nil {
		t.Fatalf("match(z) = %#v, %v, want null", got, err)
	}
	for _, args := range [][]any{nil, {conversationTurnstileUndefined}} {
		got, err := vm.call(match, args)
		if err != nil {
			t.Fatalf("match(%#v) error = %v", args, err)
		}
		matches, ok := conversationTurnstileSlice(got)
		if !ok || !reflect.DeepEqual(matches, []any{""}) {
			t.Fatalf("match(%#v) = %#v, want [empty string]", args, got)
		}
	}
	got, err := vm.call(match, []any{"(z)?b"})
	if err != nil {
		t.Fatalf("match optional capture error = %v", err)
	}
	matches, ok := conversationTurnstileSlice(got)
	if !ok || len(matches) != 2 || matches[0] != "b" || !isConversationTurnstileUndefined(matches[1]) {
		t.Fatalf("match optional capture = %#v, want [b, undefined]", got)
	}
}

func TestConversationTurnstileResolveReservesBeforeAllocation(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:          context.Background(),
		values:       map[string]any{conversationTurnstileMapKey(1): "value"},
		memoryBudget: &conversationTurnstileMemoryBudget{used: conversationTurnstileMaxRuntimeBytes - 15},
	}
	if _, err := vm.resolve([]any{1}); err == nil || !strings.Contains(err.Error(), "runtime allocation") {
		t.Fatalf("resolve error = %v, want runtime allocation limit", err)
	}
}

func TestConversationTurnstileStringConversionBoundsDepth(t *testing.T) {
	var value any = "leaf"
	for depth := 0; depth < conversationTurnstileMaxJSONDepth+2; depth++ {
		value = []any{value}
	}
	if _, err := conversationTurnstileStringLimitedContext(context.Background(), value, conversationTurnstileMaxValueBytes, nil, nil); err == nil || !strings.Contains(err.Error(), "exceeds depth") {
		t.Fatalf("string conversion error = %v, want depth limit", err)
	}
}

func TestConversationTurnstileObjectKeysUseJavaScriptOrder(t *testing.T) {
	decoded, err := decodeConversationTurnstileJSON([]byte(`{"2":"two","1":"one","01":"leading","4294967294":"max","4294967295":"not-index","a":"letter"}`))
	if err != nil {
		t.Fatalf("decode error = %v", err)
	}
	object := decoded.(*conversationTurnstileOrderedMap)
	vm := &conversationTurnstileVM{memoryBudget: &conversationTurnstileMemoryBudget{}}
	keysValue, err := vm.call(conversationTurnstileObjectRef{path: "window.Object.keys"}, []any{object})
	if err != nil {
		t.Fatalf("Object.keys error = %v", err)
	}
	keys, _ := conversationTurnstileSlice(keysValue)
	wantKeys := []any{"1", "2", "4294967294", "01", "4294967295", "a"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("Object.keys = %#v, want %#v", keys, wantKeys)
	}
	payload, err := marshalConversationTurnstileJSON(object)
	if err != nil {
		t.Fatalf("marshal error = %v", err)
	}
	wantJSON := `{"1":"one","2":"two","4294967294":"max","01":"leading","4294967295":"not-index","a":"letter"}`
	if string(payload) != wantJSON {
		t.Fatalf("JSON = %s, want %s", payload, wantJSON)
	}
}

func TestConversationTurnstileObjectKeysHandlesStringsArraysAndNull(t *testing.T) {
	vm := &conversationTurnstileVM{ctx: context.Background(), memoryBudget: &conversationTurnstileMemoryBudget{}}
	for _, test := range []struct {
		name  string
		value any
		want  []any
	}{
		{name: "string UTF-16 indices", value: "😀x", want: []any{"0", "1", "2"}},
		{name: "array indices", value: conversationTurnstileArrayValue([]any{"a", "b"}), want: []any{"0", "1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			keysValue, err := vm.call(conversationTurnstileObjectRef{path: "window.Object.keys"}, []any{test.value})
			if err != nil {
				t.Fatal(err)
			}
			keys, _ := conversationTurnstileSlice(keysValue)
			if !reflect.DeepEqual(keys, test.want) {
				t.Fatalf("Object.keys = %#v, want %#v", keys, test.want)
			}
		})
	}
	for _, value := range []any{nil, conversationTurnstileUndefined, conversationTurnstileExplicitNull} {
		if _, err := vm.call(conversationTurnstileObjectRef{path: "window.Object.keys"}, []any{value}); err == nil {
			t.Fatalf("Object.keys(%#v) error = nil", value)
		}
	}
}

func TestConversationTurnstileObjectKeysConsumeBudgetAndObserveCancellation(t *testing.T) {
	object := newConversationTurnstileOrderedMap()
	for index := range 2048 {
		object.set(fmt.Sprintf("key-%d", index), index)
	}
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{runtimeWork: conversationTurnstileMaxRuntimeWork - 1024},
	}
	if _, err := vm.call(conversationTurnstileObjectRef{path: "window.Object.keys"}, []any{object}); err == nil || !strings.Contains(err.Error(), "runtime work exceeds") {
		t.Fatalf("Object.keys budget error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	vm = &conversationTurnstileVM{ctx: ctx, memoryBudget: &conversationTurnstileMemoryBudget{}}
	if _, err := vm.call(conversationTurnstileObjectRef{path: "window.Object.keys"}, []any{object}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Object.keys cancellation error = %v, want context.Canceled", err)
	}
}

func TestConversationTurnstileRegexpIsBoundedAndCached(t *testing.T) {
	vm := &conversationTurnstileVM{memoryBudget: &conversationTurnstileMemoryBudget{}}
	if _, err := vm.compileRegexp(strings.Repeat("a", conversationTurnstileMaxRegexpBytes+1)); err == nil {
		t.Fatal("oversized regular expression was accepted")
	}
	if vm.memoryBudget.used != 0 {
		t.Fatalf("oversized regular expression consumed %d budget bytes", vm.memoryBudget.used)
	}
	if _, err := vm.compileRegexp(conversationTurnstileJSString{units: make([]uint16, conversationTurnstileMaxRegexpBytes+1)}); err == nil {
		t.Fatal("oversized UTF-16 regular expression was accepted")
	}
	if vm.memoryBudget.used != 0 {
		t.Fatalf("oversized UTF-16 regular expression consumed %d budget bytes", vm.memoryBudget.used)
	}
	first, err := vm.compileRegexp(`sentinel/[0-9]+`)
	if err != nil {
		t.Fatalf("first compile error = %v", err)
	}
	second, err := vm.compileRegexp(`sentinel/[0-9]+`)
	if err != nil {
		t.Fatalf("cached compile error = %v", err)
	}
	if first != second || len(vm.regexpCache) != 1 {
		t.Fatalf("regexp cache = first %p, second %p, size %d", first, second, len(vm.regexpCache))
	}
}

func TestConversationTurnstileLessUsesJavaScriptStringOrdering(t *testing.T) {
	vm := &conversationTurnstileVM{memoryBudget: &conversationTurnstileMemoryBudget{}}
	less, err := vm.lessThan("2", "10")
	if err != nil || less {
		t.Fatalf("\"2\" < \"10\" = %t, %v", less, err)
	}
	less, err = vm.lessThan("10", conversationTurnstileJSString{units: []uint16{'2'}})
	if err != nil || !less {
		t.Fatalf("\"10\" < \"2\" = %t, %v", less, err)
	}
}

func TestConversationTurnstileStrictEqualityDistinguishesIdentityAndUndefined(t *testing.T) {
	first := newConversationTurnstileCallable(conversationTurnstileNoop)
	alias := first
	second := newConversationTurnstileCallable(conversationTurnstileNoop)
	if !conversationTurnstileStrictEqual(first, alias) {
		t.Fatal("callable alias lost identity")
	}
	if conversationTurnstileStrictEqual(first, second) {
		t.Fatal("distinct callables compare equal")
	}
	if conversationTurnstileStrictEqual(conversationTurnstileUndefined, nil) {
		t.Fatal("undefined compares equal to null")
	}
	if !conversationTurnstileStrictEqual(conversationTurnstileUndefined, conversationTurnstileUndefined) {
		t.Fatal("undefined does not compare equal to itself")
	}
}

func TestConversationTurnstileStrictEqualityWorkIsBounded(t *testing.T) {
	vm := &conversationTurnstileVM{memoryBudget: &conversationTurnstileMemoryBudget{}}
	value := strings.Repeat("x", 1<<20)
	for index := 0; index < conversationTurnstileMaxRuntimeWork/(len(value)*2); index++ {
		equal, err := vm.strictEqual(value, value)
		if err != nil || !equal {
			t.Fatalf("strictEqual() = %t, %v before budget was exhausted", equal, err)
		}
	}
	if _, err := vm.strictEqual(value, value); err == nil || !strings.Contains(err.Error(), "runtime work exceeds") {
		t.Fatalf("strictEqual() error = %v, want runtime work limit", err)
	}
}

func TestMarshalConversationTurnstileJSONRejectsCycles(t *testing.T) {
	object := newConversationTurnstileOrderedMap()
	object.set("self", object)
	if _, err := marshalConversationTurnstileJSON(object); err == nil {
		t.Fatal("marshalConversationTurnstileJSON() accepted a cycle")
	}

	items := make([]any, 1)
	items[0] = items
	if _, err := marshalConversationTurnstileJSON(items); err == nil {
		t.Fatal("marshalConversationTurnstileJSON() accepted a slice cycle")
	}
}

func TestBuildConversationTurnstileTokenRejectsInvalidChallenge(t *testing.T) {
	for name, test := range map[string]struct {
		dx    string
		token string
	}{
		"missing dx":      {token: "requirements"},
		"missing token":   {dx: "challenge"},
		"invalid base64":  {dx: "%%%", token: "requirements"},
		"invalid program": {dx: base64.StdEncoding.EncodeToString([]byte("invalid")), token: "requirements"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := BuildConversationTurnstileToken(context.Background(), test.dx, test.token, zeroReader{}, time.Now); err == nil {
				t.Fatal("BuildConversationTurnstileToken() error = nil")
			}
		})
	}
}

func TestBuildConversationTurnstileTokenUsesSDKInstructionFallback(t *testing.T) {
	requirementsToken := "requirements"
	dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{2, 40, "value"},
	})
	token, err := BuildConversationTurnstileToken(context.Background(), dx, requirementsToken, zeroReader{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if token != "1" {
		t.Fatalf("fallback token = %q, want 1", token)
	}
}

func TestBuildConversationTurnstileTokenEncodesOrdinaryVMErrors(t *testing.T) {
	requirementsToken := "requirements"
	dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{7, 99},
	})
	token, err := BuildConversationTurnstileToken(context.Background(), dx, requirementsToken, zeroReader{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		t.Fatalf("decode error token: %v", err)
	}
	if got := string(decoded); got != "0: TypeError: value is not callable" {
		t.Fatalf("error token = %q", got)
	}
}

func TestBuildConversationTurnstileTokenRejectsInvalidInstructions(t *testing.T) {
	requirementsToken := "requirements"
	for _, test := range []struct {
		name    string
		program []any
	}{
		{name: "unknown opcode", program: []any{[]any{99}}},
		{name: "empty instruction", program: []any{[]any{}}},
		{name: "non-callable opcode", program: []any{[]any{2, 40, "literal"}, []any{40}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dx := encodeConversationTurnstileProgram(t, requirementsToken, test.program)
			token, err := BuildConversationTurnstileToken(context.Background(), dx, requirementsToken, zeroReader{}, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := base64.StdEncoding.DecodeString(token)
			if err != nil {
				t.Fatalf("decode error token: %v", err)
			}
			if got := string(decoded); !strings.Contains(got, "TypeError:") {
				t.Fatalf("error token = %q", got)
			}
		})
	}
}

func TestBuildConversationTurnstileTokenRejectsNullPropertyAccess(t *testing.T) {
	requirementsToken := "requirements"
	for _, test := range []struct {
		name       string
		objectCode []any
		want       string
	}{
		{name: "null", objectCode: []any{2, 40, nil}, want: "2: TypeError: Cannot read properties of null (reading 'x')"},
		{name: "undefined", objectCode: nil, want: "1: TypeError: Cannot read properties of undefined (reading 'x')"},
	} {
		t.Run(test.name, func(t *testing.T) {
			program := make([]any, 0, 3)
			if test.objectCode != nil {
				program = append(program, test.objectCode)
			}
			program = append(program, []any{2, 41, "x"}, []any{6, 42, 40, 41})
			dx := encodeConversationTurnstileProgram(t, requirementsToken, program)
			token, err := BuildConversationTurnstileToken(context.Background(), dx, requirementsToken, zeroReader{}, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := base64.StdEncoding.DecodeString(token)
			if err != nil {
				t.Fatalf("decode error token: %v", err)
			}
			if got := string(decoded); got != test.want {
				t.Fatalf("error token = %q, want %q", got, test.want)
			}
		})
	}
}

func TestBuildConversationTurnstileTokenRejectsOversizedRawInputs(t *testing.T) {
	oversizedDX := strings.Repeat(" ", base64.StdEncoding.EncodedLen(conversationTurnstileMaxBytes)+1)
	if _, err := BuildConversationTurnstileToken(context.Background(), oversizedDX, "requirements", zeroReader{}, time.Now); err == nil || !strings.Contains(err.Error(), "payload exceeds") {
		t.Fatalf("oversized raw dx error = %v", err)
	}
	oversizedToken := strings.Repeat("x", conversationTurnstileMaxValueBytes+1)
	if _, err := BuildConversationTurnstileToken(context.Background(), "challenge", oversizedToken, zeroReader{}, time.Now); err == nil || !strings.Contains(err.Error(), "requirements token exceeds") {
		t.Fatalf("oversized requirements token error = %v", err)
	}
}

func TestDecodeConversationTurnstileDXRejectsOversizedChallenge(t *testing.T) {
	dx := base64.StdEncoding.EncodeToString(make([]byte, conversationTurnstileMaxBytes+1))
	if _, err := decodeConversationTurnstileDX(dx); err == nil || !strings.Contains(err.Error(), "payload exceeds") {
		t.Fatalf("decodeConversationTurnstileDX() error = %v, want payload limit", err)
	}
}

func TestDecodeConversationTurnstileJSONRejectsExcessiveNodesAndDepth(t *testing.T) {
	var flat strings.Builder
	flat.Grow(conversationTurnstileMaxJSONNodes*2 + 2)
	flat.WriteByte('[')
	for index := 0; index <= conversationTurnstileMaxJSONNodes; index++ {
		if index > 0 {
			flat.WriteByte(',')
		}
		flat.WriteByte('0')
	}
	flat.WriteByte(']')
	if _, err := decodeConversationTurnstileJSON([]byte(flat.String())); err == nil || !strings.Contains(err.Error(), "nodes") {
		t.Fatalf("node limit error = %v", err)
	}
	deep := strings.Repeat("[", conversationTurnstileMaxJSONDepth+2) + "0" + strings.Repeat("]", conversationTurnstileMaxJSONDepth+2)
	if _, err := decodeConversationTurnstileJSON([]byte(deep)); err == nil || !strings.Contains(err.Error(), "depth") {
		t.Fatalf("depth limit error = %v", err)
	}
}

func TestConversationTurnstileVMRejectsExcessiveQueueDepth(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		values:          make(map[string]any),
		executionBudget: &conversationTurnstileExecutionBudget{maxSteps: defaultConversationTurnstileMaxSteps},
	}
	var nested conversationTurnstileCallable
	nested = newConversationTurnstileCallable(func([]any) (any, error) {
		previous := vm.get(9)
		vm.set(9, []any{[]any{40}})
		defer func() { vm.set(9, previous) }()
		return nil, vm.runQueue()
	})
	vm.set(40, nested)
	vm.set(9, []any{[]any{40}})
	if err := vm.runQueue(); err == nil {
		t.Fatal("runQueue() error = nil")
	}
}

func TestConversationTurnstileSettledSubroutineDoesNotRun(t *testing.T) {
	requirementsToken := "requirements"
	dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{30, 40, 50, []any{[]any{7, 40}}},
		[]any{2, 41, "ok"},
		[]any{7, 3, 41},
		[]any{17, 42, 40},
	})
	token, err := BuildConversationTurnstileToken(context.Background(), dx, requirementsToken, zeroReader{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	want := base64.StdEncoding.EncodeToString([]byte("ok"))
	if token != want {
		t.Fatalf("token = %q, want %q", token, want)
	}
}

func TestConversationTurnstileVMRejectsExcessiveRuntimeAllocation(t *testing.T) {
	requirementsToken := "requirements"
	program := []any{[]any{2, 40, "x"}}
	for index := 0; index < 24; index++ {
		program = append(program, []any{5, 40, 40})
	}
	program = append(program, []any{7, 3, 40})
	dx := encodeConversationTurnstileProgram(t, requirementsToken, program)
	_, err := BuildConversationTurnstileToken(context.Background(), dx, requirementsToken, zeroReader{}, time.Now)
	if err == nil || !strings.Contains(err.Error(), "turnstile") || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("runtime allocation error = %v", err)
	}
}

func TestBuildConversationTurnstileTokenObservesCancellationAndStepLimit(t *testing.T) {
	requirementsToken := "requirements"
	dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{2, 40, "first"},
		[]any{2, 41, "second"},
		[]any{7, 3, 41},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := BuildConversationTurnstileToken(ctx, dx, requirementsToken, zeroReader{}, time.Now); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v, want context.Canceled", err)
	}
	_, err := buildConversationTurnstileToken(context.Background(), dx, requirementsToken, zeroReader{}, time.Now, 1)
	if err == nil || !strings.Contains(err.Error(), "exceeded 1 steps") {
		t.Fatalf("step limit error = %v", err)
	}
}

func TestConversationTurnstileNestedChallengeUsesSDKInstructionFallback(t *testing.T) {
	requirementsToken := "requirements"
	child := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{8, 41, 40},
		[]any{7, 3, 41},
	})
	parent := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{2, 40, "shared"},
		[]any{0, child},
		[]any{2, 40, "unreachable"},
		[]any{7, 3, 40},
	})
	token, err := buildConversationTurnstileToken(context.Background(), parent, requirementsToken, zeroReader{}, time.Now, 10)
	if err != nil {
		t.Fatalf("nested challenge error = %v", err)
	}
	want := "1"
	if token != want {
		t.Fatalf("nested challenge token = %q, want %q", token, want)
	}
}

func TestConversationTurnstilePromiseResolveAssimilatesThenables(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:             context.Background(),
		values:          make(map[string]any),
		memoryBudget:    &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{maxSteps: 100},
	}
	thenable := newConversationTurnstileOrderedMap()
	thenable.set("then", newConversationTurnstileCallable(func(args []any) (any, error) {
		resolve := args[0].(conversationTurnstileCallable)
		return resolve.invoke([]any{"resolved"})
	}))
	vm.set(40, thenable)
	if _, err := vm.opPromiseResolve([]any{41, 40}); err != nil {
		t.Fatalf("opPromiseResolve() error = %v", err)
	}
	if got := vm.get(41); got != "resolved" {
		t.Fatalf("resolved thenable = %#v", got)
	}

	pending := newConversationTurnstileOrderedMap()
	pending.set("then", newConversationTurnstileCallable(func([]any) (any, error) {
		return conversationTurnstileUndefined, nil
	}))
	vm.deferred = false
	vm.set(42, pending)
	if _, err := vm.opPromiseResolve([]any{43, 42}); err != nil {
		t.Fatalf("pending opPromiseResolve() error = %v", err)
	}
	if !vm.deferred || !isConversationTurnstileUndefined(vm.get(43)) {
		t.Fatalf("pending thenable state = deferred %t, destination %#v", vm.deferred, vm.get(43))
	}

	nonCallableThen := newConversationTurnstileOrderedMap()
	nonCallableThen.set("then", conversationTurnstileObjectRef{path: "window.Math"})
	vm.deferred = false
	vm.set(44, nonCallableThen)
	if _, err := vm.opPromiseResolve([]any{45, 44}); err != nil {
		t.Fatalf("non-callable then property error = %v", err)
	}
	if got := vm.get(45); got != nonCallableThen {
		t.Fatalf("non-callable then resolved to %#v", got)
	}

	lateError := newConversationTurnstileOrderedMap()
	lateError.set("then", newConversationTurnstileCallable(func(args []any) (any, error) {
		resolve := args[0].(conversationTurnstileCallable)
		if _, err := resolve.invoke([]any{"settled"}); err != nil {
			return nil, err
		}
		return nil, conversationTurnstileJSError{name: "Error", message: "late"}
	}))
	vm.set(46, lateError)
	if _, err := vm.opPromiseResolve([]any{47, 46}); err != nil {
		t.Fatalf("late thenable error was not ignored: %v", err)
	}
	if got := vm.get(47); got != "settled" {
		t.Fatalf("late-error thenable resolved to %#v", got)
	}
}

func TestConversationTurnstileLocalStorageKeysUseRuntimeBudgets(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:              context.Background(),
		localStorageKeys: []string{"key"},
		memoryBudget:     &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{
			maxSteps:    100,
			runtimeWork: conversationTurnstileMaxRuntimeWork - 1,
		},
	}
	_, err := vm.call(conversationTurnstileObjectRef{path: "window.Object.keys"}, []any{conversationTurnstileObjectRef{path: "window.localStorage"}})
	if err == nil || !strings.Contains(err.Error(), "runtime work exceeds") {
		t.Fatalf("localStorage keys budget error = %v", err)
	}
}

func TestConversationTurnstileNestedChallengeUsesSharedRuntimeBudget(t *testing.T) {
	requirementsToken := "requirements"
	child := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{2, 40, "child"},
		[]any{7, 3, 40},
	})
	vm := &conversationTurnstileVM{
		ctx:               context.Background(),
		requirementsToken: requirementsToken,
		memoryBudget:      &conversationTurnstileMemoryBudget{},
		executionBudget: &conversationTurnstileExecutionBudget{
			maxSteps:    100,
			runtimeWork: conversationTurnstileMaxRuntimeWork - 1,
		},
	}
	if _, err := vm.opNestedChallenge([]any{child}); err == nil || !strings.Contains(err.Error(), "runtime work exceeds") {
		t.Fatalf("nested challenge error = %v, want shared runtime work limit", err)
	}
}

func TestConversationTurnstileDecodeStagesObserveCancellation(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString(make([]byte, 64<<10))
	if _, err := decodeConversationTurnstileDXContext(newConversationTurnstileCancelAfterContext(3), encoded); !errors.Is(err, context.Canceled) {
		t.Fatalf("base64 decode error = %v, want context.Canceled", err)
	}
	if _, err := xorConversationTurnstileBytesContext(newConversationTurnstileCancelAfterContext(2), make([]byte, 64<<10), []byte("key")); !errors.Is(err, context.Canceled) {
		t.Fatalf("XOR decode error = %v, want context.Canceled", err)
	}

	var payload strings.Builder
	payload.WriteByte('[')
	for index := 0; index < 1024; index++ {
		if index > 0 {
			payload.WriteByte(',')
		}
		payload.WriteByte('0')
	}
	payload.WriteByte(']')
	if _, err := decodeConversationTurnstileJSONWithContextAndBudget(newConversationTurnstileCancelAfterContext(2), []byte(payload.String()), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("JSON decode error = %v, want context.Canceled", err)
	}
	longString := []byte(`"` + strings.Repeat("x", 16<<10) + `"`)
	if _, err := decodeConversationTurnstileJSONWithContextAndBudget(newConversationTurnstileCancelAfterContext(5), longString, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("JSON string conversion error = %v, want context.Canceled", err)
	}
}

type conversationTurnstileCancelAfterContext struct {
	context.Context
	remaining int
}

func newConversationTurnstileCancelAfterContext(checks int) *conversationTurnstileCancelAfterContext {
	return &conversationTurnstileCancelAfterContext{Context: context.Background(), remaining: checks}
}

func (ctx *conversationTurnstileCancelAfterContext) Err() error {
	ctx.remaining--
	if ctx.remaining <= 0 {
		return context.Canceled
	}
	return nil
}

func encodeConversationTurnstileProgram(t *testing.T, requirementsToken string, program []any) string {
	t.Helper()
	payload, err := json.Marshal(program)
	if err != nil {
		t.Fatalf("encode turnstile program: %v", err)
	}
	encrypted := xorConversationTurnstileBytes(payload, []byte(requirementsToken))
	return base64.StdEncoding.EncodeToString(encrypted)
}
