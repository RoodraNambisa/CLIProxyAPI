package chatgptweb

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"
)

func TestGoConversationTurnstileCanvasMatchesBrowserSemantics(t *testing.T) {
	vm := &conversationTurnstileVM{
		browserProfile: sentinelBrowserProfileForSlot(DefaultPersona(), 0x123),
	}
	canvas, err := vm.newDOMElement("canvas")
	if err != nil {
		t.Fatalf("newDOMElement() error = %v", err)
	}
	getContext, handled, err := vm.browserProperty(canvas, "getContext")
	if err != nil || !handled {
		t.Fatalf("getContext property = %#v, handled=%v, error=%v", getContext, handled, err)
	}
	first, err := vm.call(getContext, []any{"webgl"})
	if err != nil {
		t.Fatalf("first getContext() error = %v", err)
	}
	second, err := vm.call(getContext, []any{"webgl"})
	if err != nil {
		t.Fatalf("second getContext() error = %v", err)
	}
	equal, err := vm.strictEqual(first, second)
	if err != nil || !equal {
		t.Fatalf("getContext() identity equal=%v error=%v", equal, err)
	}
	if got := conversationTurnstileBrowserObjectValueString(canvas); got != "[object HTMLCanvasElement]" {
		t.Fatalf("canvas object string = %q", got)
	}
	getBoundingClientRect, handled, err := vm.browserProperty(canvas, "getBoundingClientRect")
	if err != nil || !handled {
		t.Fatalf("getBoundingClientRect property = %#v, handled=%v, error=%v", getBoundingClientRect, handled, err)
	}
	rectangleValue, err := vm.call(getBoundingClientRect, nil)
	if err != nil {
		t.Fatalf("getBoundingClientRect() error = %v", err)
	}
	rectangle, ok := rectangleValue.(*conversationTurnstileOrderedMap)
	if !ok || rectangle.values[conversationTurnstileMapKey("width")] != 300 || rectangle.values[conversationTurnstileMapKey("height")] != 150 {
		t.Fatalf("getBoundingClientRect() = %#v", rectangleValue)
	}

	getExtension, handled, err := vm.browserProperty(first, "getExtension")
	if err != nil || !handled {
		t.Fatalf("getExtension property = %#v, handled=%v, error=%v", getExtension, handled, err)
	}
	firstExtension, err := vm.call(getExtension, []any{"WEBGL_debug_renderer_info"})
	if err != nil {
		t.Fatalf("first getExtension() error = %v", err)
	}
	secondExtension, err := vm.call(getExtension, []any{"WEBGL_debug_renderer_info"})
	if err != nil {
		t.Fatalf("second getExtension() error = %v", err)
	}
	equal, err = vm.strictEqual(firstExtension, secondExtension)
	if err != nil || !equal {
		t.Fatalf("getExtension() identity equal=%v error=%v", equal, err)
	}

	getSupportedExtensions, handled, err := vm.browserProperty(first, "getSupportedExtensions")
	if err != nil || !handled {
		t.Fatalf("getSupportedExtensions property = %#v, handled=%v, error=%v", getSupportedExtensions, handled, err)
	}
	extensions, err := vm.call(getSupportedExtensions, nil)
	if err != nil {
		t.Fatalf("getSupportedExtensions() error = %v", err)
	}
	list, ok := extensions.(*conversationTurnstileArray)
	if !ok || len(list.items) != 1 || list.items[0] != "WEBGL_debug_renderer_info" {
		t.Fatalf("getSupportedExtensions() = %#v", extensions)
	}

	toDataURL, handled, err := vm.browserProperty(canvas, "toDataURL")
	if err != nil || !handled {
		t.Fatalf("toDataURL property = %#v, handled=%v, error=%v", toDataURL, handled, err)
	}
	dataURL, err := vm.call(toDataURL, nil)
	if err != nil {
		t.Fatalf("toDataURL() error = %v", err)
	}
	payload := "sentinel-canvas-" + vm.browserProfile.version + ":" + vm.browserProfile.catalogID + ":300x150"
	want := "data:image/png;base64," + base64.StdEncoding.EncodeToString([]byte(payload))
	if dataURL != want {
		t.Fatalf("toDataURL() = %q, want %q", dataURL, want)
	}
}

func TestGoConversationTurnstileLocalStorageModelsMissingNamedProperties(t *testing.T) {
	vm := &conversationTurnstileVM{}
	vm.initializeLocalStorage()

	value, handled, err := vm.browserObjectRefProperty("window.localStorage", "missing")
	if err != nil || !handled || !isConversationTurnstileUndefined(value) {
		t.Fatalf("missing localStorage property = %#v, handled=%v, error=%v", value, handled, err)
	}
	if !vm.knownEnvironmentProperty(conversationTurnstileObjectRef{path: "window.localStorage"}, "missing") {
		t.Fatal("missing localStorage named property was classified as unimplemented")
	}
	if _, handled, err = vm.callBrowserObjectRef("window.localStorage.setItem", []any{"stored", "value"}); err != nil || !handled {
		t.Fatalf("localStorage.setItem() handled=%v error=%v", handled, err)
	}
	value, handled, err = vm.browserObjectRefProperty("window.localStorage", "stored")
	if err != nil || !handled || value != "value" {
		t.Fatalf("stored localStorage property = %#v, handled=%v, error=%v", value, handled, err)
	}
}

func TestGoConversationTurnstileDocumentBodyAndDateNow(t *testing.T) {
	current := time.UnixMilli(1_725_000_123_456)
	vm := &conversationTurnstileVM{
		environment:  make(map[string]any),
		memoryBudget: &conversationTurnstileMemoryBudget{},
		now:          func() time.Time { return current },
	}
	body, err := vm.newDOMElement("body")
	if err != nil {
		t.Fatal(err)
	}
	vm.environment["window.document.body"] = body

	documentBody, err := vm.propertyWithKey(conversationTurnstileObjectRef{path: "window.document"}, "body")
	if err != nil || documentBody != body {
		t.Fatalf("document.body = %#v, %v", documentBody, err)
	}
	appendChild, err := vm.propertyWithKey(documentBody, "appendChild")
	if err != nil {
		t.Fatal(err)
	}
	child, err := vm.newDOMElement("canvas")
	if err != nil {
		t.Fatal(err)
	}
	result, err := vm.call(appendChild, []any{child})
	if err != nil || result != child || len(body.children) != 1 || body.children[0] != child {
		t.Fatalf("appendChild() = %#v, %v; children = %#v", result, err, body.children)
	}

	dateNow, err := vm.call(conversationTurnstileObjectRef{path: "window.Date.now"}, nil)
	if err != nil || dateNow != float64(current.UnixMilli()) {
		t.Fatalf("Date.now() = %#v, %v", dateNow, err)
	}
	if _, handled, err := vm.callBrowserObjectRef("window.addEventListener", []any{"load", conversationTurnstileUndefined}); err != nil || !handled {
		t.Fatalf("addEventListener() handled=%v error=%v", handled, err)
	}
	vm.compatibilityErrors = true
	vm.programKind = SentinelProgramObserverCollect
	_, err = vm.call(conversationTurnstileObjectRef{path: "window.Date"}, nil)
	var compatibility *SentinelCompatibilityError
	if !errors.As(err, &compatibility) || compatibility.ProgramKind != SentinelProgramObserverCollect {
		t.Fatalf("Date() compatibility error = %#v", err)
	}
}
