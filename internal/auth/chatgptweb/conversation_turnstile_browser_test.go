package chatgptweb

import (
	"encoding/base64"
	"testing"
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
	payload := "sentinel-canvas-v1:3:300x150"
	want := "data:image/png;base64," + base64.StdEncoding.EncodeToString([]byte(payload))
	if dataURL != want {
		t.Fatalf("toDataURL() = %q, want %q", dataURL, want)
	}
}
