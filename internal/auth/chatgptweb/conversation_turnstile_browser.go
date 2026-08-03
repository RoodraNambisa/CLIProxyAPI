package chatgptweb

import (
	"math"
	"strings"
)

const (
	conversationTurnstileWebGLVendor                 = 0x1f00
	conversationTurnstileWebGLRenderer               = 0x1f01
	conversationTurnstileWebGLVersion                = 0x1f02
	conversationTurnstileWebGLShadingLanguageVersion = 0x8b8c
	conversationTurnstileWebGLUnmaskedVendor         = 0x9245
	conversationTurnstileWebGLUnmaskedRenderer       = 0x9246
)

type conversationTurnstileBrowserObject interface {
	conversationTurnstileBrowserPath() string
}

type conversationTurnstileDOMElement struct {
	vm         *conversationTurnstileVM
	tagName    string
	attributes map[string]string
	children   []any
	style      *conversationTurnstileOrderedMap
}

func (element *conversationTurnstileDOMElement) conversationTurnstileBrowserPath() string {
	if element == nil {
		return "window.document.element"
	}
	return "window.document." + strings.ToLower(element.tagName)
}

type conversationTurnstileCanvasContext struct {
	vm     *conversationTurnstileVM
	canvas *conversationTurnstileDOMElement
	kind   string
}

func (canvas *conversationTurnstileCanvasContext) conversationTurnstileBrowserPath() string {
	if canvas != nil && strings.Contains(strings.ToLower(canvas.kind), "webgl") {
		return "window.WebGLRenderingContext"
	}
	return "window.CanvasRenderingContext2D"
}

type conversationTurnstileWebGLExtension struct{}

func (*conversationTurnstileWebGLExtension) conversationTurnstileBrowserPath() string {
	return "window.WEBGL_debug_renderer_info"
}

func (vm *conversationTurnstileVM) initializeLocalStorage() {
	if vm == nil {
		return
	}
	vm.localStorageValues = make(map[string]string, len(vm.localStorageKeys))
	for _, key := range vm.localStorageKeys {
		vm.localStorageValues[key] = ""
	}
}

func (vm *conversationTurnstileVM) browserCallable(call func([]any) (any, error)) (any, error) {
	if err := vm.reserveRuntimeBytes(64); err != nil {
		return nil, err
	}
	return newConversationTurnstileCallable(call), nil
}

func (vm *conversationTurnstileVM) newDOMElement(name string) (*conversationTurnstileDOMElement, error) {
	name = strings.ToUpper(strings.TrimSpace(name))
	if name == "" {
		name = "DIV"
	}
	if len(name) > 256 {
		return nil, conversationTurnstileFatalError{message: "conversation turnstile element name exceeds limit"}
	}
	if err := vm.reserveRuntimeBytes(512 + len(name)); err != nil {
		return nil, err
	}
	return &conversationTurnstileDOMElement{
		vm:         vm,
		tagName:    name,
		attributes: make(map[string]string),
		style:      newConversationTurnstileOrderedMap(),
	}, nil
}

func (vm *conversationTurnstileVM) callBrowserObjectRef(name string, args []any) (any, bool, error) {
	switch name {
	case "window.document.createElement":
		tagName := "div"
		if len(args) > 0 {
			value, err := vm.runtimeString(args[0])
			if err != nil {
				return nil, true, err
			}
			tagName = value
		}
		element, err := vm.newDOMElement(tagName)
		return element, true, err
	case "window.document.createElementNS":
		tagName := "div"
		if len(args) > 1 {
			value, err := vm.runtimeString(args[1])
			if err != nil {
				return nil, true, err
			}
			tagName = value
		}
		element, err := vm.newDOMElement(tagName)
		return element, true, err
	case "window.document.querySelector", "window.document.getElementById":
		return conversationTurnstileExplicitNull, true, nil
	case "window.document.querySelectorAll", "window.document.getElementsByTagName":
		if err := vm.reserveRuntimeBytes(24); err != nil {
			return nil, true, err
		}
		return conversationTurnstileArrayValue(nil), true, nil
	case "window.document.addEventListener", "window.document.removeEventListener":
		return conversationTurnstileUndefined, true, nil
	case "window.document.dispatchEvent":
		return true, true, nil
	case "window.localStorage.getItem":
		key, err := vm.storageArgument(args, 0)
		if err != nil {
			return nil, true, err
		}
		value, exists := vm.localStorageValues[key]
		if !exists {
			return conversationTurnstileExplicitNull, true, nil
		}
		return value, true, nil
	case "window.localStorage.setItem":
		key, err := vm.storageArgument(args, 0)
		if err != nil {
			return nil, true, err
		}
		value, err := vm.storageArgument(args, 1)
		if err != nil {
			return nil, true, err
		}
		if _, exists := vm.localStorageValues[key]; !exists {
			if len(vm.localStorageKeys) >= conversationTurnstileMaxValueBytes/16 {
				return nil, true, conversationTurnstileFatalError{message: "conversation turnstile localStorage key count exceeds limit"}
			}
			if err = vm.reserveRuntimeBytes(16 + len(key)); err != nil {
				return nil, true, err
			}
			vm.localStorageKeys = append(vm.localStorageKeys, key)
		}
		if err = vm.chargeRuntimeWork(len(key) + len(value)); err != nil {
			return nil, true, err
		}
		if err = vm.reserveRuntimeBytes(len(value)); err != nil {
			return nil, true, err
		}
		vm.localStorageValues[key] = value
		return conversationTurnstileUndefined, true, nil
	case "window.localStorage.removeItem":
		key, err := vm.storageArgument(args, 0)
		if err != nil {
			return nil, true, err
		}
		if _, exists := vm.localStorageValues[key]; exists {
			delete(vm.localStorageValues, key)
			for index, candidate := range vm.localStorageKeys {
				if candidate == key {
					copy(vm.localStorageKeys[index:], vm.localStorageKeys[index+1:])
					vm.localStorageKeys = vm.localStorageKeys[:len(vm.localStorageKeys)-1]
					break
				}
			}
		}
		return conversationTurnstileUndefined, true, nil
	case "window.localStorage.clear":
		vm.localStorageKeys = nil
		vm.localStorageValues = make(map[string]string)
		return conversationTurnstileUndefined, true, nil
	case "window.localStorage.key":
		index := 0
		if len(args) > 0 {
			value, ok, err := vm.number(args[0])
			if err != nil {
				return nil, true, err
			}
			if ok && !math.IsNaN(value) && !math.IsInf(value, 0) {
				index = int(value)
			}
		}
		if index < 0 || index >= len(vm.localStorageKeys) {
			return conversationTurnstileExplicitNull, true, nil
		}
		return vm.localStorageKeys[index], true, nil
	default:
		return nil, false, nil
	}
}

func (vm *conversationTurnstileVM) storageArgument(args []any, index int) (string, error) {
	value := any(conversationTurnstileUndefined)
	if index >= 0 && index < len(args) {
		value = args[index]
	}
	return vm.runtimeString(value)
}

func (vm *conversationTurnstileVM) browserObjectRefProperty(path, key string) (any, bool, error) {
	if path != "window.localStorage" {
		return nil, false, nil
	}
	switch key {
	case "length":
		return len(vm.localStorageKeys), true, nil
	case "getItem", "setItem", "removeItem", "clear", "key":
		return conversationTurnstileObjectRef{path: path + "." + key}, true, nil
	default:
		value, exists := vm.localStorageValues[key]
		if !exists {
			return nil, false, nil
		}
		return value, true, nil
	}
}

func (vm *conversationTurnstileVM) browserProperty(object any, key string) (any, bool, error) {
	switch value := object.(type) {
	case *conversationTurnstileDOMElement:
		if value == nil {
			return conversationTurnstileUndefined, true, nil
		}
		switch key {
		case "nodeType":
			return 1, true, nil
		case "nodeName", "tagName":
			return value.tagName, true, nil
		case "style":
			return value.style, true, nil
		case "children":
			if err := vm.reserveRuntimeBytes(24 + len(value.children)*16); err != nil {
				return nil, true, err
			}
			return conversationTurnstileArrayValue(append([]any(nil), value.children...)), true, nil
		case "width":
			if value.tagName == "CANVAS" {
				return 300, true, nil
			}
		case "height":
			if value.tagName == "CANVAS" {
				return 150, true, nil
			}
		case "getContext":
			callable, err := vm.browserCallable(func(args []any) (any, error) {
				kind := "2d"
				if len(args) > 0 {
					parsed, parseErr := vm.runtimeString(args[0])
					if parseErr != nil {
						return nil, parseErr
					}
					kind = strings.ToLower(strings.TrimSpace(parsed))
				}
				switch kind {
				case "2d", "webgl", "experimental-webgl", "webgl2":
					if err := vm.reserveRuntimeBytes(128 + len(kind)); err != nil {
						return nil, err
					}
					return &conversationTurnstileCanvasContext{vm: vm, canvas: value, kind: kind}, nil
				default:
					return conversationTurnstileExplicitNull, nil
				}
			})
			return callable, true, err
		case "appendChild":
			callable, err := vm.browserCallable(func(args []any) (any, error) {
				child := conversationTurnstileArgument(args, 0)
				if err := vm.reserveRuntimeBytes(16); err != nil {
					return nil, err
				}
				value.children = append(value.children, child)
				return child, nil
			})
			return callable, true, err
		case "removeChild":
			callable, err := vm.browserCallable(func(args []any) (any, error) {
				child := conversationTurnstileArgument(args, 0)
				for index, candidate := range value.children {
					equal, equalErr := vm.strictEqual(candidate, child)
					if equalErr != nil {
						return nil, equalErr
					}
					if equal {
						copy(value.children[index:], value.children[index+1:])
						value.children = value.children[:len(value.children)-1]
						break
					}
				}
				return child, nil
			})
			return callable, true, err
		case "setAttribute":
			callable, err := vm.browserCallable(func(args []any) (any, error) {
				name, parseErr := vm.storageArgument(args, 0)
				if parseErr != nil {
					return nil, parseErr
				}
				attributeValue, parseErr := vm.storageArgument(args, 1)
				if parseErr != nil {
					return nil, parseErr
				}
				if err := vm.reserveRuntimeBytes(len(name) + len(attributeValue)); err != nil {
					return nil, err
				}
				value.attributes[name] = attributeValue
				return conversationTurnstileUndefined, nil
			})
			return callable, true, err
		case "getAttribute":
			callable, err := vm.browserCallable(func(args []any) (any, error) {
				name, parseErr := vm.storageArgument(args, 0)
				if parseErr != nil {
					return nil, parseErr
				}
				attributeValue, exists := value.attributes[name]
				if !exists {
					return conversationTurnstileExplicitNull, nil
				}
				return attributeValue, nil
			})
			return callable, true, err
		case "addEventListener", "removeEventListener":
			callable, err := vm.browserCallable(func([]any) (any, error) { return conversationTurnstileUndefined, nil })
			return callable, true, err
		case "dispatchEvent":
			callable, err := vm.browserCallable(func([]any) (any, error) { return true, nil })
			return callable, true, err
		case "getBoundingClientRect":
			callable, err := vm.browserCallable(func([]any) (any, error) {
				if err := vm.reserveRuntimeBytes(512); err != nil {
					return nil, err
				}
				rectangle := newConversationTurnstileOrderedMap()
				for _, property := range []string{"x", "y", "width", "height", "top", "right", "bottom", "left"} {
					rectangle.set(property, 0)
				}
				return rectangle, nil
			})
			return callable, true, err
		}
		if attribute, exists := value.attributes[key]; exists {
			return attribute, true, nil
		}
		return conversationTurnstileUndefined, false, nil
	case *conversationTurnstileCanvasContext:
		if value == nil {
			return conversationTurnstileUndefined, true, nil
		}
		switch key {
		case "canvas":
			return value.canvas, true, nil
		case "getExtension":
			callable, err := vm.browserCallable(func(args []any) (any, error) {
				name, parseErr := vm.storageArgument(args, 0)
				if parseErr != nil {
					return nil, parseErr
				}
				if strings.EqualFold(strings.TrimSpace(name), "WEBGL_debug_renderer_info") {
					if err := vm.reserveRuntimeBytes(64); err != nil {
						return nil, err
					}
					return &conversationTurnstileWebGLExtension{}, nil
				}
				return conversationTurnstileExplicitNull, nil
			})
			return callable, true, err
		case "getParameter":
			callable, err := vm.browserCallable(func(args []any) (any, error) {
				parameter := float64(0)
				if len(args) > 0 {
					parsed, _, parseErr := vm.number(args[0])
					if parseErr != nil {
						return nil, parseErr
					}
					parameter = parsed
				}
				switch int(parameter) {
				case conversationTurnstileWebGLVendor:
					return "WebKit", nil
				case conversationTurnstileWebGLRenderer:
					return "WebKit WebGL", nil
				case conversationTurnstileWebGLVersion:
					return "WebGL 1.0 (OpenGL ES 2.0 Chromium)", nil
				case conversationTurnstileWebGLShadingLanguageVersion:
					return "WebGL GLSL ES 1.0 (OpenGL ES GLSL ES 1.0 Chromium)", nil
				case conversationTurnstileWebGLUnmaskedVendor:
					return vm.browserProfile.webGLVendor, nil
				case conversationTurnstileWebGLUnmaskedRenderer:
					return vm.browserProfile.webGLRenderer, nil
				default:
					return 0, nil
				}
			})
			return callable, true, err
		case "fillRect", "clearRect", "drawImage", "save", "restore":
			callable, err := vm.browserCallable(func([]any) (any, error) { return conversationTurnstileUndefined, nil })
			return callable, true, err
		case "measureText":
			callable, err := vm.browserCallable(func(args []any) (any, error) {
				text := ""
				if len(args) > 0 {
					parsed, parseErr := vm.runtimeString(args[0])
					if parseErr != nil {
						return nil, parseErr
					}
					text = parsed
				}
				if err := vm.reserveRuntimeBytes(128); err != nil {
					return nil, err
				}
				measurement := newConversationTurnstileOrderedMap()
				measurement.set("width", float64(len(text))*7.5)
				return measurement, nil
			})
			return callable, true, err
		}
		return conversationTurnstileUndefined, false, nil
	case *conversationTurnstileWebGLExtension:
		switch key {
		case "UNMASKED_VENDOR_WEBGL":
			return conversationTurnstileWebGLUnmaskedVendor, true, nil
		case "UNMASKED_RENDERER_WEBGL":
			return conversationTurnstileWebGLUnmaskedRenderer, true, nil
		}
		return conversationTurnstileUndefined, false, nil
	default:
		return nil, false, nil
	}
}

func (vm *conversationTurnstileVM) knownEnvironmentProperty(object, propertyKey any) bool {
	objectRef, ok := object.(conversationTurnstileObjectRef)
	if !ok {
		return false
	}
	key, err := vm.runtimeString(propertyKey)
	if err != nil {
		vm.fail(err)
		return false
	}
	_, exists := vm.environment[objectRef.path+"."+key]
	return exists
}

func conversationTurnstileBrowserNativeObjectPath(path string) bool {
	switch path {
	case "window.document.createElement", "window.document.createElementNS",
		"window.document.querySelector", "window.document.querySelectorAll",
		"window.document.getElementById", "window.document.getElementsByTagName",
		"window.document.addEventListener", "window.document.removeEventListener", "window.document.dispatchEvent",
		"window.localStorage.getItem", "window.localStorage.setItem", "window.localStorage.removeItem",
		"window.localStorage.clear", "window.localStorage.key":
		return true
	default:
		return false
	}
}

func conversationTurnstileBrowserObjectString(path string) (string, bool) {
	if conversationTurnstileBrowserNativeObjectPath(path) {
		name := path[strings.LastIndex(path, ".")+1:]
		return "function " + name + "() { [native code] }", true
	}
	switch path {
	case "window.document":
		return "[object HTMLDocument]", true
	case "window.screen":
		return "[object Screen]", true
	case "window.navigator":
		return "[object Navigator]", true
	default:
		return "", false
	}
}

func conversationTurnstileBrowserObjectValueString(object conversationTurnstileBrowserObject) string {
	if object == nil {
		return "[object Object]"
	}
	path := object.conversationTurnstileBrowserPath()
	switch {
	case strings.Contains(path, "CanvasRenderingContext2D"):
		return "[object CanvasRenderingContext2D]"
	case strings.Contains(path, "WebGLRenderingContext"):
		return "[object WebGLRenderingContext]"
	case strings.Contains(path, "WEBGL_debug_renderer_info"):
		return "[object Object]"
	default:
		return "[object HTMLElement]"
	}
}
