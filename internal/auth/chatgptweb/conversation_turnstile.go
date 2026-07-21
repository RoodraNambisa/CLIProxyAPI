package chatgptweb

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	defaultConversationTurnstileMaxSteps   = 20_000
	conversationTurnstileMaxBytes          = 4 << 20
	conversationTurnstileMaxQueueDepth     = 64
	conversationTurnstileMaxValueBytes     = 8 << 20
	conversationTurnstileMaxRuntimeBytes   = 32 << 20
	conversationTurnstileMaxJSONDepth      = 128
	conversationTurnstileMaxJSONNodes      = 131_072
	conversationTurnstileMaxRegexpBytes    = 64 << 10
	conversationTurnstileMaxRuntimeWork    = 64 << 20
	conversationTurnstileMaxPrototypeDepth = 256
)

var conversationTurnstileLocalStorageKeys = []string{
	"STATSIG_LOCAL_STORAGE_INTERNAL_STORE_V4",
	"STATSIG_LOCAL_STORAGE_STABLE_ID",
	"client-correlated-secret",
	"oai/apps/capExpiresAt",
	"oai-did",
	"STATSIG_LOCAL_STORAGE_LOGGING_REQUEST",
	"UiState.isNavigationCollapsed.1",
}

type conversationTurnstileUndefinedValue struct{}

type conversationTurnstileExplicitNullValue struct{}

var conversationTurnstileUndefined = conversationTurnstileUndefinedValue{}
var conversationTurnstileExplicitNull = conversationTurnstileExplicitNullValue{}

type conversationTurnstileCallable struct {
	identity *struct{ marker byte }
	call     func([]any) (any, error)
}

func newConversationTurnstileCallable(call func([]any) (any, error)) conversationTurnstileCallable {
	return conversationTurnstileCallable{identity: &struct{ marker byte }{}, call: call}
}

func (callable conversationTurnstileCallable) invoke(args []any) (any, error) {
	if callable.call == nil {
		return conversationTurnstileUndefined, nil
	}
	return callable.call(args)
}

type conversationTurnstileJSString struct {
	units []uint16
}

type conversationTurnstileObjectRef struct {
	path string
}

type conversationTurnstileArray struct {
	items   []any
	present []bool
}

func (array *conversationTurnstileArray) has(index int) bool {
	if array == nil || index < 0 || index >= len(array.items) {
		return false
	}
	return array.present == nil || array.present[index]
}

func (array *conversationTurnstileArray) append(value any) {
	if array == nil {
		return
	}
	array.items = append(array.items, value)
	if array.present != nil {
		array.present = append(array.present, true)
	}
}

func (array *conversationTurnstileArray) remove(index int) {
	if array == nil || index < 0 || index >= len(array.items) {
		return
	}
	copy(array.items[index:], array.items[index+1:])
	array.items[len(array.items)-1] = nil
	array.items = array.items[:len(array.items)-1]
	if array.present != nil {
		copy(array.present[index:], array.present[index+1:])
		array.present = array.present[:len(array.present)-1]
	}
}

type conversationTurnstileBoxedPrimitive struct {
	value any
}

type conversationTurnstileLocationRef struct {
	href     string
	origin   string
	pathname string
	search   string
}

// ConversationTurnstileEnvironment supplies the browser values observed by a
// compact Sentinel challenge. ScriptSources should come from the same bootstrap
// document used to create the requirements token.
type ConversationTurnstileEnvironment struct {
	Persona          Persona
	ScriptSources    []string
	Location         string
	LocalStorageKeys []string
}

type conversationTurnstileOrderedMap struct {
	keys         []any
	values       map[string]any
	prototype    any
	prototypeSet bool
}

func newConversationTurnstileOrderedMap() *conversationTurnstileOrderedMap {
	return &conversationTurnstileOrderedMap{values: make(map[string]any)}
}

func (ordered *conversationTurnstileOrderedMap) set(key, value any) {
	if ordered == nil {
		return
	}
	propertyKey, mapKey := conversationTurnstilePropertyKey(key)
	ordered.setWithMapKey(propertyKey, mapKey, value)
}

func (ordered *conversationTurnstileOrderedMap) setWithMapKey(propertyKey any, mapKey string, value any) {
	if ordered == nil {
		return
	}
	if ordered.values == nil {
		ordered.values = make(map[string]any)
	}
	if _, exists := ordered.values[mapKey]; !exists {
		ordered.keys = append(ordered.keys, propertyKey)
	}
	ordered.values[mapKey] = value
}

func (ordered *conversationTurnstileOrderedMap) get(key any) (any, bool) {
	if ordered == nil {
		return nil, false
	}
	_, mapKey := conversationTurnstilePropertyKey(key)
	value, exists := ordered.values[mapKey]
	return value, exists
}

func (ordered *conversationTurnstileOrderedMap) jsKeys(ctx context.Context, reserve, charge func(int) error) ([]any, error) {
	if ordered == nil || len(ordered.keys) == 0 {
		return nil, nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if charge != nil {
		if err := charge(len(ordered.keys) * 32); err != nil {
			return nil, err
		}
		for index, key := range ordered.keys {
			if index&255 == 0 && ctx != nil {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			if err := charge(conversationTurnstileStringStorageSize(key)); err != nil {
				return nil, err
			}
		}
	}
	if reserve != nil {
		if err := reserve(len(ordered.keys) * 16); err != nil {
			return nil, err
		}
	}
	keys := append([]any(nil), ordered.keys...)
	sort.SliceStable(keys, func(left, right int) bool {
		leftIndex, leftIsIndex := conversationTurnstileArrayIndexKey(keys[left])
		rightIndex, rightIsIndex := conversationTurnstileArrayIndexKey(keys[right])
		if leftIsIndex != rightIsIndex {
			return leftIsIndex
		}
		return leftIsIndex && leftIndex < rightIndex
	})
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func conversationTurnstileArrayIndexKey(value any) (uint32, bool) {
	var key string
	switch typed := value.(type) {
	case string:
		key = typed
	case conversationTurnstileJSString:
		if len(typed.units) == 0 || len(typed.units) > 10 || len(typed.units) > 1 && typed.units[0] == '0' {
			return 0, false
		}
		var parsed uint64
		for _, unit := range typed.units {
			if unit < '0' || unit > '9' {
				return 0, false
			}
			parsed = parsed*10 + uint64(unit-'0')
			if parsed >= math.MaxUint32 {
				return 0, false
			}
		}
		return uint32(parsed), true
	default:
		return 0, false
	}
	if key == "" || len(key) > 1 && key[0] == '0' {
		return 0, false
	}
	parsed, err := strconv.ParseUint(key, 10, 32)
	if err != nil || parsed >= math.MaxUint32 || strconv.FormatUint(parsed, 10) != key {
		return 0, false
	}
	return uint32(parsed), true
}

func conversationTurnstileSortStringKeys(keys []string) {
	sort.Slice(keys, func(left, right int) bool {
		leftIndex, leftIsIndex := conversationTurnstileArrayIndexKey(keys[left])
		rightIndex, rightIsIndex := conversationTurnstileArrayIndexKey(keys[right])
		if leftIsIndex != rightIsIndex {
			return leftIsIndex
		}
		if leftIsIndex {
			return leftIndex < rightIndex
		}
		return keys[left] < keys[right]
	})
}

type conversationTurnstileProcessMapRef struct {
	vm *conversationTurnstileVM
}

type conversationTurnstileFatalError struct {
	message string
}

func (err conversationTurnstileFatalError) Error() string {
	return err.message
}

type conversationTurnstileJSError struct {
	name    string
	message string
}

type conversationTurnstileThrownValue struct {
	text string
}

func (err conversationTurnstileThrownValue) Error() string {
	return err.text
}

func (err conversationTurnstileJSError) Error() string {
	name := strings.TrimSpace(err.name)
	if name == "" {
		name = "Error"
	}
	message := strings.TrimSpace(err.message)
	if message == "" {
		return name
	}
	return name + ": " + message
}

func conversationTurnstileTypeError(message string) error {
	return conversationTurnstileJSError{name: "TypeError", message: message}
}

func conversationTurnstileErrorString(err error) string {
	if err == nil {
		return ""
	}
	var jsError conversationTurnstileJSError
	if errors.As(err, &jsError) {
		return jsError.Error()
	}
	var thrown conversationTurnstileThrownValue
	if errors.As(err, &thrown) {
		return thrown.Error()
	}
	return conversationTurnstileJSError{name: "Error", message: err.Error()}.Error()
}

func conversationTurnstileFatalInstructionError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	var fatal conversationTurnstileFatalError
	if errors.As(err, &fatal) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

type conversationTurnstileMemoryBudget struct {
	used int
}

func (budget *conversationTurnstileMemoryBudget) reserve(size int) error {
	if size < 0 || size > conversationTurnstileMaxValueBytes {
		return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile value exceeds %d bytes", conversationTurnstileMaxValueBytes)}
	}
	if budget == nil {
		return conversationTurnstileFatalError{message: "conversation turnstile memory budget is unavailable"}
	}
	if budget.used > conversationTurnstileMaxRuntimeBytes-size {
		return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile runtime allocation exceeds %d bytes", conversationTurnstileMaxRuntimeBytes)}
	}
	budget.used += size
	return nil
}

type conversationTurnstileExecutionBudget struct {
	steps       int
	maxSteps    int
	runtimeWork int
}

type conversationTurnstileVM struct {
	ctx                  context.Context
	values               map[string]any
	result               string
	reader               io.Reader
	now                  func() time.Time
	startedAt            time.Time
	depth                int
	queueDepth           int
	environment          map[string]any
	scriptSources        []string
	localStorageKeys     []string
	requirementsToken    string
	challengeEnvironment ConversationTurnstileEnvironment
	memoryBudget         *conversationTurnstileMemoryBudget
	executionBudget      *conversationTurnstileExecutionBudget
	regexpCache          map[string]*regexp.Regexp
	fatalErr             error
	processMap           *conversationTurnstileProcessMapRef
	settled              bool
	deferred             bool
	instructionCount     int
}

// BuildConversationTurnstileToken executes the compact Sentinel VM challenge
// returned in turnstile.dx using the requirements token sent to prepare.
func BuildConversationTurnstileToken(ctx context.Context, dx, requirementsToken string, reader io.Reader, now func() time.Time) (string, error) {
	return BuildConversationTurnstileTokenWithEnvironment(ctx, dx, requirementsToken, ConversationTurnstileEnvironment{}, reader, now)
}

// BuildConversationTurnstileTokenWithEnvironment executes a compact Sentinel
// challenge with the persona and script list used for the upstream request.
func BuildConversationTurnstileTokenWithEnvironment(ctx context.Context, dx, requirementsToken string, environment ConversationTurnstileEnvironment, reader io.Reader, now func() time.Time) (string, error) {
	return buildConversationTurnstileTokenWithEnvironment(ctx, dx, requirementsToken, environment, reader, now, defaultConversationTurnstileMaxSteps, 0, nil, nil)
}

func buildConversationTurnstileToken(ctx context.Context, dx, requirementsToken string, reader io.Reader, now func() time.Time, maxSteps int) (string, error) {
	return buildConversationTurnstileTokenWithEnvironment(ctx, dx, requirementsToken, ConversationTurnstileEnvironment{}, reader, now, maxSteps, 0, nil, nil)
}

func buildConversationTurnstileTokenWithEnvironment(ctx context.Context, dx, requirementsToken string, environment ConversationTurnstileEnvironment, reader io.Reader, now func() time.Time, maxSteps, depth int, memoryBudget *conversationTurnstileMemoryBudget, executionBudget *conversationTurnstileExecutionBudget) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(dx) > base64.StdEncoding.EncodedLen(conversationTurnstileMaxBytes) {
		return "", fmt.Errorf("decode conversation turnstile challenge: payload exceeds %d bytes", conversationTurnstileMaxBytes)
	}
	if len(requirementsToken) > conversationTurnstileMaxValueBytes {
		return "", fmt.Errorf("conversation turnstile requirements token exceeds %d bytes", conversationTurnstileMaxValueBytes)
	}
	dx = strings.TrimSpace(dx)
	requirementsToken = strings.TrimSpace(requirementsToken)
	if dx == "" || requirementsToken == "" {
		return "", fmt.Errorf("invalid conversation turnstile challenge")
	}
	decoded, err := decodeConversationTurnstileDXContext(ctx, dx)
	if err != nil {
		return "", err
	}
	if memoryBudget == nil {
		memoryBudget = &conversationTurnstileMemoryBudget{}
	}
	programData, err := xorConversationTurnstileBytesContext(ctx, decoded, []byte(requirementsToken))
	if err != nil {
		return "", err
	}
	programValue, err := decodeConversationTurnstileJSONWithContextAndBudget(ctx, programData, memoryBudget)
	if err != nil {
		return "", fmt.Errorf("decode conversation turnstile program: %w", err)
	}
	program, ok := conversationTurnstileSlice(programValue)
	if !ok {
		return "", fmt.Errorf("decode conversation turnstile program: root is not an array")
	}
	if now == nil {
		now = time.Now
	}
	if maxSteps <= 0 {
		maxSteps = defaultConversationTurnstileMaxSteps
	}
	if executionBudget == nil {
		executionBudget = &conversationTurnstileExecutionBudget{maxSteps: maxSteps}
	} else if executionBudget.maxSteps <= 0 {
		executionBudget.maxSteps = maxSteps
	}
	vm := &conversationTurnstileVM{
		ctx:                  ctx,
		values:               make(map[string]any),
		reader:               randomReader(reader),
		now:                  now,
		startedAt:            now(),
		depth:                depth,
		requirementsToken:    requirementsToken,
		challengeEnvironment: environment,
		memoryBudget:         memoryBudget,
		executionBudget:      executionBudget,
		regexpCache:          make(map[string]*regexp.Regexp),
	}
	vm.environment, vm.scriptSources, vm.localStorageKeys = normalizeConversationTurnstileEnvironment(environment, vm.startedAt)
	vm.initialize(program, requirementsToken)
	if err = vm.runQueue(); err != nil {
		if fatalErr := conversationTurnstileFatalInstructionError(ctx, err); fatalErr != nil {
			return "", fatalErr
		}
		message := strconv.Itoa(vm.instructionCount) + ": " + conversationTurnstileErrorString(err)
		encoded, encodeErr := vm.base64EncodeBinaryString(message)
		if encodeErr != nil {
			return "", encodeErr
		}
		return encoded, nil
	}
	if strings.TrimSpace(vm.result) == "" {
		return strconv.Itoa(vm.instructionCount), nil
	}
	return vm.result, nil
}

func normalizeConversationTurnstileEnvironment(environment ConversationTurnstileEnvironment, startedAt time.Time) (map[string]any, []string, []string) {
	persona := normalizePersona(environment.Persona)
	location := strings.TrimSpace(environment.Location)
	if location == "" {
		location = "https://chatgpt.com/"
	}
	locationOrigin, locationPath, locationSearch := conversationTurnstileLocationParts(location)
	locationRef := &conversationTurnstileLocationRef{
		href:     location,
		origin:   locationOrigin,
		pathname: locationPath,
		search:   locationSearch,
	}
	sources := append([]string(nil), environment.ScriptSources...)
	if len(sources) == 0 {
		sources = []string{defaultConversationPoWScript}
	}
	storageKeys := append([]string(nil), environment.LocalStorageKeys...)
	if len(storageKeys) == 0 {
		storageKeys = append([]string(nil), conversationTurnstileLocalStorageKeys...)
	}
	languages := conversationTurnstileArrayValue([]any{persona.Language})
	values := map[string]any{
		"window.navigator.userAgent":                persona.UserAgent,
		"window.navigator.language":                 persona.Language,
		"window.navigator.languages":                languages,
		"window.navigator.hardwareConcurrency":      persona.HardwareConcurrency,
		"window.navigator.platform":                 persona.Platform,
		"window.navigator.vendor":                   "Google Inc.",
		"window.navigator.webdriver":                false,
		"window.navigator.cookieEnabled":            true,
		"window.navigator.onLine":                   true,
		"window.screen.width":                       persona.ScreenWidth,
		"window.screen.height":                      persona.ScreenHeight,
		"window.screen.availWidth":                  persona.ScreenWidth,
		"window.screen.availHeight":                 persona.ScreenHeight,
		"window.screen.colorDepth":                  24,
		"window.screen.pixelDepth":                  24,
		"window.innerWidth":                         persona.ScreenWidth,
		"window.innerHeight":                        persona.ScreenHeight,
		"window.devicePixelRatio":                   2,
		"window.location":                           locationRef,
		"window.location.href":                      location,
		"window.location.origin":                    locationOrigin,
		"window.location.pathname":                  locationPath,
		"window.location.search":                    locationSearch,
		"window.document.URL":                       location,
		"window.document.location":                  locationRef,
		"window.document.referrer":                  "https://auth.openai.com/",
		"window.document.readyState":                "complete",
		"window.document.hidden":                    false,
		"window.document.visibilityState":           "visible",
		"window.history.length":                     1,
		"window.localStorage.length":                len(storageKeys),
		"window.performance.timeOrigin":             float64(startedAt.UnixNano()) / float64(time.Millisecond),
		"window.performance.memory.jsHeapSizeLimit": float64(4_294_967_296),
	}
	return values, sources, storageKeys
}

func conversationTurnstileLocationParts(location string) (string, string, string) {
	parsed, err := url.Parse(strings.TrimSpace(location))
	if err != nil || parsed == nil || parsed.Scheme == "" || parsed.Host == "" {
		return strings.TrimSuffix(location, "/"), "/", ""
	}
	pathname := parsed.EscapedPath()
	if pathname == "" {
		pathname = "/"
	}
	origin := parsed.Scheme + "://" + parsed.Host
	search := ""
	if parsed.RawQuery != "" {
		search = "?" + parsed.RawQuery
	}
	return origin, pathname, search
}

func decodeConversationTurnstileDX(value string) ([]byte, error) {
	return decodeConversationTurnstileDXContext(context.Background(), value)
}

func decodeConversationTurnstileDXContext(ctx context.Context, value string) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if base64.StdEncoding.DecodedLen(len(value)) > conversationTurnstileMaxBytes {
		return nil, fmt.Errorf("decode conversation turnstile challenge: payload exceeds %d bytes", conversationTurnstileMaxBytes)
	}
	decoded, err := decodeConversationTurnstileBase64(ctx, base64.StdEncoding, value)
	if err == nil {
		if len(decoded) > conversationTurnstileMaxBytes {
			return nil, fmt.Errorf("decode conversation turnstile challenge: payload exceeds %d bytes", conversationTurnstileMaxBytes)
		}
		return decoded, nil
	}
	decoded, rawErr := decodeConversationTurnstileBase64(ctx, base64.RawStdEncoding, value)
	if rawErr != nil {
		return nil, fmt.Errorf("decode conversation turnstile challenge: %w", err)
	}
	if len(decoded) > conversationTurnstileMaxBytes {
		return nil, fmt.Errorf("decode conversation turnstile challenge: payload exceeds %d bytes", conversationTurnstileMaxBytes)
	}
	return decoded, nil
}

func decodeConversationTurnstileBase64(ctx context.Context, encoding *base64.Encoding, value string) ([]byte, error) {
	const chunkSize = 16 << 10
	decoded := make([]byte, encoding.DecodedLen(len(value)))
	written := 0
	for offset := 0; offset < len(value); {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := min(offset+chunkSize, len(value))
		count, err := encoding.Decode(decoded[written:], []byte(value[offset:end]))
		written += count
		if err != nil {
			return nil, err
		}
		offset = end
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return decoded[:written], nil
}

func xorConversationTurnstileBytes(value, key []byte) []byte {
	out, _ := xorConversationTurnstileBytesContext(context.Background(), value, key)
	return out
}

func xorConversationTurnstileBytesContext(ctx context.Context, value, key []byte) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	out := make([]byte, len(value))
	for index := range value {
		if index%(16<<10) == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		out[index] = value[index]
		if len(key) > 0 {
			out[index] ^= key[index%len(key)]
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeConversationTurnstileJSON(payload []byte) (any, error) {
	return decodeConversationTurnstileJSONWithBudget(payload, &conversationTurnstileMemoryBudget{})
}

type conversationTurnstileJSONDecodeState struct {
	ctx          context.Context
	memoryBudget *conversationTurnstileMemoryBudget
	payload      []byte
	offset       int
	nodes        int
	checkedAt    int
	checkedNodes int
}

func decodeConversationTurnstileJSONWithBudget(payload []byte, memoryBudget *conversationTurnstileMemoryBudget) (any, error) {
	return decodeConversationTurnstileJSONWithContextAndBudget(context.Background(), payload, memoryBudget)
}

func decodeConversationTurnstileJSONWithContextAndBudget(ctx context.Context, payload []byte, memoryBudget *conversationTurnstileMemoryBudget) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if memoryBudget == nil {
		memoryBudget = &conversationTurnstileMemoryBudget{}
	}
	state := &conversationTurnstileJSONDecodeState{ctx: ctx, memoryBudget: memoryBudget, payload: payload}
	value, err := state.parseValue(0)
	if err != nil {
		return nil, err
	}
	if err = state.skipWhitespace(); err != nil {
		return nil, err
	}
	if state.offset != len(state.payload) {
		return nil, fmt.Errorf("multiple JSON values at byte %d", state.offset)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return value, nil
}

func (state *conversationTurnstileJSONDecodeState) parseValue(depth int) (any, error) {
	if depth > conversationTurnstileMaxJSONDepth {
		return nil, conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile JSON exceeds depth %d", conversationTurnstileMaxJSONDepth)}
	}
	state.nodes++
	if state.nodes > conversationTurnstileMaxJSONNodes {
		return nil, conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile JSON exceeds %d nodes", conversationTurnstileMaxJSONNodes)}
	}
	if err := state.checkContextAt(state.offset, false); err != nil {
		return nil, err
	}
	if err := state.skipWhitespace(); err != nil {
		return nil, err
	}
	if state.offset >= len(state.payload) {
		return nil, io.ErrUnexpectedEOF
	}
	switch state.payload[state.offset] {
	case '[':
		return state.parseArray(depth)
	case '{':
		return state.parseObject(depth)
	case '"':
		return state.parseString()
	default:
		return state.parseScalar()
	}
}

func (state *conversationTurnstileJSONDecodeState) parseArray(depth int) (any, error) {
	if err := state.memoryBudget.reserve(24); err != nil {
		return nil, err
	}
	state.offset++
	array := &conversationTurnstileArray{items: make([]any, 0, 1)}
	if err := state.skipWhitespace(); err != nil {
		return nil, err
	}
	if state.consumeByte(']') {
		return array, nil
	}
	for {
		item, err := state.parseValue(depth + 1)
		if err != nil {
			return nil, err
		}
		if err = state.memoryBudget.reserve(16); err != nil {
			return nil, err
		}
		array.items = append(array.items, item)
		if err = state.skipWhitespace(); err != nil {
			return nil, err
		}
		if state.consumeByte(']') {
			return array, nil
		}
		if !state.consumeByte(',') {
			return nil, state.syntaxError("expected ',' or ']'")
		}
	}
}

func (state *conversationTurnstileJSONDecodeState) parseObject(depth int) (any, error) {
	if err := state.memoryBudget.reserve(128); err != nil {
		return nil, err
	}
	state.offset++
	object := newConversationTurnstileOrderedMap()
	if err := state.skipWhitespace(); err != nil {
		return nil, err
	}
	if state.consumeByte('}') {
		return object, nil
	}
	for {
		if err := state.skipWhitespace(); err != nil {
			return nil, err
		}
		if state.offset >= len(state.payload) || state.payload[state.offset] != '"' {
			return nil, state.syntaxError("expected object key")
		}
		key, err := state.parseString()
		if err != nil {
			return nil, err
		}
		if err = state.memoryBudget.reserve(48); err != nil {
			return nil, err
		}
		if err = state.skipWhitespace(); err != nil {
			return nil, err
		}
		if !state.consumeByte(':') {
			return nil, state.syntaxError("expected ':'")
		}
		value, err := state.parseValue(depth + 1)
		if err != nil {
			return nil, err
		}
		object.set(key, value)
		if err = state.skipWhitespace(); err != nil {
			return nil, err
		}
		if state.consumeByte('}') {
			return object, nil
		}
		if !state.consumeByte(',') {
			return nil, state.syntaxError("expected ',' or '}'")
		}
	}
}

func (state *conversationTurnstileJSONDecodeState) parseString() (any, error) {
	end, err := state.scanString(state.offset)
	if err != nil {
		return nil, err
	}
	rawSize := end - state.offset
	if err = state.memoryBudget.reserve(16); err != nil {
		return nil, err
	}
	if err = state.memoryBudget.reserve(rawSize * 2); err != nil {
		return nil, err
	}
	if rawSize >= 4<<10 {
		if err = state.checkContextAt(state.offset, true); err != nil {
			return nil, err
		}
	}
	units := make([]uint16, 0, rawSize)
	for index := state.offset + 1; index < end-1; {
		if err = state.checkContextAt(index, false); err != nil {
			return nil, err
		}
		character := state.payload[index]
		if character == '\\' {
			index++
			escaped := state.payload[index]
			index++
			switch escaped {
			case '"', '\\', '/':
				units = append(units, uint16(escaped))
			case 'b':
				units = append(units, '\b')
			case 'f':
				units = append(units, '\f')
			case 'n':
				units = append(units, '\n')
			case 'r':
				units = append(units, '\r')
			case 't':
				units = append(units, '\t')
			case 'u':
				unit, ok := parseConversationTurnstileHexUnit(state.payload[index : index+4])
				if !ok {
					return nil, state.syntaxError("invalid unicode escape")
				}
				units = append(units, unit)
				index += 4
			}
			continue
		}
		runeValue, width := utf8.DecodeRune(state.payload[index : end-1])
		if runeValue == utf8.RuneError && width == 1 {
			return nil, state.syntaxError("invalid UTF-8 in string")
		}
		if runeValue <= 0xffff {
			units = append(units, uint16(runeValue))
		} else {
			high, low := utf16.EncodeRune(runeValue)
			units = append(units, uint16(high), uint16(low))
		}
		index += width
	}
	state.offset = end
	if conversationTurnstileHasIsolatedSurrogate(units) {
		return conversationTurnstileJSString{units: units}, nil
	}
	return string(utf16.Decode(units)), nil
}

func (state *conversationTurnstileJSONDecodeState) parseScalar() (any, error) {
	start := state.offset
	for state.offset < len(state.payload) {
		if err := state.checkContextAt(state.offset, false); err != nil {
			return nil, err
		}
		switch state.payload[state.offset] {
		case ' ', '\t', '\r', '\n', ',', ']', '}':
			goto parsed
		default:
			state.offset++
		}
	}
parsed:
	if state.offset == start {
		return nil, state.syntaxError("expected JSON value")
	}
	raw := state.payload[start:state.offset]
	if err := state.memoryBudget.reserve(16 + len(raw)); err != nil {
		return nil, err
	}
	switch string(raw) {
	case "null":
		return nil, nil
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	if !json.Valid(raw) {
		return nil, state.syntaxError("invalid JSON scalar")
	}
	parsed, _ := strconv.ParseFloat(string(raw), 64)
	return parsed, nil
}

func (state *conversationTurnstileJSONDecodeState) skipWhitespace() error {
	for state.offset < len(state.payload) {
		if err := state.checkContextAt(state.offset, false); err != nil {
			return err
		}
		switch state.payload[state.offset] {
		case ' ', '\t', '\r', '\n':
			state.offset++
		default:
			return nil
		}
	}
	return nil
}

func (state *conversationTurnstileJSONDecodeState) checkContextAt(offset int, force bool) error {
	if state.ctx == nil {
		return nil
	}
	if !force && offset-state.checkedAt < 4<<10 && state.nodes-state.checkedNodes < 256 {
		return nil
	}
	state.checkedAt = offset
	state.checkedNodes = state.nodes
	return state.ctx.Err()
}

func (state *conversationTurnstileJSONDecodeState) consumeByte(expected byte) bool {
	if state.offset >= len(state.payload) || state.payload[state.offset] != expected {
		return false
	}
	state.offset++
	return true
}

func (state *conversationTurnstileJSONDecodeState) syntaxError(message string) error {
	return fmt.Errorf("%s at byte %d", message, state.offset)
}

func (state *conversationTurnstileJSONDecodeState) scanString(start int) (int, error) {
	if start >= len(state.payload) || state.payload[start] != '"' {
		return 0, fmt.Errorf("expected JSON string at byte %d", start)
	}
	for index := start + 1; index < len(state.payload); {
		if err := state.checkContextAt(index, false); err != nil {
			return 0, err
		}
		switch state.payload[index] {
		case '"':
			return index + 1, nil
		case '\\':
			index++
			if index >= len(state.payload) {
				return 0, io.ErrUnexpectedEOF
			}
			switch state.payload[index] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				index++
			case 'u':
				if index+4 >= len(state.payload) {
					return 0, io.ErrUnexpectedEOF
				}
				if _, ok := parseConversationTurnstileHexUnit(state.payload[index+1 : index+5]); !ok {
					return 0, fmt.Errorf("invalid unicode escape at byte %d", index)
				}
				index += 5
			default:
				return 0, fmt.Errorf("invalid escape at byte %d", index)
			}
		default:
			if state.payload[index] < 0x20 {
				return 0, fmt.Errorf("invalid control character at byte %d", index)
			}
			_, width := utf8.DecodeRune(state.payload[index:])
			if width == 1 && state.payload[index] >= utf8.RuneSelf {
				return 0, fmt.Errorf("invalid UTF-8 at byte %d", index)
			}
			index += width
		}
	}
	return 0, io.ErrUnexpectedEOF
}

func parseConversationTurnstileHexUnit(value []byte) (uint16, bool) {
	if len(value) != 4 {
		return 0, false
	}
	var unit uint16
	for _, character := range value {
		unit <<= 4
		switch {
		case character >= '0' && character <= '9':
			unit |= uint16(character - '0')
		case character >= 'a' && character <= 'f':
			unit |= uint16(character-'a') + 10
		case character >= 'A' && character <= 'F':
			unit |= uint16(character-'A') + 10
		default:
			return 0, false
		}
	}
	return unit, true
}

func conversationTurnstileHasIsolatedSurrogate(units []uint16) bool {
	for index := 0; index < len(units); index++ {
		unit := units[index]
		if unit >= 0xd800 && unit <= 0xdbff {
			if index+1 < len(units) && units[index+1] >= 0xdc00 && units[index+1] <= 0xdfff {
				index++
				continue
			}
			return true
		}
		if unit >= 0xdc00 && unit <= 0xdfff {
			return true
		}
	}
	return false
}

func (vm *conversationTurnstileVM) initialize(program []any, requirementsToken string) {
	vm.set(0, newConversationTurnstileCallable(vm.opNestedChallenge))
	vm.set(1, newConversationTurnstileCallable(vm.opXOR))
	vm.set(2, newConversationTurnstileCallable(vm.opSetLiteral))
	vm.set(3, newConversationTurnstileCallable(vm.opResult))
	vm.set(4, newConversationTurnstileCallable(vm.opReject))
	vm.set(5, newConversationTurnstileCallable(vm.opAppend))
	vm.set(6, newConversationTurnstileCallable(vm.opProperty))
	vm.set(7, newConversationTurnstileCallable(vm.opCall))
	vm.set(8, newConversationTurnstileCallable(vm.opCopy))
	vm.set(9, program)
	vm.set(10, conversationTurnstileObjectRef{path: "window"})
	vm.set(11, newConversationTurnstileCallable(vm.opScriptSource))
	vm.set(12, newConversationTurnstileCallable(vm.opProcessMap))
	vm.set(13, newConversationTurnstileCallable(vm.opCallRaw))
	vm.set(14, newConversationTurnstileCallable(vm.opJSONParse))
	vm.set(15, newConversationTurnstileCallable(vm.opJSONStringify))
	vm.set(16, requirementsToken)
	vm.set(17, newConversationTurnstileCallable(vm.opCallResult))
	vm.set(18, newConversationTurnstileCallable(vm.opBase64Decode))
	vm.set(19, newConversationTurnstileCallable(vm.opBase64Encode))
	vm.set(20, newConversationTurnstileCallable(vm.opIfEqual))
	vm.set(21, newConversationTurnstileCallable(vm.opIfElapsed))
	vm.set(22, newConversationTurnstileCallable(vm.opNestedQueue))
	vm.set(23, newConversationTurnstileCallable(vm.opIfDefined))
	vm.set(24, newConversationTurnstileCallable(vm.opBoundProperty))
	vm.set(25, newConversationTurnstileCallable(conversationTurnstileNoop))
	vm.set(26, newConversationTurnstileCallable(conversationTurnstileNoop))
	vm.set(27, newConversationTurnstileCallable(vm.opRemove))
	vm.set(28, newConversationTurnstileCallable(conversationTurnstileNoop))
	vm.set(29, newConversationTurnstileCallable(vm.opLessThan))
	vm.set(30, newConversationTurnstileCallable(vm.opSubroutine))
	vm.set(33, newConversationTurnstileCallable(vm.opMultiply))
	vm.set(34, newConversationTurnstileCallable(vm.opPromiseResolve))
	vm.set(35, newConversationTurnstileCallable(vm.opDivide))
}

func (vm *conversationTurnstileVM) runQueue() error {
	vm.queueDepth++
	defer func() { vm.queueDepth-- }()
	if vm.queueDepth > conversationTurnstileMaxQueueDepth {
		return conversationTurnstileFatalError{message: "conversation turnstile queue depth limit exceeded"}
	}
	for {
		if vm.fatalErr != nil {
			return vm.fatalErr
		}
		if err := vm.ctx.Err(); err != nil {
			return err
		}
		queueValue := vm.get(9)
		if vm.fatalErr != nil {
			return vm.fatalErr
		}
		queue, ok := conversationTurnstileSlice(queueValue)
		if !ok || len(queue) == 0 {
			return nil
		}
		if err := vm.chargeStep(); err != nil {
			return err
		}
		vm.set(9, queue[1:])
		if vm.fatalErr != nil {
			return vm.fatalErr
		}
		instruction, ok := conversationTurnstileSlice(queue[0])
		if !ok {
			return conversationTurnstileTypeError("instruction is not iterable")
		}
		opcode := any(conversationTurnstileUndefined)
		var callArgs []any
		if len(instruction) > 0 {
			opcode = instruction[0]
		}
		if len(instruction) > 1 {
			callArgs = instruction[1:]
		}
		callable := vm.get(opcode)
		if vm.fatalErr != nil {
			return vm.fatalErr
		}
		if _, err := vm.call(callable, callArgs); err != nil {
			if vm.fatalErr != nil {
				return vm.fatalErr
			}
			if ctxErr := vm.ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		}
		if vm.fatalErr != nil {
			return vm.fatalErr
		}
		if vm.deferred {
			return nil
		}
		if vm.fatalErr != nil {
			return vm.fatalErr
		}
		vm.instructionCount++
		if vm.settled {
			return nil
		}
	}
}

func (vm *conversationTurnstileVM) sharedExecutionBudget() *conversationTurnstileExecutionBudget {
	if vm.executionBudget == nil {
		vm.executionBudget = &conversationTurnstileExecutionBudget{maxSteps: defaultConversationTurnstileMaxSteps}
	} else if vm.executionBudget.maxSteps <= 0 {
		vm.executionBudget.maxSteps = defaultConversationTurnstileMaxSteps
	}
	return vm.executionBudget
}

func (vm *conversationTurnstileVM) chargeStep() error {
	if vm.ctx != nil {
		if err := vm.ctx.Err(); err != nil {
			return err
		}
	}
	budget := vm.sharedExecutionBudget()
	budget.steps++
	if budget.steps > budget.maxSteps {
		return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile VM exceeded %d steps", budget.maxSteps)}
	}
	return nil
}

func (vm *conversationTurnstileVM) get(key any) any {
	mapKey := vm.mapKey(key)
	if vm.fatalErr != nil {
		return conversationTurnstileUndefined
	}
	value, exists := vm.values[mapKey]
	if !exists {
		return conversationTurnstileUndefined
	}
	return value
}

func (vm *conversationTurnstileVM) set(key, value any) {
	mapKey := vm.mapKey(key)
	if vm.fatalErr != nil {
		return
	}
	if vm.values == nil {
		vm.values = make(map[string]any)
	}
	vm.values[mapKey] = value
}

func (vm *conversationTurnstileVM) chargeRuntimeWork(size int) error {
	if vm.ctx != nil {
		if err := vm.ctx.Err(); err != nil {
			return err
		}
	}
	budget := vm.sharedExecutionBudget()
	if size < 0 || size > conversationTurnstileMaxRuntimeWork || budget.runtimeWork > conversationTurnstileMaxRuntimeWork-size {
		return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile runtime work exceeds %d bytes", conversationTurnstileMaxRuntimeWork)}
	}
	budget.runtimeWork += size
	return nil
}

func (vm *conversationTurnstileVM) strictEqual(left, right any) (bool, error) {
	if conversationTurnstileIsString(left) && conversationTurnstileIsString(right) {
		workSize := conversationTurnstileStringStorageSize(left) + conversationTurnstileStringStorageSize(right)
		if err := vm.chargeRuntimeWork(workSize); err != nil {
			return false, err
		}
	}
	return conversationTurnstileStrictEqual(left, right), nil
}

func conversationTurnstileStringStorageSize(value any) int {
	switch typed := value.(type) {
	case string:
		return len(typed)
	case conversationTurnstileJSString:
		return len(typed.units) * 2
	case conversationTurnstileObjectRef:
		return len(typed.path)
	default:
		return 0
	}
}

func (vm *conversationTurnstileVM) fail(err error) {
	if err != nil && vm.fatalErr == nil {
		vm.fatalErr = err
	}
}

func (vm *conversationTurnstileVM) mapKey(value any) string {
	workSize := 1
	allocationSize := 64
	switch typed := value.(type) {
	case string:
		units := conversationTurnstileUTF16Length(typed)
		workSize = len(typed)
		allocationSize = 2 + units*8
	case conversationTurnstileJSString:
		workSize = len(typed.units) * 2
		allocationSize = 2 + len(typed.units)*8
	}
	if err := vm.chargeRuntimeWork(workSize); err != nil {
		vm.fail(err)
		return ""
	}
	if err := vm.reserveRuntimeBytes(allocationSize); err != nil {
		vm.fail(err)
		return ""
	}
	return conversationTurnstileMapKey(value)
}

func (vm *conversationTurnstileVM) reserveRuntimeBytes(size int) error {
	if vm.memoryBudget == nil {
		vm.memoryBudget = &conversationTurnstileMemoryBudget{}
	}
	return vm.memoryBudget.reserve(size)
}

func (vm *conversationTurnstileVM) runtimeString(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		if err := vm.chargeRuntimeWork(len(typed) * 2); err != nil {
			return "", err
		}
		if err := vm.reserveRuntimeBytes(len(typed)); err != nil {
			return "", err
		}
		return typed, nil
	case conversationTurnstileJSString:
		outputSize := conversationTurnstileUTF16UTF8Length(typed.units)
		if err := vm.chargeRuntimeWork(len(typed.units)*2 + outputSize); err != nil {
			return "", err
		}
		if err := vm.reserveRuntimeBytes(outputSize); err != nil {
			return "", err
		}
		return conversationTurnstileStringFromUTF16(typed.units), nil
	}
	if err := vm.chargeRuntimeWork(conversationTurnstileStringStorageSize(value)); err != nil {
		return "", err
	}
	return conversationTurnstileStringLimitedContext(
		vm.ctx,
		value,
		conversationTurnstileMaxValueBytes,
		vm.chargeRuntimeWork,
		vm.reserveRuntimeBytes,
	)
}

func (vm *conversationTurnstileVM) toPropertyKey(value any) (any, error) {
	propertyKey, err := vm.primitiveWithHint(value, true)
	if err != nil {
		return nil, err
	}
	switch propertyKey.(type) {
	case string, conversationTurnstileJSString:
	default:
		converted, err := vm.runtimeString(propertyKey)
		if err != nil {
			return nil, err
		}
		propertyKey = converted
	}
	return propertyKey, nil
}

func (vm *conversationTurnstileVM) propertyKey(value any) (any, string, error) {
	propertyKey, err := vm.toPropertyKey(value)
	if err != nil {
		return nil, "", err
	}
	mapKey := vm.mapKey(propertyKey)
	if vm.fatalErr != nil {
		return nil, "", vm.fatalErr
	}
	return propertyKey, mapKey, nil
}

func (vm *conversationTurnstileVM) runtimeUTF16(value any) ([]uint16, error) {
	if typed, ok := value.(conversationTurnstileJSString); ok {
		if err := vm.chargeRuntimeWork(len(typed.units) * 2); err != nil {
			return nil, err
		}
		return typed.units, nil
	}
	if typed, ok := value.(string); ok {
		if err := vm.chargeRuntimeWork(len(typed)); err != nil {
			return nil, err
		}
		if err := vm.reserveRuntimeBytes(conversationTurnstileUTF16Length(typed) * 2); err != nil {
			return nil, err
		}
		return conversationTurnstileUTF16Units(typed), nil
	}
	converted, err := vm.runtimeString(value)
	if err != nil {
		return nil, err
	}
	if err = vm.reserveRuntimeBytes(conversationTurnstileUTF16Length(converted) * 2); err != nil {
		return nil, err
	}
	return conversationTurnstileUTF16Units(converted), nil
}

func (vm *conversationTurnstileVM) utf16Index(value, search any, start float64) (int, error) {
	valueUnits, err := vm.runtimeUTF16(value)
	if err != nil {
		return -1, err
	}
	searchUnits, err := vm.runtimeUTF16(search)
	if err != nil {
		return -1, err
	}
	if err = vm.reserveRuntimeBytes(len(searchUnits) * 8); err != nil {
		return -1, err
	}
	if err = vm.chargeRuntimeWork((len(valueUnits) + len(searchUnits)) * 2); err != nil {
		return -1, err
	}
	startIndex := 0
	if !math.IsNaN(start) && start > 0 {
		if math.IsInf(start, 1) || start >= float64(len(valueUnits)) {
			startIndex = len(valueUnits)
		} else {
			startIndex = int(math.Trunc(start))
		}
	}
	index := conversationTurnstileUTF16UnitsIndex(valueUnits[startIndex:], searchUnits)
	if index < 0 {
		return -1, nil
	}
	return startIndex + index, nil
}

func (vm *conversationTurnstileVM) number(value any) (float64, bool, error) {
	switch value.(type) {
	case string, conversationTurnstileJSString:
		converted, err := vm.runtimeString(value)
		if err != nil {
			return 0, false, err
		}
		if err = vm.chargeRuntimeWork(len(converted)); err != nil {
			return 0, false, err
		}
		trimmed := strings.TrimSpace(converted)
		parsed, ok := parseConversationTurnstileNumberString(trimmed)
		return parsed, ok, nil
	case conversationTurnstileUndefinedValue, conversationTurnstileExplicitNullValue, nil, json.Number, float64, float32, int, int64, bool:
		parsed, ok := conversationTurnstileNumber(value)
		return parsed, ok, nil
	default:
		switch value.(type) {
		case *conversationTurnstileBoxedPrimitive, *conversationTurnstileOrderedMap, map[string]any:
			primitive, err := vm.primitive(value)
			if err != nil {
				return 0, false, err
			}
			return vm.number(primitive)
		}
		converted, err := vm.runtimeString(value)
		if err != nil {
			return 0, false, err
		}
		if err = vm.chargeRuntimeWork(len(converted)); err != nil {
			return 0, false, err
		}
		parsed, ok := parseConversationTurnstileNumberString(strings.TrimSpace(converted))
		return parsed, ok, nil
	}
}

func (vm *conversationTurnstileVM) primitive(value any) (any, error) {
	return vm.primitiveWithHint(value, false)
}

func (vm *conversationTurnstileVM) primitiveWithHint(value any, stringHint bool) (any, error) {
	if boxed, ok := value.(*conversationTurnstileBoxedPrimitive); ok {
		if boxed == nil {
			return nil, nil
		}
		return boxed.value, nil
	}
	switch value.(type) {
	case conversationTurnstileUndefinedValue, conversationTurnstileExplicitNullValue, nil, json.Number, float64, float32, int, int64, bool, string, conversationTurnstileJSString:
		return value, nil
	case *conversationTurnstileOrderedMap, map[string]any:
		first, second := "valueOf", "toString"
		if stringHint {
			first, second = second, first
		}
		for _, name := range [2]string{first, second} {
			method, exists, defaultObjectPrototype, err := vm.lookupPrimitiveMethod(value, name)
			if err != nil {
				return nil, err
			}
			if !exists {
				if name == "toString" && defaultObjectPrototype {
					return "[object Object]", nil
				}
				continue
			}
			if !conversationTurnstileCallableValue(method) {
				continue
			}
			primitive, err := vm.call(method, nil)
			if err != nil {
				return nil, err
			}
			if conversationTurnstilePrimitive(primitive) {
				return primitive, nil
			}
		}
		return nil, conversationTurnstileTypeError("Cannot convert object to primitive value")
	default:
		return vm.runtimeString(value)
	}
}

func (vm *conversationTurnstileVM) compileRegexp(value any) (*regexp.Regexp, error) {
	switch typed := value.(type) {
	case string:
		if len(typed) > conversationTurnstileMaxRegexpBytes {
			return nil, conversationTurnstileRegexpSizeError()
		}
	case conversationTurnstileJSString:
		if len(typed.units) > conversationTurnstileMaxRegexpBytes {
			return nil, conversationTurnstileRegexpSizeError()
		}
	}
	pattern, err := conversationTurnstileStringLimited(value, conversationTurnstileMaxRegexpBytes+1)
	if err != nil {
		return nil, err
	}
	if len(pattern) > conversationTurnstileMaxRegexpBytes {
		return nil, conversationTurnstileRegexpSizeError()
	}
	if err = vm.chargeRuntimeWork(conversationTurnstileStringStorageSize(value) + len(pattern)); err != nil {
		return nil, err
	}
	if err = vm.reserveRuntimeBytes(len(pattern)); err != nil {
		return nil, err
	}
	if vm.regexpCache == nil {
		vm.regexpCache = make(map[string]*regexp.Regexp)
	}
	if matcher := vm.regexpCache[pattern]; matcher != nil {
		return matcher, nil
	}
	if err = vm.reserveRuntimeBytes(256 + len(pattern)*8); err != nil {
		return nil, err
	}
	matcher, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	vm.regexpCache[pattern] = matcher
	return matcher, nil
}

type conversationTurnstileContextRuneReader struct {
	ctx    context.Context
	reader *strings.Reader
	err    error
}

func (reader *conversationTurnstileContextRuneReader) ReadRune() (rune, int, error) {
	if reader.ctx != nil {
		if err := reader.ctx.Err(); err != nil {
			reader.err = err
			return 0, 0, err
		}
	}
	return reader.reader.ReadRune()
}

func (vm *conversationTurnstileVM) regexpMatchIndices(matcher *regexp.Regexp, text string, submatches bool) ([]int, error) {
	patternSize := len(matcher.String())
	if patternSize < 1 {
		patternSize = 1
	}
	if len(text) > conversationTurnstileMaxRuntimeWork/patternSize {
		return nil, conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile runtime work exceeds %d bytes", conversationTurnstileMaxRuntimeWork)}
	}
	if err := vm.chargeRuntimeWork(len(text) * patternSize); err != nil {
		return nil, err
	}
	reader := &conversationTurnstileContextRuneReader{ctx: vm.ctx, reader: strings.NewReader(text)}
	var indices []int
	if submatches {
		indices = matcher.FindReaderSubmatchIndex(reader)
	} else {
		indices = matcher.FindReaderIndex(reader)
	}
	if reader.err != nil {
		return nil, reader.err
	}
	return indices, nil
}

func conversationTurnstileRegexpSizeError() error {
	return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile regular expression exceeds %d bytes", conversationTurnstileMaxRegexpBytes)}
}

func conversationTurnstileMapKey(value any) string {
	switch typed := value.(type) {
	case conversationTurnstileUndefinedValue:
		return "u:"
	case nil:
		return "l:"
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return "n:nan"
		}
		return conversationTurnstileNumberMapKey(parsed)
	case float64:
		return conversationTurnstileNumberMapKey(typed)
	case float32:
		return conversationTurnstileNumberMapKey(float64(typed))
	case int:
		return conversationTurnstileNumberMapKey(float64(typed))
	case int64:
		return conversationTurnstileNumberMapKey(float64(typed))
	case bool:
		return "b:" + strconv.FormatBool(typed)
	case string:
		return conversationTurnstileUTF16MapKey(conversationTurnstileUTF16Units(typed))
	case conversationTurnstileJSString:
		return conversationTurnstileUTF16MapKey(typed.units)
	case conversationTurnstileCallable:
		return fmt.Sprintf("f:%p", typed.identity)
	case conversationTurnstileObjectRef:
		return "o:global:" + typed.path
	case *conversationTurnstileOrderedMap:
		return fmt.Sprintf("o:ordered:%p", typed)
	case *conversationTurnstileProcessMapRef:
		return fmt.Sprintf("o:process-map:%p", typed)
	case *conversationTurnstileLocationRef:
		return fmt.Sprintf("o:location:%p", typed)
	case *conversationTurnstileArray:
		return fmt.Sprintf("o:array:%p", typed)
	case *conversationTurnstileBoxedPrimitive:
		return fmt.Sprintf("o:boxed:%p", typed)
	case map[string]any:
		return fmt.Sprintf("o:map:%x", reflect.ValueOf(typed).Pointer())
	case []any:
		return fmt.Sprintf("o:slice:%x:%d:%d", reflect.ValueOf(typed).Pointer(), len(typed), cap(typed))
	case []string:
		return fmt.Sprintf("o:string-slice:%x:%d:%d", reflect.ValueOf(typed).Pointer(), len(typed), cap(typed))
	default:
		return "v:" + fmt.Sprint(value)
	}
}

func conversationTurnstileNumberMapKey(value float64) string {
	if value == 0 {
		return "n:0"
	}
	if math.IsNaN(value) {
		return "n:nan"
	}
	return "n:" + strconv.FormatFloat(value, 'g', -1, 64)
}

func conversationTurnstilePropertyKey(value any) (any, string) {
	switch value.(type) {
	case string, conversationTurnstileJSString:
		return value, conversationTurnstileMapKey(value)
	default:
		converted := conversationTurnstileString(value)
		return converted, conversationTurnstileMapKey(converted)
	}
}

func conversationTurnstileUTF16MapKey(units []uint16) string {
	payload := make([]byte, 2+len(units)*2)
	payload[0] = 's'
	payload[1] = ':'
	for index, unit := range units {
		payload[2+index*2] = byte(unit >> 8)
		payload[3+index*2] = byte(unit)
	}
	return string(payload)
}

func conversationTurnstileSlice(value any) ([]any, bool) {
	switch typed := value.(type) {
	case *conversationTurnstileArray:
		if typed == nil {
			return nil, false
		}
		return typed.items, true
	case []any:
		return typed, true
	case []string:
		out := make([]any, len(typed))
		for index := range typed {
			out[index] = typed[index]
		}
		return out, true
	default:
		return nil, false
	}
}

func conversationTurnstileArrayValue(values []any) *conversationTurnstileArray {
	capacity := len(values)
	if capacity == 0 {
		capacity = 1
	}
	items := make([]any, len(values), capacity)
	copy(items, values)
	return &conversationTurnstileArray{items: items}
}

func (vm *conversationTurnstileVM) opNestedChallenge(args []any) (any, error) {
	if len(args) == 0 {
		return nil, nil
	}
	nestedChallenge, err := vm.runtimeString(args[0])
	if err != nil {
		return nil, err
	}
	if len(nestedChallenge) > base64.StdEncoding.EncodedLen(conversationTurnstileMaxBytes) {
		return nil, conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile nested challenge exceeds %d bytes", conversationTurnstileMaxBytes)}
	}
	// The SDK serializes nested VMs behind the currently running VM. Awaiting
	// that queued promise blocks this instruction until the outer 500 ms
	// fallback resolves with the number of completed instructions.
	vm.deferred = true
	vm.set(9, conversationTurnstileArrayValue(nil))
	return nil, nil
}

func (vm *conversationTurnstileVM) opXOR(args []any) (any, error) {
	if len(args) < 2 {
		return nil, nil
	}
	value, err := vm.runtimeUTF16(vm.get(args[0]))
	if err != nil {
		return nil, err
	}
	key, err := vm.runtimeUTF16(vm.get(args[1]))
	if err != nil {
		return nil, err
	}
	if len(key) == 0 {
		return nil, nil
	}
	if err := vm.reserveRuntimeBytes(len(value) * 2); err != nil {
		return nil, err
	}
	result := append([]uint16(nil), value...)
	for index := range result {
		result[index] ^= key[index%len(key)]
	}
	vm.set(args[0], conversationTurnstileJSString{units: result})
	return nil, nil
}

func (vm *conversationTurnstileVM) opSetLiteral(args []any) (any, error) {
	if len(args) >= 2 {
		vm.set(args[0], args[1])
	}
	return nil, nil
}

func (vm *conversationTurnstileVM) opResult(args []any) (any, error) {
	if len(args) == 0 {
		return nil, nil
	}
	encoded, err := vm.base64EncodeBinaryString(args[0])
	if err != nil {
		return nil, err
	}
	if vm.settled {
		return nil, nil
	}
	vm.settled = true
	vm.result = encoded
	return nil, nil
}

func (vm *conversationTurnstileVM) opReject(args []any) (any, error) {
	if vm.settled {
		return nil, nil
	}
	vm.settled = true
	return nil, conversationTurnstileFatalError{message: "conversation turnstile challenge rejected"}
}

func (vm *conversationTurnstileVM) opAppend(args []any) (any, error) {
	if len(args) < 2 {
		return nil, nil
	}
	current := vm.get(args[0])
	incoming := vm.get(args[1])
	if array, ok := current.(*conversationTurnstileArray); ok && array != nil {
		if err := vm.reserveRuntimeBytes(16); err != nil {
			return nil, err
		}
		array.append(incoming)
		return nil, nil
	}
	if list, ok := conversationTurnstileSlice(current); ok {
		if err := vm.reserveRuntimeBytes((len(list) + 1) * 16); err != nil {
			return nil, err
		}
		vm.set(args[0], conversationTurnstileArrayValue(append(append([]any(nil), list...), incoming)))
		return nil, nil
	}
	leftPrimitive, err := vm.primitive(current)
	if err != nil {
		return nil, err
	}
	rightPrimitive, err := vm.primitive(incoming)
	if err != nil {
		return nil, err
	}
	if conversationTurnstileIsString(leftPrimitive) || conversationTurnstileIsString(rightPrimitive) {
		left, err := vm.runtimeString(leftPrimitive)
		if err != nil {
			return nil, err
		}
		right, err := vm.runtimeString(rightPrimitive)
		if err != nil {
			return nil, err
		}
		if err = vm.reserveRuntimeBytes(len(left) + len(right)); err != nil {
			return nil, err
		}
		vm.set(args[0], left+right)
		return nil, nil
	}
	left, _ := conversationTurnstileNumber(leftPrimitive)
	right, _ := conversationTurnstileNumber(rightPrimitive)
	vm.set(args[0], left+right)
	return nil, nil
}

func (vm *conversationTurnstileVM) opProperty(args []any) (any, error) {
	if len(args) >= 3 {
		value, err := vm.property(vm.get(args[1]), vm.get(args[2]))
		if err != nil {
			return nil, err
		}
		vm.set(args[0], value)
	}
	return nil, nil
}

func (vm *conversationTurnstileVM) opBoundProperty(args []any) (any, error) {
	if len(args) >= 3 {
		value, err := vm.bindProperty(vm.get(args[1]), vm.get(args[2]))
		if err != nil {
			return nil, err
		}
		vm.set(args[0], value)
	}
	return nil, nil
}

func (vm *conversationTurnstileVM) opCall(args []any) (any, error) {
	if len(args) == 0 {
		return nil, nil
	}
	resolved, err := vm.resolve(args[1:])
	if err != nil {
		return nil, err
	}
	_, err = vm.call(vm.get(args[0]), resolved)
	return nil, err
}

func (vm *conversationTurnstileVM) opCopy(args []any) (any, error) {
	if len(args) >= 2 {
		vm.set(args[0], vm.get(args[1]))
	}
	return nil, nil
}

func (vm *conversationTurnstileVM) opScriptSource(args []any) (any, error) {
	if len(args) < 2 {
		return nil, nil
	}
	matcher, err := vm.compileRegexp(vm.get(args[1]))
	if err != nil {
		return nil, err
	}
	for _, source := range vm.scriptSources {
		match, matchErr := vm.regexpMatchIndices(matcher, source, false)
		if matchErr != nil {
			return nil, matchErr
		}
		if match != nil && match[0] != match[1] {
			matched := source[match[0]:match[1]]
			if err = vm.reserveRuntimeBytes(len(matched)); err != nil {
				return nil, err
			}
			vm.set(args[0], matched)
			return nil, nil
		}
	}
	vm.set(args[0], nil)
	return nil, nil
}

func (vm *conversationTurnstileVM) opProcessMap(args []any) (any, error) {
	if len(args) > 0 {
		if vm.processMap == nil {
			vm.processMap = &conversationTurnstileProcessMapRef{vm: vm}
		}
		vm.set(args[0], vm.processMap)
	}
	return nil, nil
}

func (vm *conversationTurnstileVM) opCallRaw(args []any) (any, error) {
	if len(args) < 2 {
		return nil, nil
	}
	_, err := vm.call(vm.get(args[1]), args[2:])
	if err != nil {
		if fatalErr := conversationTurnstileFatalInstructionError(vm.ctx, err); fatalErr != nil {
			return nil, fatalErr
		}
		vm.set(args[0], conversationTurnstileErrorString(err))
	}
	return nil, nil
}

func (vm *conversationTurnstileVM) opJSONParse(args []any) (any, error) {
	if len(args) < 2 {
		return nil, nil
	}
	input, err := vm.runtimeString(vm.get(args[1]))
	if err != nil {
		return nil, err
	}
	value, err := decodeConversationTurnstileJSONWithContextAndBudget(vm.ctx, []byte(input), vm.memoryBudget)
	if err != nil {
		if fatalErr := conversationTurnstileFatalInstructionError(vm.ctx, err); fatalErr != nil {
			return nil, fatalErr
		}
		return nil, conversationTurnstileJSError{name: "SyntaxError", message: err.Error()}
	}
	vm.set(args[0], value)
	return nil, nil
}

func (vm *conversationTurnstileVM) opJSONStringify(args []any) (any, error) {
	if len(args) < 2 {
		return nil, nil
	}
	value := vm.get(args[1])
	if conversationTurnstileJSONUnsupported(value) {
		vm.set(args[0], conversationTurnstileUndefined)
		return nil, nil
	}
	payload, err := marshalConversationTurnstileJSONLimitedContext(
		vm.ctx,
		value,
		conversationTurnstileMaxValueBytes,
		vm.chargeRuntimeWork,
		vm.reserveRuntimeBytes,
	)
	if err != nil {
		return nil, err
	}
	if err = vm.reserveRuntimeBytes(len(payload)); err != nil {
		return nil, err
	}
	vm.set(args[0], string(payload))
	return nil, nil
}

func (vm *conversationTurnstileVM) opPromiseResolve(args []any) (any, error) {
	if len(args) < 2 {
		return nil, nil
	}
	value, settled, err := vm.resolveThenable(vm.get(args[1]), 0)
	if err != nil {
		return nil, err
	}
	if !settled {
		vm.deferred = true
		vm.set(9, conversationTurnstileArrayValue(nil))
		return nil, nil
	}
	vm.set(args[0], value)
	return nil, nil
}

func (vm *conversationTurnstileVM) resolveThenable(value any, depth int) (any, bool, error) {
	if depth > conversationTurnstileMaxQueueDepth {
		return nil, true, conversationTurnstileTypeError("chaining cycle detected for promise")
	}
	var then any
	switch value.(type) {
	case *conversationTurnstileOrderedMap, map[string]any:
		var err error
		then, err = vm.property(value, "then")
		if err != nil {
			return nil, true, err
		}
	default:
		return value, true, nil
	}
	if _, callable := then.(conversationTurnstileCallable); !callable {
		objectRef, isObjectRef := then.(conversationTurnstileObjectRef)
		if !isObjectRef || !conversationTurnstileCallableObjectPath(objectRef.path) {
			return value, true, nil
		}
	}
	settled := false
	rejected := false
	var resolved any = conversationTurnstileUndefined
	resolve := newConversationTurnstileCallable(func(args []any) (any, error) {
		if !settled {
			settled = true
			if len(args) > 0 {
				resolved = args[0]
			}
		}
		return conversationTurnstileUndefined, nil
	})
	reject := newConversationTurnstileCallable(func(args []any) (any, error) {
		if !settled {
			settled = true
			rejected = true
			if len(args) > 0 {
				resolved = args[0]
			}
		}
		return conversationTurnstileUndefined, nil
	})
	if _, err := vm.call(then, []any{resolve, reject}); err != nil {
		if fatalErr := conversationTurnstileFatalInstructionError(vm.ctx, err); fatalErr != nil {
			return nil, true, fatalErr
		}
		if !settled {
			return nil, true, err
		}
	}
	if !settled {
		return nil, false, nil
	}
	if rejected {
		message, err := vm.runtimeString(resolved)
		if err != nil {
			return nil, true, err
		}
		return nil, true, conversationTurnstileThrownValue{text: message}
	}
	if conversationTurnstileStrictEqual(value, resolved) {
		return nil, true, conversationTurnstileTypeError("chaining cycle detected for promise")
	}
	return vm.resolveThenable(resolved, depth+1)
}

func (vm *conversationTurnstileVM) opCallResult(args []any) (any, error) {
	if len(args) < 2 {
		return nil, nil
	}
	resolved, err := vm.resolve(args[2:])
	if err != nil {
		return nil, err
	}
	value, err := vm.call(vm.get(args[1]), resolved)
	if err != nil {
		if fatalErr := conversationTurnstileFatalInstructionError(vm.ctx, err); fatalErr != nil {
			return nil, fatalErr
		}
		vm.set(args[0], conversationTurnstileErrorString(err))
		return nil, nil
	}
	vm.set(args[0], value)
	return nil, nil
}

func (vm *conversationTurnstileVM) opBase64Decode(args []any) (any, error) {
	if len(args) == 0 {
		return nil, nil
	}
	encoded, err := vm.runtimeString(vm.get(args[0]))
	if err != nil {
		return nil, err
	}
	if err = vm.reserveRuntimeBytes(len(encoded) + 3); err != nil {
		return nil, err
	}
	encoded = strings.Map(func(character rune) rune {
		switch character {
		case ' ', '\t', '\r', '\n', '\f':
			return -1
		default:
			return character
		}
	}, encoded)
	if err = vm.chargeRuntimeWork(len(encoded)); err != nil {
		return nil, err
	}
	if remainder := len(encoded) % 4; remainder != 0 {
		encoded += strings.Repeat("=", 4-remainder)
	}
	decodedSize := base64.StdEncoding.DecodedLen(len(encoded))
	if err := vm.reserveRuntimeBytes(decodedSize * 3); err != nil {
		return nil, err
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	units := make([]uint16, len(decoded))
	for index, value := range decoded {
		units[index] = uint16(value)
	}
	vm.set(args[0], conversationTurnstileJSString{units: units})
	return nil, nil
}

func (vm *conversationTurnstileVM) opBase64Encode(args []any) (any, error) {
	if len(args) > 0 {
		value, err := vm.base64EncodeBinaryString(vm.get(args[0]))
		if err != nil {
			return nil, err
		}
		vm.set(args[0], value)
	}
	return nil, nil
}

func (vm *conversationTurnstileVM) base64EncodeBinaryString(value any) (string, error) {
	units, err := vm.runtimeUTF16(value)
	if err != nil {
		return "", err
	}
	encodedSize := base64.StdEncoding.EncodedLen(len(units))
	if err = vm.reserveRuntimeBytes(len(units) + encodedSize); err != nil {
		return "", err
	}
	bytesValue := make([]byte, len(units))
	for index, unit := range units {
		if unit > 0xff {
			return "", fmt.Errorf("conversation turnstile btoa input contains a code unit above 255")
		}
		bytesValue[index] = byte(unit)
	}
	return base64.StdEncoding.EncodeToString(bytesValue), nil
}

func (vm *conversationTurnstileVM) opIfEqual(args []any) (any, error) {
	if len(args) < 3 {
		return nil, nil
	}
	equal, err := vm.strictEqual(vm.get(args[0]), vm.get(args[1]))
	if err != nil || !equal {
		return nil, err
	}
	_, err = vm.call(vm.get(args[2]), args[3:])
	return nil, err
}

func (vm *conversationTurnstileVM) opIfElapsed(args []any) (any, error) {
	if len(args) < 4 {
		return nil, nil
	}
	left, leftOK, err := vm.number(vm.get(args[0]))
	if err != nil {
		return nil, err
	}
	right, rightOK, err := vm.number(vm.get(args[1]))
	if err != nil {
		return nil, err
	}
	threshold, thresholdOK, err := vm.number(vm.get(args[2]))
	if err != nil {
		return nil, err
	}
	delta := left - right
	if !leftOK || !rightOK || !thresholdOK || math.IsNaN(left) || math.IsNaN(right) || math.IsNaN(threshold) || math.IsNaN(delta) || absConversationTurnstileNumber(delta) <= threshold {
		return nil, nil
	}
	_, err = vm.call(vm.get(args[3]), args[4:])
	return nil, err
}

func (vm *conversationTurnstileVM) opNestedQueue(args []any) (_ any, err error) {
	if len(args) < 2 {
		return nil, nil
	}
	queue, ok := conversationTurnstileSlice(args[1])
	if !ok {
		return nil, nil
	}
	previous := vm.get(9)
	vm.set(9, queue)
	defer func() { vm.set(9, previous) }()
	if err = vm.runQueue(); err != nil {
		if fatalErr := conversationTurnstileFatalInstructionError(vm.ctx, err); fatalErr != nil {
			return nil, fatalErr
		}
		vm.set(args[0], conversationTurnstileErrorString(err))
	}
	return nil, nil
}

func (vm *conversationTurnstileVM) opIfDefined(args []any) (any, error) {
	if len(args) < 2 || isConversationTurnstileUndefined(vm.get(args[0])) {
		return nil, nil
	}
	_, err := vm.call(vm.get(args[1]), args[2:])
	return nil, err
}

func (vm *conversationTurnstileVM) opRemove(args []any) (any, error) {
	if len(args) < 2 {
		return nil, nil
	}
	current := vm.get(args[0])
	incoming := vm.get(args[1])
	if array, ok := current.(*conversationTurnstileArray); ok && array != nil {
		removeIndex := -1
		for index := range array.items {
			if !array.has(index) {
				continue
			}
			if err := vm.chargeRuntimeWork(1); err != nil {
				return nil, err
			}
			equal, err := vm.strictEqual(array.items[index], incoming)
			if err != nil {
				return nil, err
			}
			if equal {
				removeIndex = index
				break
			}
		}
		if removeIndex < 0 {
			removeIndex = len(array.items) - 1
		}
		if removeIndex >= 0 {
			if err := vm.chargeRuntimeWork(len(array.items) - removeIndex - 1); err != nil {
				return nil, err
			}
			array.remove(removeIndex)
		}
		return nil, nil
	}
	left, leftOK, err := vm.number(current)
	if err != nil {
		return nil, err
	}
	right, rightOK, err := vm.number(incoming)
	if err != nil {
		return nil, err
	}
	if leftOK && rightOK {
		vm.set(args[0], left-right)
	} else {
		vm.set(args[0], math.NaN())
	}
	return nil, nil
}

func (vm *conversationTurnstileVM) opLessThan(args []any) (any, error) {
	if len(args) < 3 {
		return nil, nil
	}
	less, err := vm.lessThan(vm.get(args[1]), vm.get(args[2]))
	if err != nil {
		return nil, err
	}
	vm.set(args[0], less)
	return nil, nil
}

func (vm *conversationTurnstileVM) opSubroutine(args []any) (any, error) {
	if len(args) < 3 {
		return nil, nil
	}
	destination := args[0]
	returnKey := args[1]
	var captureKeys []any
	captureMode := false
	queueValue := args[2]
	if len(args) > 3 {
		if queue, ok := conversationTurnstileSlice(args[3]); ok {
			captureKeys, _ = conversationTurnstileSlice(args[2])
			queueValue = args[3]
			captureMode = captureKeys != nil && queue != nil
		}
	}
	queue, ok := conversationTurnstileSlice(queueValue)
	if !ok {
		return nil, nil
	}
	callable := newConversationTurnstileCallable(func(callArgs []any) (_ any, err error) {
		if vm.settled {
			return conversationTurnstileUndefined, nil
		}
		previous := vm.get(9)
		vm.set(9, queue)
		defer func() { vm.set(9, previous) }()
		if captureMode {
			for index, key := range captureKeys {
				if index < len(callArgs) {
					vm.set(key, callArgs[index])
				}
			}
		}
		if err = vm.runQueue(); err != nil {
			if fatalErr := conversationTurnstileFatalInstructionError(vm.ctx, err); fatalErr != nil {
				return nil, fatalErr
			}
			return conversationTurnstileErrorString(err), nil
		}
		return vm.get(returnKey), nil
	})
	vm.set(destination, callable)
	return nil, nil
}

func (vm *conversationTurnstileVM) opMultiply(args []any) (any, error) {
	if len(args) < 3 {
		return nil, nil
	}
	left, leftOK, err := vm.number(vm.get(args[1]))
	if err != nil {
		return nil, err
	}
	right, rightOK, err := vm.number(vm.get(args[2]))
	if err != nil {
		return nil, err
	}
	if !leftOK || !rightOK {
		vm.set(args[0], math.NaN())
		return nil, nil
	}
	vm.set(args[0], left*right)
	return nil, nil
}

func (vm *conversationTurnstileVM) opDivide(args []any) (any, error) {
	if len(args) < 3 {
		return nil, nil
	}
	left, leftOK, err := vm.number(vm.get(args[1]))
	if err != nil {
		return nil, err
	}
	right, rightOK, err := vm.number(vm.get(args[2]))
	if err != nil {
		return nil, err
	}
	if !leftOK || !rightOK {
		vm.set(args[0], math.NaN())
		return nil, nil
	}
	if right == 0 {
		vm.set(args[0], float64(0))
	} else {
		vm.set(args[0], left/right)
	}
	return nil, nil
}

func conversationTurnstileNoop([]any) (any, error) {
	return nil, nil
}

func (vm *conversationTurnstileVM) resolve(keys []any) ([]any, error) {
	if err := vm.reserveArrayElements(len(keys), 16); err != nil {
		return nil, err
	}
	values := make([]any, len(keys))
	for index := range keys {
		values[index] = vm.get(keys[index])
		if vm.fatalErr != nil {
			return nil, vm.fatalErr
		}
	}
	return values, nil
}

func (vm *conversationTurnstileVM) call(target any, args []any) (any, error) {
	if callable, ok := target.(conversationTurnstileCallable); ok && callable.call != nil {
		value, err := callable.invoke(args)
		if _, explicitNull := value.(conversationTurnstileExplicitNullValue); explicitNull {
			return nil, err
		}
		if value == nil {
			value = conversationTurnstileUndefined
		}
		return value, err
	}
	name := ""
	switch typed := target.(type) {
	case conversationTurnstileObjectRef:
		name = typed.path
	default:
		return nil, conversationTurnstileTypeError("value is not callable")
	}
	switch name {
	case "window.String":
		if len(args) == 0 {
			return "", nil
		}
		primitive, err := vm.stringPrimitive(args[0])
		if err != nil {
			return nil, err
		}
		return vm.runtimeString(primitive)
	case "window.Array":
		if len(args) != 1 {
			if err := vm.reserveRuntimeBytes(len(args) * 16); err != nil {
				return nil, err
			}
			return conversationTurnstileArrayValue(args), nil
		}
		if length, numeric := conversationTurnstileNumberPrimitive(args[0]); numeric {
			if math.IsNaN(length) || math.IsInf(length, 0) || length < 0 || length != math.Trunc(length) || length > math.MaxUint32 {
				return nil, conversationTurnstileJSError{name: "RangeError", message: "Invalid array length"}
			}
			if length > float64(conversationTurnstileMaxValueBytes/16) {
				return nil, conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile value exceeds %d bytes", conversationTurnstileMaxValueBytes)}
			}
			if err := vm.reserveRuntimeBytes(int(length) * 17); err != nil {
				return nil, err
			}
			items := make([]any, int(length))
			for index := range items {
				items[index] = conversationTurnstileUndefined
			}
			present := make([]bool, len(items))
			return &conversationTurnstileArray{items: items, present: present}, nil
		}
		if err := vm.reserveRuntimeBytes(16); err != nil {
			return nil, err
		}
		return conversationTurnstileArrayValue(args), nil
	case "window.Object":
		if len(args) == 0 || args[0] == nil || isConversationTurnstileUndefined(args[0]) {
			if err := vm.reserveRuntimeBytes(128); err != nil {
				return nil, err
			}
			return newConversationTurnstileOrderedMap(), nil
		}
		if _, null := args[0].(conversationTurnstileExplicitNullValue); null {
			if err := vm.reserveRuntimeBytes(128); err != nil {
				return nil, err
			}
			return newConversationTurnstileOrderedMap(), nil
		}
		switch args[0].(type) {
		case string, conversationTurnstileJSString, bool, json.Number, float64, float32, int, int64:
			if err := vm.reserveRuntimeBytes(32); err != nil {
				return nil, err
			}
			return &conversationTurnstileBoxedPrimitive{value: args[0]}, nil
		default:
			return args[0], nil
		}
	case "window.performance.now":
		randomValue, err := randomFloat64(vm.reader)
		if err != nil {
			return nil, err
		}
		elapsed := vm.now().Sub(vm.startedAt)
		if elapsed < 0 {
			elapsed = 0
		}
		return (float64(elapsed.Nanoseconds()) + randomValue) / float64(time.Millisecond), nil
	case "window.Object.create":
		// The Sentinel compact dispatcher returns a normal object for a JSON null
		// argument. Preserve that captured SDK behavior rather than native JS.
		var prototype any
		prototypeSet := false
		if len(args) > 0 && args[0] != nil && !isConversationTurnstileUndefined(args[0]) {
			prototype = args[0]
			if _, explicitNull := prototype.(conversationTurnstileExplicitNullValue); explicitNull {
				prototype = nil
			} else if conversationTurnstilePrimitive(prototype) {
				return nil, conversationTurnstileTypeError(fmt.Sprintf("Object prototype may only be an Object or null: %s", conversationTurnstileString(prototype)))
			}
			prototypeSet = true
		}
		if err := vm.reserveRuntimeBytes(128); err != nil {
			return nil, err
		}
		object := newConversationTurnstileOrderedMap()
		object.prototype = prototype
		object.prototypeSet = prototypeSet
		return object, nil
	case "window.Object.keys":
		if len(args) == 0 {
			return nil, conversationTurnstileTypeError("Cannot convert undefined or null to object")
		}
		switch value := args[0].(type) {
		case conversationTurnstileUndefinedValue, conversationTurnstileExplicitNullValue, nil:
			return nil, conversationTurnstileTypeError("Cannot convert undefined or null to object")
		case conversationTurnstileObjectRef:
			if value.path == "window.localStorage" {
				return vm.localStorageKeyArray()
			}
			return conversationTurnstileArrayValue(nil), nil
		case string:
			if err := vm.chargeRuntimeWork(len(value)); err != nil {
				return nil, err
			}
			return vm.objectIndexKeys(conversationTurnstileUTF16Length(value))
		case conversationTurnstileJSString:
			if err := vm.chargeRuntimeWork(len(value.units) * 2); err != nil {
				return nil, err
			}
			return vm.objectIndexKeys(len(value.units))
		case *conversationTurnstileArray:
			if value == nil {
				return nil, conversationTurnstileTypeError("Cannot convert undefined or null to object")
			}
			return vm.objectArrayKeys(value)
		case *conversationTurnstileBoxedPrimitive:
			if value != nil && conversationTurnstileIsString(value.value) {
				units, err := vm.runtimeUTF16(value.value)
				if err != nil {
					return nil, err
				}
				return vm.objectIndexKeys(len(units))
			}
			return conversationTurnstileArrayValue(nil), nil
		case []any:
			return vm.objectIndexKeys(len(value))
		case []string:
			return vm.objectIndexKeys(len(value))
		case *conversationTurnstileOrderedMap:
			keys, err := value.jsKeys(vm.ctx, vm.reserveRuntimeBytes, vm.chargeRuntimeWork)
			if err != nil {
				return nil, err
			}
			if err = vm.reserveRuntimeBytes(len(keys) * 16); err != nil {
				return nil, err
			}
			return conversationTurnstileArrayValue(keys), nil
		case *conversationTurnstileProcessMapRef:
			return conversationTurnstileArrayValue(nil), nil
		case map[string]any:
			keys := make([]string, 0, len(value))
			for key := range value {
				keys = append(keys, key)
			}
			conversationTurnstileSortStringKeys(keys)
			if err := vm.reserveRuntimeBytes(len(keys) * 16); err != nil {
				return nil, err
			}
			return conversationTurnstileStrings(keys), nil
		}
		return conversationTurnstileArrayValue(nil), nil
	case "window.Array.isArray":
		if len(args) == 0 {
			return false, nil
		}
		_, ok := conversationTurnstileSlice(args[0])
		return ok, nil
	case "window.Array.from":
		return vm.arrayFrom(args)
	case "window.Math.abs":
		if len(args) == 0 {
			return math.NaN(), nil
		}
		value, ok, err := vm.number(args[0])
		if err != nil {
			return nil, err
		}
		if !ok {
			return math.NaN(), nil
		}
		return math.Abs(value), nil
	case "window.Math.random":
		return randomFloat64(vm.reader)
	case "window.String.fromCharCode":
		if err := vm.reserveRuntimeBytes(len(args) * 2); err != nil {
			return nil, err
		}
		units := make([]uint16, 0, len(args))
		for _, arg := range args {
			value, _, err := vm.number(arg)
			if err != nil {
				return nil, err
			}
			units = append(units, uint16(int(value)&0xffff))
		}
		return conversationTurnstileJSString{units: units}, nil
	case "window.Reflect.set":
		if len(args) < 3 {
			return false, nil
		}
		switch object := args[0].(type) {
		case *conversationTurnstileOrderedMap:
			propertyKey, mapKey, err := vm.propertyKey(args[1])
			if err != nil {
				return nil, err
			}
			object.setWithMapKey(propertyKey, mapKey, args[2])
			return true, nil
		case map[string]any:
			propertyKey, err := vm.toPropertyKey(args[1])
			if err != nil {
				return nil, err
			}
			key, err := vm.runtimeString(propertyKey)
			if err != nil {
				return nil, err
			}
			object[key] = args[2]
			return true, nil
		}
		return false, nil
	default:
		return nil, conversationTurnstileTypeError("value is not callable")
	}
}

func (vm *conversationTurnstileVM) stringPrimitive(value any) (any, error) {
	return vm.primitiveWithHint(value, true)
}

func (vm *conversationTurnstileVM) lookupPrimitiveMethod(value any, name string) (any, bool, bool, error) {
	visited := make(map[*conversationTurnstileOrderedMap]struct{})
	depth := 0
	for {
		switch object := value.(type) {
		case *conversationTurnstileOrderedMap:
			if object == nil {
				return nil, false, false, nil
			}
			depth++
			if depth > conversationTurnstileMaxPrototypeDepth {
				return nil, false, false, conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile prototype chain exceeds %d objects", conversationTurnstileMaxPrototypeDepth)}
			}
			if err := vm.chargeRuntimeWork(1); err != nil {
				return nil, false, false, err
			}
			if _, seen := visited[object]; seen {
				return nil, false, false, conversationTurnstileTypeError("Cannot convert object to primitive value")
			}
			visited[object] = struct{}{}
			if method, exists := object.get(name); exists {
				return method, true, false, nil
			}
			if !object.prototypeSet {
				return nil, false, true, nil
			}
			if object.prototype == nil {
				return nil, false, false, nil
			}
			value = object.prototype
		case map[string]any:
			method, exists := object[name]
			return method, exists, !exists, nil
		default:
			method, err := vm.property(value, name)
			if err != nil {
				return nil, false, false, err
			}
			return method, !isConversationTurnstileUndefined(method), false, nil
		}
	}
}

func conversationTurnstilePrimitive(value any) bool {
	switch value.(type) {
	case nil, conversationTurnstileUndefinedValue, conversationTurnstileExplicitNullValue,
		json.Number, float64, float32, int, int64, bool, string, conversationTurnstileJSString:
		return true
	default:
		return false
	}
}

func conversationTurnstileCallableValue(value any) bool {
	if callable, ok := value.(conversationTurnstileCallable); ok {
		return callable.call != nil
	}
	objectRef, ok := value.(conversationTurnstileObjectRef)
	return ok && conversationTurnstileCallableObjectPath(objectRef.path)
}

func (vm *conversationTurnstileVM) arrayFrom(args []any) (any, error) {
	if len(args) == 0 || isConversationTurnstileUndefined(args[0]) {
		return nil, conversationTurnstileTypeError("undefined is not iterable (cannot read property Symbol(Symbol.iterator))")
	}
	if args[0] == nil {
		return nil, conversationTurnstileTypeError("object null is not iterable (cannot read property Symbol(Symbol.iterator))")
	}
	if _, explicitNull := args[0].(conversationTurnstileExplicitNullValue); explicitNull {
		return nil, conversationTurnstileTypeError("object null is not iterable (cannot read property Symbol(Symbol.iterator))")
	}
	mapFn := any(conversationTurnstileUndefined)
	if len(args) > 1 {
		mapFn = args[1]
		if !isConversationTurnstileUndefined(mapFn) && !conversationTurnstileCallableValue(mapFn) {
			return nil, conversationTurnstileTypeError(fmt.Sprintf("%s is not a function", conversationTurnstileString(mapFn)))
		}
	}
	items, err := vm.arrayFromItems(args[0])
	if err != nil {
		return nil, err
	}
	if err = vm.reserveArrayElements(len(items), 16); err != nil {
		return nil, err
	}
	if !isConversationTurnstileUndefined(mapFn) {
		for index := range items {
			items[index], err = vm.call(mapFn, []any{items[index], index})
			if err != nil {
				return nil, err
			}
		}
	}
	return conversationTurnstileArrayValue(items), nil
}

func (vm *conversationTurnstileVM) reserveArrayElements(count, bytesPerElement int) error {
	if count < 0 || count > conversationTurnstileMaxValueBytes/16 {
		return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile value exceeds %d bytes", conversationTurnstileMaxValueBytes)}
	}
	if err := vm.chargeRuntimeWork(count); err != nil {
		return err
	}
	return vm.reserveRuntimeBytes(count * bytesPerElement)
}

func (vm *conversationTurnstileVM) arrayFromItems(source any) ([]any, error) {
	if conversationTurnstileIsString(source) {
		units, err := vm.runtimeUTF16(source)
		if err != nil {
			return nil, err
		}
		if err = vm.reserveArrayElements(len(units), 40); err != nil {
			return nil, err
		}
		items := make([]any, 0, len(units))
		for index := 0; index < len(units); {
			if index&255 == 0 && vm.ctx != nil {
				if err = vm.ctx.Err(); err != nil {
					return nil, err
				}
			}
			start := index
			index++
			if units[start] >= 0xd800 && units[start] <= 0xdbff && index < len(units) && units[index] >= 0xdc00 && units[index] <= 0xdfff {
				index++
			}
			items = append(items, conversationTurnstileJSString{units: units[start:index]})
		}
		return items, nil
	}
	if array, ok := source.(*conversationTurnstileArray); ok && array != nil {
		if err := vm.reserveArrayElements(len(array.items), 16); err != nil {
			return nil, err
		}
		items := make([]any, len(array.items))
		for index := range items {
			if index&255 == 0 && vm.ctx != nil {
				if err := vm.ctx.Err(); err != nil {
					return nil, err
				}
			}
			items[index] = conversationTurnstileUndefined
			if array.has(index) {
				items[index] = array.items[index]
			}
		}
		return items, nil
	}
	if values, ok := conversationTurnstileSlice(source); ok {
		if err := vm.reserveArrayElements(len(values), 16); err != nil {
			return nil, err
		}
		return append([]any(nil), values...), nil
	}
	lengthValue, err := vm.property(source, "length")
	if err != nil {
		return nil, err
	}
	lengthNumber, _, err := vm.number(lengthValue)
	if err != nil {
		return nil, err
	}
	length := conversationTurnstileToLength(lengthNumber)
	if length > uint64(conversationTurnstileMaxValueBytes/16) {
		return nil, conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile value exceeds %d bytes", conversationTurnstileMaxValueBytes)}
	}
	if err = vm.reserveArrayElements(int(length), 16); err != nil {
		return nil, err
	}
	items := make([]any, int(length))
	for index := range items {
		items[index], err = vm.property(source, strconv.Itoa(index))
		if err != nil {
			return nil, err
		}
	}
	return items, nil
}

func conversationTurnstileToLength(value float64) uint64 {
	if math.IsNaN(value) || value <= 0 {
		return 0
	}
	const maxSafeInteger = uint64(1<<53 - 1)
	if math.IsInf(value, 1) || value >= float64(maxSafeInteger) {
		return maxSafeInteger
	}
	return uint64(math.Floor(value))
}

func (vm *conversationTurnstileVM) localStorageKeyArray() (any, error) {
	if len(vm.localStorageKeys) > conversationTurnstileMaxValueBytes/16 {
		return nil, conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile localStorage keys exceed %d bytes", conversationTurnstileMaxValueBytes)}
	}
	totalBytes := len(vm.localStorageKeys) * 16
	for index, key := range vm.localStorageKeys {
		if index&255 == 0 && vm.ctx != nil {
			if err := vm.ctx.Err(); err != nil {
				return nil, err
			}
		}
		if len(key) > conversationTurnstileMaxValueBytes-totalBytes {
			return nil, conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile localStorage keys exceed %d bytes", conversationTurnstileMaxValueBytes)}
		}
		totalBytes += len(key)
		if err := vm.chargeRuntimeWork(len(key)); err != nil {
			return nil, err
		}
	}
	if err := vm.reserveRuntimeBytes(totalBytes); err != nil {
		return nil, err
	}
	items := make([]any, len(vm.localStorageKeys))
	for index, key := range vm.localStorageKeys {
		items[index] = key
	}
	return &conversationTurnstileArray{items: items}, nil
}

func (vm *conversationTurnstileVM) objectIndexKeys(length int) (any, error) {
	if length <= 0 {
		return conversationTurnstileArrayValue(nil), nil
	}
	maxKeyBytes := len(strconv.Itoa(length - 1))
	if length > conversationTurnstileMaxValueBytes/(16+maxKeyBytes) {
		return nil, conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile object keys exceed %d bytes", conversationTurnstileMaxValueBytes)}
	}
	if err := vm.reserveRuntimeBytes(length * (16 + maxKeyBytes)); err != nil {
		return nil, err
	}
	keys := make([]any, length)
	for index := range keys {
		if index&255 == 0 && vm.ctx != nil {
			if err := vm.ctx.Err(); err != nil {
				return nil, err
			}
		}
		keys[index] = strconv.Itoa(index)
	}
	return &conversationTurnstileArray{items: keys}, nil
}

func (vm *conversationTurnstileVM) objectArrayKeys(array *conversationTurnstileArray) (any, error) {
	if array == nil || len(array.items) == 0 {
		return conversationTurnstileArrayValue(nil), nil
	}
	if array.present == nil {
		return vm.objectIndexKeys(len(array.items))
	}
	count := 0
	for _, present := range array.present {
		if present {
			count++
		}
	}
	maxKeyBytes := len(strconv.Itoa(len(array.items) - 1))
	if err := vm.reserveRuntimeBytes(count * (16 + maxKeyBytes)); err != nil {
		return nil, err
	}
	keys := make([]any, 0, count)
	for index := range array.items {
		if index&255 == 0 && vm.ctx != nil {
			if err := vm.ctx.Err(); err != nil {
				return nil, err
			}
		}
		if array.has(index) {
			keys = append(keys, strconv.Itoa(index))
		}
	}
	return &conversationTurnstileArray{items: keys}, nil
}

func (vm *conversationTurnstileVM) bindProperty(object, key any) (any, error) {
	propertyKey, err := vm.toPropertyKey(key)
	if err != nil {
		return conversationTurnstileUndefined, err
	}
	keyText, err := vm.runtimeString(propertyKey)
	if err != nil {
		return conversationTurnstileUndefined, err
	}
	if processMap, ok := object.(*conversationTurnstileProcessMapRef); ok {
		return processMap.method(keyText), nil
	}
	switch object.(type) {
	case string, conversationTurnstileJSString:
		return vm.bindStringProperty(object, keyText)
	}
	return vm.property(object, propertyKey)
}

func (vm *conversationTurnstileVM) bindStringProperty(value any, keyText string) (any, error) {
	switch keyText {
	case "toString":
		return newConversationTurnstileCallable(func([]any) (any, error) { return value, nil }), nil
	case "charCodeAt":
		return newConversationTurnstileCallable(func(args []any) (any, error) {
			indexValue := float64(0)
			if len(args) > 0 {
				if parsed, ok, err := vm.number(args[0]); err != nil {
					return nil, err
				} else if ok {
					indexValue = conversationTurnstileToIntegerOrInfinity(parsed)
				}
			}
			length := 0
			if text, ok := value.(string); ok {
				if err := vm.chargeRuntimeWork(len(text)); err != nil {
					return nil, err
				}
				length = conversationTurnstileUTF16Length(text)
			} else if text, ok := value.(conversationTurnstileJSString); ok {
				length = len(text.units)
			}
			if indexValue < 0 || math.IsInf(indexValue, 0) || indexValue >= float64(length) {
				return math.NaN(), nil
			}
			index := int(indexValue)
			unit, ok := conversationTurnstileUTF16CodeUnitAt(value, index)
			if !ok {
				return math.NaN(), nil
			}
			return int(unit), nil
		}), nil
	case "indexOf":
		return newConversationTurnstileCallable(func(args []any) (any, error) {
			search := any(conversationTurnstileUndefined)
			if len(args) > 0 {
				search = args[0]
			}
			start := float64(0)
			if len(args) > 1 {
				parsed, ok, err := vm.number(args[1])
				if err != nil {
					return nil, err
				}
				if ok {
					start = parsed
				}
			}
			return vm.utf16Index(value, search, start)
		}), nil
	case "search":
		return newConversationTurnstileCallable(func(args []any) (any, error) {
			pattern := any("")
			if len(args) > 0 && !isConversationTurnstileUndefined(args[0]) {
				pattern = args[0]
			}
			matcher, err := vm.compileRegexp(pattern)
			if err != nil {
				return nil, err
			}
			text, err := vm.runtimeString(value)
			if err != nil {
				return nil, err
			}
			match, err := vm.regexpMatchIndices(matcher, text, false)
			if err != nil {
				return nil, err
			}
			if match == nil {
				return -1, nil
			}
			return conversationTurnstileUTF16Length(text[:match[0]]), nil
		}), nil
	case "match":
		return newConversationTurnstileCallable(func(args []any) (any, error) {
			pattern := any("")
			if len(args) > 0 && !isConversationTurnstileUndefined(args[0]) {
				pattern = args[0]
			}
			matcher, err := vm.compileRegexp(pattern)
			if err != nil {
				return nil, err
			}
			text, err := vm.runtimeString(value)
			if err != nil {
				return nil, err
			}
			indices, err := vm.regexpMatchIndices(matcher, text, true)
			if err != nil {
				return nil, err
			}
			if indices == nil {
				return conversationTurnstileExplicitNull, nil
			}
			if err = vm.reserveRuntimeBytes(64 + len(indices)/2*16); err != nil {
				return nil, err
			}
			matches := make([]any, len(indices)/2)
			for index := range matches {
				start, end := indices[index*2], indices[index*2+1]
				if start >= 0 && end >= start {
					matches[index] = text[start:end]
				} else {
					matches[index] = conversationTurnstileUndefined
				}
			}
			return &conversationTurnstileArray{items: matches}, nil
		}), nil
	}
	return vm.property(value, keyText)
}

func (processMap *conversationTurnstileProcessMapRef) method(name string) any {
	if processMap == nil || processMap.vm == nil {
		return nil
	}
	vm := processMap.vm
	switch name {
	case "size":
		return len(vm.values)
	case "get":
		return newConversationTurnstileCallable(func(args []any) (any, error) {
			return vm.get(conversationTurnstileArgument(args, 0)), nil
		})
	case "set":
		return newConversationTurnstileCallable(func(args []any) (any, error) {
			vm.set(conversationTurnstileArgument(args, 0), conversationTurnstileArgument(args, 1))
			return processMap, nil
		})
	case "has":
		return newConversationTurnstileCallable(func(args []any) (any, error) {
			key := vm.mapKey(conversationTurnstileArgument(args, 0))
			if vm.fatalErr != nil {
				return nil, vm.fatalErr
			}
			_, exists := vm.values[key]
			return exists, nil
		})
	case "delete":
		return newConversationTurnstileCallable(func(args []any) (any, error) {
			key := vm.mapKey(conversationTurnstileArgument(args, 0))
			if vm.fatalErr != nil {
				return nil, vm.fatalErr
			}
			_, exists := vm.values[key]
			delete(vm.values, key)
			return exists, nil
		})
	case "clear":
		return newConversationTurnstileCallable(func([]any) (any, error) {
			vm.values = make(map[string]any)
			return nil, nil
		})
	}
	return conversationTurnstileUndefined
}

func conversationTurnstileArgument(args []any, index int) any {
	if index >= 0 && index < len(args) {
		return args[index]
	}
	return conversationTurnstileUndefined
}

func (vm *conversationTurnstileVM) property(object, key any) (any, error) {
	switch object.(type) {
	case nil, conversationTurnstileExplicitNullValue:
		keyText, err := vm.runtimeString(key)
		if err != nil {
			return conversationTurnstileUndefined, err
		}
		return conversationTurnstileUndefined, conversationTurnstileTypeError(fmt.Sprintf("Cannot read properties of null (reading '%s')", keyText))
	case conversationTurnstileUndefinedValue:
		keyText, err := vm.runtimeString(key)
		if err != nil {
			return conversationTurnstileUndefined, err
		}
		return conversationTurnstileUndefined, conversationTurnstileTypeError(fmt.Sprintf("Cannot read properties of undefined (reading '%s')", keyText))
	}
	propertyKey, err := vm.toPropertyKey(key)
	if err != nil {
		return conversationTurnstileUndefined, err
	}
	switch value := object.(type) {
	case *conversationTurnstileOrderedMap:
		if value == nil {
			return conversationTurnstileUndefined, nil
		}
		mapKey := vm.mapKey(propertyKey)
		if vm.fatalErr != nil {
			return conversationTurnstileUndefined, vm.fatalErr
		}
		visited := make(map[*conversationTurnstileOrderedMap]struct{})
		for depth := 1; ; depth++ {
			if depth > conversationTurnstileMaxPrototypeDepth {
				return conversationTurnstileUndefined, conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile prototype chain exceeds %d objects", conversationTurnstileMaxPrototypeDepth)}
			}
			if err = vm.chargeRuntimeWork(1); err != nil {
				return conversationTurnstileUndefined, err
			}
			if _, seen := visited[value]; seen {
				return conversationTurnstileUndefined, conversationTurnstileTypeError("cyclic object prototype chain")
			}
			visited[value] = struct{}{}
			if item, exists := value.values[mapKey]; exists {
				return item, nil
			}
			if !value.prototypeSet || value.prototype == nil {
				return conversationTurnstileUndefined, nil
			}
			next, ok := value.prototype.(*conversationTurnstileOrderedMap)
			if !ok {
				return vm.property(value.prototype, propertyKey)
			}
			if next == nil {
				return conversationTurnstileUndefined, nil
			}
			value = next
		}
	}
	keyText, err := vm.runtimeString(propertyKey)
	if err != nil {
		return conversationTurnstileUndefined, err
	}
	switch value := object.(type) {
	case conversationTurnstileObjectRef:
		path := value.path + "." + keyText
		if err := vm.chargeRuntimeWork(len(value.path) + len(keyText)); err != nil {
			return conversationTurnstileUndefined, err
		}
		if err := vm.reserveRuntimeBytes(len(path)); err != nil {
			return conversationTurnstileUndefined, err
		}
		if environmentValue, exists := vm.environment[path]; exists {
			return environmentValue, nil
		}
		if conversationTurnstileNativeObjectPath(path) {
			return conversationTurnstileObjectRef{path: path}, nil
		}
		prefix := path + "."
		for environmentPath := range vm.environment {
			if strings.HasPrefix(environmentPath, prefix) {
				return conversationTurnstileObjectRef{path: path}, nil
			}
		}
		return conversationTurnstileUndefined, nil
	case *conversationTurnstileProcessMapRef:
		return value.method(keyText), nil
	case map[string]any:
		item, exists := value[keyText]
		if !exists {
			return conversationTurnstileUndefined, nil
		}
		return item, nil
	case *conversationTurnstileLocationRef:
		if value == nil {
			return conversationTurnstileUndefined, nil
		}
		switch keyText {
		case "href":
			return value.href, nil
		case "origin":
			return value.origin, nil
		case "pathname":
			return value.pathname, nil
		case "search":
			return value.search, nil
		case "toString":
			return newConversationTurnstileCallable(func([]any) (any, error) { return value.href, nil }), nil
		}
	case *conversationTurnstileArray:
		if value == nil {
			return conversationTurnstileUndefined, nil
		}
		if keyText == "length" {
			return len(value.items), nil
		}
		index, validIndex := conversationTurnstileArrayIndexKey(keyText)
		if validIndex && value.has(int(index)) {
			return value.items[int(index)], nil
		}
	case []any:
		if keyText == "length" {
			return len(value), nil
		}
		index, validIndex := conversationTurnstileArrayIndexKey(keyText)
		if validIndex && uint64(index) < uint64(len(value)) {
			return value[int(index)], nil
		}
	case *conversationTurnstileBoxedPrimitive:
		if value == nil {
			return conversationTurnstileUndefined, nil
		}
		switch keyText {
		case "toString":
			return newConversationTurnstileCallable(func([]any) (any, error) {
				return vm.runtimeString(value.value)
			}), nil
		case "valueOf":
			return newConversationTurnstileCallable(func([]any) (any, error) { return value.value, nil }), nil
		}
		if conversationTurnstileIsString(value.value) {
			return vm.property(value.value, propertyKey)
		}
	case string:
		if keyText == "length" {
			if err := vm.chargeRuntimeWork(len(value)); err != nil {
				return conversationTurnstileUndefined, err
			}
			return conversationTurnstileUTF16Length(value), nil
		}
		if index, validIndex := conversationTurnstileArrayIndexKey(keyText); validIndex {
			if err := vm.chargeRuntimeWork(len(value)); err != nil {
				return conversationTurnstileUndefined, err
			}
			if unit, ok := conversationTurnstileUTF16CodeUnitAt(value, int(index)); ok {
				return conversationTurnstileJSString{units: []uint16{unit}}, nil
			}
		}
	case conversationTurnstileJSString:
		if keyText == "length" {
			return len(value.units), nil
		}
		if index, validIndex := conversationTurnstileArrayIndexKey(keyText); validIndex && uint64(index) < uint64(len(value.units)) {
			return conversationTurnstileJSString{units: []uint16{value.units[int(index)]}}, nil
		}
	}
	return conversationTurnstileUndefined, nil
}

func conversationTurnstileStrings(values []string) *conversationTurnstileArray {
	out := make([]any, len(values))
	for index := range values {
		out[index] = values[index]
	}
	return conversationTurnstileArrayValue(out)
}

func conversationTurnstileNativeObjectPath(path string) bool {
	switch path {
	case "window", "window.Array", "window.Math", "window.Reflect", "window.performance",
		"window.localStorage", "window.Object", "window.String",
		"window.Array.isArray", "window.Array.from",
		"window.Reflect.set", "window.performance.now", "window.Object.create",
		"window.Object.keys", "window.Math.random", "window.Math.abs",
		"window.String.fromCharCode":
		return true
	default:
		return false
	}
}

func conversationTurnstileString(value any) string {
	converted, err := conversationTurnstileStringLimited(value, conversationTurnstileMaxValueBytes)
	if err != nil {
		return ""
	}
	return converted
}

func conversationTurnstileStringLimited(value any, maxBytes int) (string, error) {
	return conversationTurnstileStringLimitedContext(nil, value, maxBytes, nil, nil)
}

func conversationTurnstileStringLimitedContext(ctx context.Context, value any, maxBytes int, charge, reserve func(int) error) (string, error) {
	if maxBytes <= 0 {
		return "", conversationTurnstileFatalError{message: "conversation turnstile string limit is invalid"}
	}
	buffer := &conversationTurnstileLimitedBuffer{ctx: ctx, max: maxBytes, charge: charge, reserve: reserve}
	nodes := 0
	if err := writeConversationTurnstileString(buffer, value, make(map[conversationTurnstileJSONVisit]bool), 0, &nodes); err != nil {
		return "", err
	}
	if buffer.err != nil {
		return "", buffer.err
	}
	if reserve != nil {
		if err := reserve(buffer.Len()); err != nil {
			return "", err
		}
	}
	return buffer.String(), nil
}

func writeConversationTurnstileString(buffer *conversationTurnstileLimitedBuffer, value any, seen map[conversationTurnstileJSONVisit]bool, depth int, nodes *int) error {
	if buffer.err != nil {
		return buffer.err
	}
	if buffer.ctx != nil {
		if err := buffer.ctx.Err(); err != nil {
			return err
		}
	}
	if depth > conversationTurnstileMaxJSONDepth {
		return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile string conversion exceeds depth %d", conversationTurnstileMaxJSONDepth)}
	}
	(*nodes)++
	if *nodes > conversationTurnstileMaxJSONNodes {
		return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile string conversion exceeds %d nodes", conversationTurnstileMaxJSONNodes)}
	}
	switch typed := value.(type) {
	case conversationTurnstileUndefinedValue:
		_, _ = buffer.WriteString("undefined")
	case nil:
		_, _ = buffer.WriteString("null")
	case string:
		_, _ = buffer.WriteString(typed)
	case conversationTurnstileObjectRef:
		if replacement, ok := conversationTurnstileGlobalObjectString(typed.path); ok {
			_, _ = buffer.WriteString(replacement)
		} else {
			_, _ = buffer.WriteString("[object Object]")
		}
	case conversationTurnstileJSString:
		_, _ = buffer.WriteUTF16(typed.units)
	case *conversationTurnstileLocationRef:
		if typed == nil {
			_, _ = buffer.WriteString("null")
		} else {
			_, _ = buffer.WriteString(typed.href)
		}
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			_, _ = buffer.WriteString("NaN")
		} else {
			_, _ = buffer.WriteString(conversationTurnstileFormatNumber(parsed))
		}
	case float64:
		_, _ = buffer.WriteString(conversationTurnstileFormatNumber(typed))
	case float32:
		_, _ = buffer.WriteString(strconv.FormatFloat(float64(typed), 'f', -1, 32))
	case int:
		_, _ = buffer.WriteString(strconv.Itoa(typed))
	case int64:
		_, _ = buffer.WriteString(strconv.FormatInt(typed, 10))
	case bool:
		_, _ = buffer.WriteString(strconv.FormatBool(typed))
	case *conversationTurnstileBoxedPrimitive:
		if typed != nil {
			return writeConversationTurnstileString(buffer, typed.value, seen, depth+1, nodes)
		}
	case *conversationTurnstileArray:
		if typed == nil {
			return nil
		}
		visit := conversationTurnstileJSONVisit{kind: 's', ptr: reflect.ValueOf(typed).Pointer()}
		if seen[visit] {
			return conversationTurnstileFatalError{message: "conversation turnstile string conversion contains a cycle"}
		}
		seen[visit] = true
		defer delete(seen, visit)
		for index, item := range typed.items {
			if index > 0 {
				_ = buffer.WriteByte(',')
			}
			if item == nil || isConversationTurnstileUndefined(item) {
				continue
			}
			if err := writeConversationTurnstileString(buffer, item, seen, depth+1, nodes); err != nil {
				return err
			}
		}
	case []any:
		if typed == nil {
			return nil
		}
		visit := conversationTurnstileJSONVisit{kind: 's', ptr: reflect.ValueOf(typed).Pointer()}
		if visit.ptr != 0 && seen[visit] {
			return conversationTurnstileFatalError{message: "conversation turnstile string conversion contains a cycle"}
		}
		if visit.ptr != 0 {
			seen[visit] = true
			defer delete(seen, visit)
		}
		for index, item := range typed {
			if index > 0 {
				_ = buffer.WriteByte(',')
			}
			if item == nil || isConversationTurnstileUndefined(item) {
				continue
			}
			if err := writeConversationTurnstileString(buffer, item, seen, depth+1, nodes); err != nil {
				return err
			}
		}
	case []string:
		for index, item := range typed {
			if index > 0 {
				_ = buffer.WriteByte(',')
			}
			_, _ = buffer.WriteString(item)
		}
	case *conversationTurnstileOrderedMap:
		_, _ = buffer.WriteString("[object Object]")
	case *conversationTurnstileProcessMapRef:
		_, _ = buffer.WriteString("[object Map]")
	case conversationTurnstileCallable:
		_, _ = buffer.WriteString("function () { [native code] }")
	case map[string]any:
		_, _ = buffer.WriteString("[object Object]")
	default:
		_, _ = buffer.WriteString(fmt.Sprint(value))
	}
	return buffer.err
}

func conversationTurnstileGlobalObjectString(path string) (string, bool) {
	switch path {
	case "window.Array":
		return "function Array() { [native code] }", true
	case "window.Math":
		return "[object Math]", true
	case "window.Reflect":
		return "[object Reflect]", true
	case "window.performance":
		return "[object Performance]", true
	case "window.localStorage":
		return "[object Storage]", true
	case "window.Object":
		return "function Object() { [native code] }", true
	case "window.String":
		return "function String() { [native code] }", true
	case "window.Array.isArray":
		return "function isArray() { [native code] }", true
	case "window.Array.from":
		return "function from() { [native code] }", true
	case "window.Reflect.set":
		return "function set() { [native code] }", true
	case "window.performance.now":
		return "function () { [native code] }", true
	case "window.Object.create":
		return "function create() { [native code] }", true
	case "window.Object.keys":
		return "function keys() { [native code] }", true
	case "window.Math.random":
		return "function random() { [native code] }", true
	case "window.Math.abs":
		return "function abs() { [native code] }", true
	case "window.String.fromCharCode":
		return "function fromCharCode() { [native code] }", true
	default:
		return "", false
	}
}

func conversationTurnstileFormatNumber(value float64) string {
	if math.IsNaN(value) {
		return "NaN"
	}
	if math.IsInf(value, 1) {
		return "Infinity"
	}
	if math.IsInf(value, -1) {
		return "-Infinity"
	}
	if value == 0 {
		return "0"
	}
	absolute := math.Abs(value)
	if absolute >= 1e-6 && absolute < 1e21 {
		return strconv.FormatFloat(value, 'f', -1, 64)
	}
	formatted := strconv.FormatFloat(value, 'e', -1, 64)
	mantissa, exponent, found := strings.Cut(formatted, "e")
	if !found {
		return formatted
	}
	sign := ""
	if strings.HasPrefix(exponent, "+") || strings.HasPrefix(exponent, "-") {
		sign = exponent[:1]
		exponent = exponent[1:]
	}
	exponent = strings.TrimLeft(exponent, "0")
	if exponent == "" {
		exponent = "0"
	}
	return mantissa + "e" + sign + exponent
}

func conversationTurnstileNumber(value any) (float64, bool) {
	switch typed := value.(type) {
	case conversationTurnstileUndefinedValue:
		return math.NaN(), true
	case conversationTurnstileExplicitNullValue:
		return 0, true
	case nil:
		return 0, true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case bool:
		if typed {
			return 1, true
		}
		return 0, true
	case string:
		return parseConversationTurnstileNumberString(strings.TrimSpace(typed))
	case conversationTurnstileJSString:
		return conversationTurnstileNumber(conversationTurnstileStringFromUTF16(typed.units))
	default:
		return 0, false
	}
}

func parseConversationTurnstileNumberString(value string) (float64, bool) {
	if value == "" {
		return 0, true
	}
	if len(value) > 2 && value[0] == '0' {
		base := 0
		switch value[1] {
		case 'x', 'X':
			base = 16
		case 'b', 'B':
			base = 2
		case 'o', 'O':
			base = 8
		}
		if base != 0 {
			parsed := float64(0)
			for _, character := range value[2:] {
				digit := -1
				switch {
				case character >= '0' && character <= '9':
					digit = int(character - '0')
				case character >= 'a' && character <= 'f':
					digit = int(character-'a') + 10
				case character >= 'A' && character <= 'F':
					digit = int(character-'A') + 10
				}
				if digit < 0 || digit >= base {
					return math.NaN(), false
				}
				parsed = parsed*float64(base) + float64(digit)
			}
			return parsed, true
		}
	}
	parsed, err := strconv.ParseFloat(value, 64)
	return parsed, err == nil || math.IsInf(parsed, 0)
}

func conversationTurnstileToIntegerOrInfinity(value float64) float64 {
	if math.IsNaN(value) || value == 0 {
		return 0
	}
	if math.IsInf(value, 0) {
		return value
	}
	return math.Trunc(value)
}

func conversationTurnstileIsString(value any) bool {
	switch value.(type) {
	case string, conversationTurnstileJSString:
		return true
	default:
		return false
	}
}

func isConversationTurnstileUndefined(value any) bool {
	_, ok := value.(conversationTurnstileUndefinedValue)
	return ok
}

func conversationTurnstileStrictEqual(left, right any) bool {
	leftNumber, leftIsNumber := conversationTurnstileNumberPrimitive(left)
	rightNumber, rightIsNumber := conversationTurnstileNumberPrimitive(right)
	if leftIsNumber || rightIsNumber {
		return leftIsNumber && rightIsNumber && !math.IsNaN(leftNumber) && !math.IsNaN(rightNumber) && leftNumber == rightNumber
	}
	switch leftValue := left.(type) {
	case conversationTurnstileUndefinedValue:
		return isConversationTurnstileUndefined(right)
	case nil:
		return right == nil
	case string:
		return conversationTurnstileStringEqual(leftValue, right)
	case conversationTurnstileJSString:
		return conversationTurnstileStringEqual(leftValue, right)
	case bool:
		rightValue, ok := right.(bool)
		return ok && leftValue == rightValue
	case *conversationTurnstileOrderedMap:
		rightValue, ok := right.(*conversationTurnstileOrderedMap)
		return ok && leftValue == rightValue
	case *conversationTurnstileProcessMapRef:
		rightValue, ok := right.(*conversationTurnstileProcessMapRef)
		return ok && leftValue == rightValue
	case conversationTurnstileObjectRef:
		rightValue, ok := right.(conversationTurnstileObjectRef)
		return ok && leftValue.path == rightValue.path
	case *conversationTurnstileLocationRef:
		rightValue, ok := right.(*conversationTurnstileLocationRef)
		return ok && leftValue == rightValue
	case []any:
		rightValue, ok := right.([]any)
		return ok && reflect.ValueOf(leftValue).Pointer() == reflect.ValueOf(rightValue).Pointer() && len(leftValue) == len(rightValue)
	case *conversationTurnstileArray:
		rightValue, ok := right.(*conversationTurnstileArray)
		return ok && leftValue == rightValue
	case *conversationTurnstileBoxedPrimitive:
		rightValue, ok := right.(*conversationTurnstileBoxedPrimitive)
		return ok && leftValue == rightValue
	case conversationTurnstileCallable:
		rightValue, ok := right.(conversationTurnstileCallable)
		return ok && leftValue.identity != nil && leftValue.identity == rightValue.identity
	default:
		return false
	}
}

func conversationTurnstileStringEqual(left, right any) bool {
	switch leftValue := left.(type) {
	case string:
		switch rightValue := right.(type) {
		case string:
			return leftValue == rightValue
		case conversationTurnstileJSString:
			return conversationTurnstileStringUnitsEqual(leftValue, rightValue.units)
		}
	case conversationTurnstileJSString:
		switch rightValue := right.(type) {
		case string:
			return conversationTurnstileStringUnitsEqual(rightValue, leftValue.units)
		case conversationTurnstileJSString:
			if len(leftValue.units) != len(rightValue.units) {
				return false
			}
			for index := range leftValue.units {
				if leftValue.units[index] != rightValue.units[index] {
					return false
				}
			}
			return true
		}
	}
	return false
}

func conversationTurnstileNumberPrimitive(value any) (float64, bool) {
	switch value.(type) {
	case json.Number, float64, float32, int, int64:
		return conversationTurnstileNumber(value)
	default:
		return 0, false
	}
}

func (vm *conversationTurnstileVM) lessThan(left, right any) (bool, error) {
	leftPrimitive, err := vm.primitive(left)
	if err != nil {
		return false, err
	}
	rightPrimitive, err := vm.primitive(right)
	if err != nil {
		return false, err
	}
	if conversationTurnstileIsString(leftPrimitive) && conversationTurnstileIsString(rightPrimitive) {
		leftUnits, err := vm.runtimeUTF16(leftPrimitive)
		if err != nil {
			return false, err
		}
		rightUnits, err := vm.runtimeUTF16(rightPrimitive)
		if err != nil {
			return false, err
		}
		if err = vm.chargeRuntimeWork((len(leftUnits) + len(rightUnits)) * 2); err != nil {
			return false, err
		}
		for index := 0; index < len(leftUnits) && index < len(rightUnits); index++ {
			if leftUnits[index] != rightUnits[index] {
				return leftUnits[index] < rightUnits[index], nil
			}
		}
		return len(leftUnits) < len(rightUnits), nil
	}
	leftNumber, leftOK, err := vm.number(leftPrimitive)
	if err != nil {
		return false, err
	}
	rightNumber, rightOK, err := vm.number(rightPrimitive)
	if err != nil {
		return false, err
	}
	return leftOK && rightOK && !math.IsNaN(leftNumber) && !math.IsNaN(rightNumber) && leftNumber < rightNumber, nil
}

func absConversationTurnstileNumber(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

func conversationTurnstileUTF16Units(value string) []uint16 {
	units := make([]uint16, 0, len(value))
	for offset := 0; offset < len(value); {
		first, second, count, width := conversationTurnstileNextUTF16(value, offset)
		units = append(units, first)
		if count == 2 {
			units = append(units, second)
		}
		offset += width
	}
	return units
}

func conversationTurnstileUTF16Length(value string) int {
	length := 0
	for offset := 0; offset < len(value); {
		_, _, count, width := conversationTurnstileNextUTF16(value, offset)
		length += count
		offset += width
	}
	return length
}

func conversationTurnstileStringUnitsEqual(value string, units []uint16) bool {
	position := 0
	for offset := 0; offset < len(value); {
		first, second, count, width := conversationTurnstileNextUTF16(value, offset)
		if position >= len(units) || units[position] != first {
			return false
		}
		position++
		if count == 2 {
			if position >= len(units) || units[position] != second {
				return false
			}
			position++
		}
		offset += width
	}
	return position == len(units)
}

func conversationTurnstileUTF16CodeUnitAt(value any, index int) (uint16, bool) {
	if index < 0 {
		return 0, false
	}
	if typed, ok := value.(conversationTurnstileJSString); ok {
		if index >= len(typed.units) {
			return 0, false
		}
		return typed.units[index], true
	}
	text, ok := value.(string)
	if !ok {
		return 0, false
	}
	position := 0
	for offset := 0; offset < len(text); {
		first, second, count, width := conversationTurnstileNextUTF16(text, offset)
		if position == index {
			return first, true
		}
		if count == 2 && position+1 == index {
			return second, true
		}
		position += count
		offset += width
	}
	return 0, false
}

func conversationTurnstileNextUTF16(value string, offset int) (uint16, uint16, int, int) {
	if offset+2 < len(value) && value[offset] == 0xed && value[offset+1]&0xc0 == 0x80 && value[offset+2]&0xc0 == 0x80 {
		codePoint := rune(value[offset]&0x0f)<<12 | rune(value[offset+1]&0x3f)<<6 | rune(value[offset+2]&0x3f)
		if codePoint >= 0xd800 && codePoint <= 0xdfff {
			return uint16(codePoint), 0, 1, 3
		}
	}
	runeValue, width := utf8.DecodeRuneInString(value[offset:])
	if runeValue <= 0xffff {
		return uint16(runeValue), 0, 1, width
	}
	high, low := utf16.EncodeRune(runeValue)
	return uint16(high), uint16(low), 2, width
}

func conversationTurnstileStringFromUTF16(units []uint16) string {
	encoded := make([]byte, 0, len(units)*3)
	for index := 0; index < len(units); index++ {
		unit := units[index]
		if unit >= 0xd800 && unit <= 0xdbff && index+1 < len(units) && units[index+1] >= 0xdc00 && units[index+1] <= 0xdfff {
			encoded = utf8.AppendRune(encoded, utf16.DecodeRune(rune(unit), rune(units[index+1])))
			index++
			continue
		}
		if unit >= 0xd800 && unit <= 0xdfff {
			encoded = append(encoded,
				byte(0xe0|unit>>12),
				byte(0x80|(unit>>6)&0x3f),
				byte(0x80|unit&0x3f),
			)
			continue
		}
		encoded = utf8.AppendRune(encoded, rune(unit))
	}
	return string(encoded)
}

func conversationTurnstileUTF16Index(value, search string) int {
	byteIndex := strings.Index(value, search)
	if byteIndex < 0 {
		return -1
	}
	return conversationTurnstileUTF16Length(value[:byteIndex])
}

func conversationTurnstileUTF16UnitsIndex(valueUnits, searchUnits []uint16) int {
	if len(searchUnits) == 0 {
		return 0
	}
	if len(searchUnits) > len(valueUnits) {
		return -1
	}
	prefix := make([]int, len(searchUnits))
	for index, matched := 1, 0; index < len(searchUnits); index++ {
		for matched > 0 && searchUnits[index] != searchUnits[matched] {
			matched = prefix[matched-1]
		}
		if searchUnits[index] == searchUnits[matched] {
			matched++
		}
		prefix[index] = matched
	}
	for index, matched := 0, 0; index < len(valueUnits); index++ {
		for matched > 0 && valueUnits[index] != searchUnits[matched] {
			matched = prefix[matched-1]
		}
		if valueUnits[index] == searchUnits[matched] {
			matched++
		}
		if matched == len(searchUnits) {
			return index - len(searchUnits) + 1
		}
	}
	return -1
}

type conversationTurnstileJSONVisit struct {
	kind byte
	ptr  uintptr
}

func marshalConversationTurnstileJSON(value any) ([]byte, error) {
	return marshalConversationTurnstileJSONLimited(value, conversationTurnstileMaxValueBytes)
}

type conversationTurnstileLimitedBuffer struct {
	bytes.Buffer
	ctx     context.Context
	max     int
	charge  func(int) error
	reserve func(int) error
	err     error
}

func (buffer *conversationTurnstileLimitedBuffer) prepareWrite(size int) error {
	if buffer.err != nil {
		return buffer.err
	}
	if buffer.ctx != nil {
		if err := buffer.ctx.Err(); err != nil {
			buffer.err = err
			return err
		}
	}
	if buffer.charge != nil {
		if err := buffer.charge(size); err != nil {
			buffer.err = err
			return err
		}
	}
	if size < 0 || size > buffer.max-buffer.Len() {
		buffer.err = conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile JSON exceeds %d bytes", buffer.max)}
		return buffer.err
	}
	if buffer.reserve != nil {
		if err := buffer.reserve(size); err != nil {
			buffer.err = err
			return err
		}
	}
	return nil
}

func (buffer *conversationTurnstileLimitedBuffer) Write(payload []byte) (int, error) {
	if err := buffer.prepareWrite(len(payload)); err != nil {
		return 0, err
	}
	return buffer.Buffer.Write(payload)
}

func (buffer *conversationTurnstileLimitedBuffer) WriteString(value string) (int, error) {
	if err := buffer.prepareWrite(len(value)); err != nil {
		return 0, err
	}
	return buffer.Buffer.WriteString(value)
}

func (buffer *conversationTurnstileLimitedBuffer) WriteByte(value byte) error {
	if err := buffer.prepareWrite(1); err != nil {
		return err
	}
	return buffer.Buffer.WriteByte(value)
}

func (buffer *conversationTurnstileLimitedBuffer) WriteUTF16(units []uint16) (int, error) {
	size := conversationTurnstileUTF16UTF8Length(units)
	if err := buffer.prepareWrite(size); err != nil {
		return 0, err
	}
	return buffer.Buffer.WriteString(conversationTurnstileStringFromUTF16(units))
}

func conversationTurnstileUTF16UTF8Length(units []uint16) int {
	length := 0
	for index := 0; index < len(units); index++ {
		unit := units[index]
		switch {
		case unit < 0x80:
			length++
		case unit < 0x800:
			length += 2
		case unit >= 0xd800 && unit <= 0xdbff && index+1 < len(units) && units[index+1] >= 0xdc00 && units[index+1] <= 0xdfff:
			length += 4
			index++
		default:
			length += 3
		}
	}
	return length
}

func marshalConversationTurnstileJSONLimited(value any, maxBytes int) ([]byte, error) {
	return marshalConversationTurnstileJSONLimitedContext(nil, value, maxBytes, nil, nil)
}

func marshalConversationTurnstileJSONLimitedContext(ctx context.Context, value any, maxBytes int, charge, reserve func(int) error) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, conversationTurnstileFatalError{message: "conversation turnstile JSON limit is invalid"}
	}
	buffer := &conversationTurnstileLimitedBuffer{ctx: ctx, max: maxBytes, charge: charge, reserve: reserve}
	nodes := 0
	if err := writeConversationTurnstileJSON(buffer, value, make(map[conversationTurnstileJSONVisit]bool), 0, &nodes); err != nil {
		return nil, err
	}
	if buffer.err != nil {
		return nil, buffer.err
	}
	return buffer.Bytes(), nil
}

func writeConversationTurnstileJSON(buffer *conversationTurnstileLimitedBuffer, value any, seen map[conversationTurnstileJSONVisit]bool, depth int, nodes *int) error {
	if buffer.err != nil {
		return buffer.err
	}
	if buffer.ctx != nil {
		if err := buffer.ctx.Err(); err != nil {
			return err
		}
	}
	if depth > conversationTurnstileMaxJSONDepth {
		return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile JSON exceeds depth %d", conversationTurnstileMaxJSONDepth)}
	}
	(*nodes)++
	if *nodes > conversationTurnstileMaxJSONNodes {
		return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile JSON exceeds %d nodes", conversationTurnstileMaxJSONNodes)}
	}
	switch typed := value.(type) {
	case *conversationTurnstileBoxedPrimitive:
		if typed == nil {
			buffer.WriteString("null")
			return nil
		}
		return writeConversationTurnstileJSON(buffer, typed.value, seen, depth+1, nodes)
	case *conversationTurnstileOrderedMap:
		if typed == nil {
			buffer.WriteString("null")
			return nil
		}
		visit := conversationTurnstileJSONVisit{kind: 'o', ptr: reflect.ValueOf(typed).Pointer()}
		if seen[visit] {
			return fmt.Errorf("conversation turnstile JSON contains a cycle")
		}
		seen[visit] = true
		defer delete(seen, visit)
		buffer.WriteByte('{')
		written := 0
		keys, err := typed.jsKeys(buffer.ctx, buffer.reserve, buffer.charge)
		if err != nil {
			return err
		}
		for _, key := range keys {
			item := typed.values[conversationTurnstileMapKey(key)]
			if conversationTurnstileJSONUnsupported(item) {
				continue
			}
			if written > 0 {
				buffer.WriteByte(',')
			}
			if stringKey, ok := key.(conversationTurnstileJSString); ok {
				if err := writeConversationTurnstileJSONStringUnits(buffer, stringKey.units); err != nil {
					return err
				}
			} else {
				units, errUnits := conversationTurnstileUTF16UnitsWithBufferBudget(buffer, conversationTurnstileString(key))
				if errUnits != nil {
					return errUnits
				}
				if err := writeConversationTurnstileJSONStringUnits(buffer, units); err != nil {
					return err
				}
			}
			buffer.WriteByte(':')
			if err := writeConversationTurnstileJSON(buffer, item, seen, depth+1, nodes); err != nil {
				return err
			}
			written++
		}
		buffer.WriteByte('}')
		return nil
	case *conversationTurnstileProcessMapRef:
		buffer.WriteString("{}")
		return nil
	case conversationTurnstileObjectRef:
		buffer.WriteString("{}")
		return nil
	case *conversationTurnstileArray:
		if typed == nil {
			buffer.WriteString("null")
			return nil
		}
		visit := conversationTurnstileJSONVisit{kind: 'a', ptr: reflect.ValueOf(typed).Pointer()}
		if seen[visit] {
			return fmt.Errorf("conversation turnstile JSON contains a cycle")
		}
		seen[visit] = true
		defer delete(seen, visit)
		buffer.WriteByte('[')
		for index, item := range typed.items {
			if index > 0 {
				buffer.WriteByte(',')
			}
			if conversationTurnstileJSONUnsupported(item) {
				buffer.WriteString("null")
				continue
			}
			if err := writeConversationTurnstileJSON(buffer, item, seen, depth+1, nodes); err != nil {
				return err
			}
		}
		buffer.WriteByte(']')
		return nil
	case []any:
		if typed == nil {
			buffer.WriteString("null")
			return nil
		}
		visit := conversationTurnstileJSONVisit{kind: 'a', ptr: reflect.ValueOf(typed).Pointer()}
		if visit.ptr != 0 && seen[visit] {
			return fmt.Errorf("conversation turnstile JSON contains a cycle")
		}
		if visit.ptr != 0 {
			seen[visit] = true
			defer delete(seen, visit)
		}
		buffer.WriteByte('[')
		for index, item := range typed {
			if index > 0 {
				buffer.WriteByte(',')
			}
			if conversationTurnstileJSONUnsupported(item) {
				buffer.WriteString("null")
				continue
			}
			if err := writeConversationTurnstileJSON(buffer, item, seen, depth+1, nodes); err != nil {
				return err
			}
		}
		buffer.WriteByte(']')
		return nil
	case map[string]any:
		if buffer.reserve != nil {
			if err := buffer.reserve(len(typed) * 16); err != nil {
				return err
			}
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		conversationTurnstileSortStringKeys(keys)
		buffer.WriteByte('{')
		written := 0
		for _, key := range keys {
			item := typed[key]
			if conversationTurnstileJSONUnsupported(item) {
				continue
			}
			if written > 0 {
				buffer.WriteByte(',')
			}
			units, errUnits := conversationTurnstileUTF16UnitsWithBufferBudget(buffer, key)
			if errUnits != nil {
				return errUnits
			}
			if err := writeConversationTurnstileJSONStringUnits(buffer, units); err != nil {
				return err
			}
			buffer.WriteByte(':')
			if err := writeConversationTurnstileJSON(buffer, item, seen, depth+1, nodes); err != nil {
				return err
			}
			written++
		}
		buffer.WriteByte('}')
		return nil
	case conversationTurnstileCallable, conversationTurnstileUndefinedValue:
		buffer.WriteString("null")
		return nil
	case string:
		units, err := conversationTurnstileUTF16UnitsWithBufferBudget(buffer, typed)
		if err != nil {
			return err
		}
		return writeConversationTurnstileJSONStringUnits(buffer, units)
	case conversationTurnstileJSString:
		return writeConversationTurnstileJSONStringUnits(buffer, typed.units)
	case *conversationTurnstileLocationRef:
		buffer.WriteString("{}")
		return nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			buffer.WriteString("null")
			return nil
		}
		if typed == 0 {
			buffer.WriteByte('0')
			return nil
		}
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	buffer.Write(payload)
	return nil
}

func conversationTurnstileUTF16UnitsWithBufferBudget(buffer *conversationTurnstileLimitedBuffer, value string) ([]uint16, error) {
	if buffer != nil && buffer.reserve != nil {
		if err := buffer.reserve(conversationTurnstileUTF16Length(value) * 2); err != nil {
			return nil, err
		}
	}
	return conversationTurnstileUTF16Units(value), nil
}

func writeConversationTurnstileJSONStringUnits(buffer *conversationTurnstileLimitedBuffer, units []uint16) error {
	const hexadecimal = "0123456789abcdef"
	writeEscapedUnit := func(unit uint16) {
		_, _ = buffer.WriteString(`\u`)
		_ = buffer.WriteByte(hexadecimal[(unit>>12)&0xf])
		_ = buffer.WriteByte(hexadecimal[(unit>>8)&0xf])
		_ = buffer.WriteByte(hexadecimal[(unit>>4)&0xf])
		_ = buffer.WriteByte(hexadecimal[unit&0xf])
	}
	_ = buffer.WriteByte('"')
	for index := 0; index < len(units); index++ {
		unit := units[index]
		switch unit {
		case '"', '\\':
			_ = buffer.WriteByte('\\')
			_ = buffer.WriteByte(byte(unit))
		case '\b':
			_, _ = buffer.WriteString(`\b`)
		case '\f':
			_, _ = buffer.WriteString(`\f`)
		case '\n':
			_, _ = buffer.WriteString(`\n`)
		case '\r':
			_, _ = buffer.WriteString(`\r`)
		case '\t':
			_, _ = buffer.WriteString(`\t`)
		default:
			if unit < 0x20 {
				writeEscapedUnit(unit)
				continue
			}
			if unit >= 0xd800 && unit <= 0xdbff {
				if index+1 < len(units) && units[index+1] >= 0xdc00 && units[index+1] <= 0xdfff {
					runeValue := utf16.DecodeRune(rune(unit), rune(units[index+1]))
					_, _ = buffer.WriteString(string(runeValue))
					index++
					continue
				}
				writeEscapedUnit(unit)
				continue
			}
			if unit >= 0xdc00 && unit <= 0xdfff {
				writeEscapedUnit(unit)
				continue
			}
			_, _ = buffer.WriteString(string(rune(unit)))
		}
	}
	_ = buffer.WriteByte('"')
	return buffer.err
}

func conversationTurnstileJSONUnsupported(value any) bool {
	switch typed := value.(type) {
	case conversationTurnstileCallable, conversationTurnstileUndefinedValue:
		return true
	case conversationTurnstileObjectRef:
		return conversationTurnstileCallableObjectPath(typed.path)
	default:
		return false
	}
}

func conversationTurnstileCallableObjectPath(path string) bool {
	switch path {
	case "window.Array", "window.Object", "window.String", "window.Reflect.set",
		"window.Array.isArray", "window.Array.from",
		"window.performance.now", "window.Object.create", "window.Object.keys",
		"window.Math.random", "window.Math.abs", "window.String.fromCharCode":
		return true
	default:
		return false
	}
}
