package chatgptweb

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestGoConversationTurnstileSolverClassifiesUnknownOpcode(t *testing.T) {
	const requirementsToken = "requirements"
	dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{[]any{36, "argument"}})
	_, err := (GoConversationTurnstileSolver{}).Solve(context.Background(), ConversationTurnstileSolveRequest{
		DX:                dx,
		RequirementsToken: requirementsToken,
		Reader:            zeroReader{},
		Now:               time.Now,
	})
	var compatibility *SentinelCompatibilityError
	if !errors.As(err, &compatibility) {
		t.Fatalf("Solve() error = %v, want SentinelCompatibilityError", err)
	}
	if compatibility.Kind != SentinelCompatibilityUnknownOpcode {
		t.Fatalf("compatibility kind = %q", compatibility.Kind)
	}
	if compatibility.ProgramKind != SentinelProgramTurnstile || compatibility.Operation != "opcode:36" || compatibility.OpcodeSignature == "" {
		t.Fatalf("compatibility error = %+v", compatibility)
	}
}

func TestGoConversationTurnstileSolverClassifiesUnsupportedBoundBuiltinProperty(t *testing.T) {
	const requirementsToken = "requirements"
	tests := []struct {
		name      string
		receiver  any
		operation string
	}{
		{name: "string", receiver: "value", operation: "String.futureMethod"},
		{name: "array", receiver: []any{"value"}, operation: "Array.futureMethod"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{
				[]any{2, 40, test.receiver},
				[]any{2, 41, "futureMethod"},
				[]any{24, 42, 40, 41},
			})
			_, err := (GoConversationTurnstileSolver{}).Solve(context.Background(), ConversationTurnstileSolveRequest{
				DX:                dx,
				RequirementsToken: requirementsToken,
				Reader:            zeroReader{},
				Now:               time.Now,
			})
			var compatibility *SentinelCompatibilityError
			if !errors.As(err, &compatibility) || compatibility.Kind != SentinelCompatibilityUnsupportedValue {
				t.Fatalf("Solve() error = %v, compatibility = %+v", err, compatibility)
			}
			if compatibility.Operation != test.operation {
				t.Fatalf("operation = %q, want %q", compatibility.Operation, test.operation)
			}
		})
	}
}

func TestConversationTurnstileJavaScriptOnlyRegexpRecognizesValidatedECMAScriptFeatures(t *testing.T) {
	for _, pattern := range []string{
		`(?=foo)bar`,
		`^foo(?=bar)$`,
		`^foo(?<=bar)$`,
		`foo(?=bar)[a-z]`,
		`foo(?=bar)(?=baz)`,
		`(foo)\1`,
		`(foo)\2`,
		`(?=a)+`,
		`(?<word>foo)\k<word>`,
		`\u0061`,
		`\u00zz`,
		`\xzz`,
		`\c1`,
		`\k`,
		`[]`,
		`[^]`,
		`a{1001}`,
		`\A`,
		`\z`,
		`\a`,
	} {
		if !conversationTurnstileJavaScriptOnlyRegexp(pattern) {
			t.Fatalf("pattern %q was not recognized as a supported JavaScript-only regular expression", pattern)
		}
	}
	for _, pattern := range []string{
		`(`,
		`(?=foo`,
		`(?<=foo)+`,
		`(?<word>foo)\k<missing>`,
		`[\k](?<word>foo)`,
		`a{2,1}`,
	} {
		if conversationTurnstileJavaScriptOnlyRegexp(pattern) {
			t.Fatalf("pattern %q was incorrectly classified as a safe JavaScript-only regular expression", pattern)
		}
	}
}

func TestConversationTurnstileRegexpKeepsEquivalentRE2EscapesOnGoPath(t *testing.T) {
	for _, pattern := range []string{`\/`, `\-`, `\_`, `[\/]`, `[\-]`, `[\_]`} {
		vm := &conversationTurnstileVM{
			ctx:                 context.Background(),
			memoryBudget:        &conversationTurnstileMemoryBudget{},
			executionBudget:     &conversationTurnstileExecutionBudget{maxSteps: 100},
			compatibilityErrors: true,
		}
		if _, err := vm.compileRegexp(pattern); err != nil {
			t.Errorf("compileRegexp(%q) error = %v", pattern, err)
		}
	}
}

func TestConversationTurnstileRegexpFallsBackForRE2OnlySemantics(t *testing.T) {
	for _, pattern := range []string{
		`\x{41}`,
		`\p{Greek}`,
		`\P{Latin}`,
		`[[:alpha:]]`,
		`.`,
		`^..$`,
		`\s`,
		`\S`,
		`\D`,
		`\W`,
		`[^a]`,
		`[\B]`,
		`[ -￿]`,
		`[a[:alpha:]]`,
		`(?<x>a)|(?<x>b)`,
		`(?:(?<x>a)|(?:(?<x>b)))`,
		`(?i:foo)`,
		`(?im-s:foo)`,
		`(?i-:foo)`,
		`(?ims-:foo)`,
		`(?-i:foo)`,
		`(?<=a){x}`,
		`(?<=a){`,
		`\é`,
		`[\é]`,
		`[\t-￿]`,
		`[\x09-￿]`,
		`[\u0009-￿]`,
		`\0`,
		`\123`,
		`\400`,
		`emoji😀+`,
	} {
		vm := &conversationTurnstileVM{
			ctx:                 context.Background(),
			memoryBudget:        &conversationTurnstileMemoryBudget{},
			executionBudget:     &conversationTurnstileExecutionBudget{maxSteps: 100},
			compatibilityErrors: true,
		}
		_, err := vm.compileRegexp(pattern)
		var compatibility *SentinelCompatibilityError
		if !errors.As(err, &compatibility) || compatibility.Kind != SentinelCompatibilityUnsupportedValue {
			t.Errorf("compileRegexp(%q) error = %#v", pattern, err)
		}
	}
}

func TestConversationTurnstileRegexpFallsBackForIsolatedUTF16Surrogate(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:                 context.Background(),
		memoryBudget:        &conversationTurnstileMemoryBudget{},
		executionBudget:     &conversationTurnstileExecutionBudget{maxSteps: 100},
		compatibilityErrors: true,
	}
	_, err := vm.compileRegexp(conversationTurnstileJSString{units: []uint16{'[', 0xd800, ']'}})
	var compatibility *SentinelCompatibilityError
	if !errors.As(err, &compatibility) || compatibility.Kind != SentinelCompatibilityUnsupportedValue || !strings.HasPrefix(compatibility.Operation, "RegExp:") {
		t.Fatalf("compileRegexp() error = %v, compatibility = %+v", err, compatibility)
	}
}

func TestConversationTurnstileRegexpRejectsInvalidECMAScriptExtensions(t *testing.T) {
	for _, pattern := range []string{
		`(?i)foo`, `(?ii:foo)`, `(?i-i:foo)`, `(?-:foo)`, `(?u:foo)`,
		`(?P<word>foo)`, `(?<x>a)(?<x>b)`, `(?<x>a)|(?<x>b)(?<x>c)`,
		`(?:a|(?<x>b))(?:(?<x>c)|d)`,
		`(?<=a){1}`,
	} {
		vm := &conversationTurnstileVM{
			ctx:                 context.Background(),
			memoryBudget:        &conversationTurnstileMemoryBudget{},
			executionBudget:     &conversationTurnstileExecutionBudget{maxSteps: 100},
			compatibilityErrors: true,
		}
		_, err := vm.compileRegexp(pattern)
		if err == nil {
			t.Errorf("compileRegexp(%q) error = nil", pattern)
			continue
		}
		var compatibility *SentinelCompatibilityError
		if errors.As(err, &compatibility) {
			t.Errorf("compileRegexp(%q) returned compatibility fallback: %#v", pattern, err)
		}
	}
}

func TestConversationTurnstileNamedCaptureAnalysisUsesLinearStorage(t *testing.T) {
	const groups = 1024
	var pattern strings.Builder
	for index := 0; index < groups; index++ {
		fmt.Fprintf(&pattern, "(?<group%d>", index)
	}
	pattern.WriteByte('x')
	pattern.WriteString(strings.Repeat(")", groups))
	value := pattern.String()
	duplicate, invalid, hasNamedCaptures, workLimitExceeded := conversationTurnstileRegexpNamedCaptureAnalysis(value)
	if duplicate || invalid || !hasNamedCaptures || workLimitExceeded {
		t.Fatalf("analysis = duplicate:%v invalid:%v named:%v limited:%v", duplicate, invalid, hasNamedCaptures, workLimitExceeded)
	}
	benchmark := testing.Benchmark(func(b *testing.B) {
		for b.Loop() {
			conversationTurnstileRegexpNamedCaptureAnalysis(value)
		}
	})
	if allocated := benchmark.AllocedBytesPerOp(); allocated > int64(len(value))*128 {
		t.Fatalf("named capture analysis allocated %d bytes for %d-byte pattern", allocated, len(value))
	}
}

func TestConversationTurnstileNamedCaptureAnalysisBoundsDuplicateWork(t *testing.T) {
	var pattern strings.Builder
	for index := 0; pattern.Len() < conversationTurnstileMaxRegexpBytes-32; index++ {
		if index > 0 {
			pattern.WriteByte('|')
		}
		pattern.WriteString(`(?<same>x)`)
	}
	value := pattern.String()
	duplicate, invalid, hasNamedCaptures, workLimitExceeded := conversationTurnstileRegexpNamedCaptureAnalysis(value)
	if invalid || !hasNamedCaptures || !duplicate || !workLimitExceeded {
		t.Fatalf("analysis = duplicate:%v invalid:%v named:%v limited:%v", duplicate, invalid, hasNamedCaptures, workLimitExceeded)
	}
}

func TestConversationSentinelCompatibilitySignatureScopesValueSensitiveFallbacks(t *testing.T) {
	base := strings.Repeat("a", sha256.Size*2)
	first := conversationSentinelCompatibilitySignature(base, &SentinelCompatibilityError{
		Kind:      SentinelCompatibilityMissingEnvironment,
		Operation: "window.navigator.futureCapability",
	})
	second := conversationSentinelCompatibilitySignature(base, &SentinelCompatibilityError{
		Kind:      SentinelCompatibilityMissingEnvironment,
		Operation: "window.navigator.otherCapability",
	})
	if first == base || first == second {
		t.Fatalf("compatibility signatures = %q and %q", first, second)
	}
	unknown := conversationSentinelCompatibilitySignature(base, &SentinelCompatibilityError{
		Kind:      SentinelCompatibilityUnknownOpcode,
		Operation: "99",
	})
	if unknown != base {
		t.Fatalf("unknown opcode signature = %q, want %q", unknown, base)
	}
}

func TestConversationTurnstileRegexpValidationIsLinearForMalformedRepeats(t *testing.T) {
	pattern := "(?=a)" + strings.Repeat("{", conversationTurnstileMaxRegexpBytes-5)
	started := time.Now()
	_ = conversationTurnstileJavaScriptOnlyRegexp(pattern)
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("malformed repeat validation took %s", elapsed)
	}
}

func TestGoConversationTurnstileSolverDoesNotClassifyMalformedInstructions(t *testing.T) {
	const requirementsToken = "requirements"
	tests := []struct {
		name    string
		program []any
	}{
		{name: "empty instruction", program: []any{[]any{}}},
		{name: "string opcode", program: []any{[]any{"unknown"}}},
		{name: "unbound dynamic register", program: []any{[]any{40}}},
		{name: "bound non-callable", program: []any{[]any{2, 40, "literal"}, []any{40}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dx := encodeConversationTurnstileProgram(t, requirementsToken, test.program)
			_, err := (GoConversationTurnstileSolver{}).Solve(context.Background(), ConversationTurnstileSolveRequest{
				DX:                dx,
				RequirementsToken: requirementsToken,
				Reader:            zeroReader{},
				Now:               time.Now,
			})
			var compatibility *SentinelCompatibilityError
			if errors.As(err, &compatibility) {
				t.Fatalf("Solve() error = %v, want ordinary VM behavior", err)
			}
		})
	}
}

func TestGoConversationTurnstileSolverClassifiesRequiredBrowserProperty(t *testing.T) {
	const requirementsToken = "requirements"
	dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{2, 40, "navigator"},
		[]any{6, 41, 10, 40},
		[]any{2, 42, "futureCapability"},
		[]any{6, 43, 41, 42},
		[]any{7, 43},
	})
	_, err := (GoConversationTurnstileSolver{}).Solve(context.Background(), ConversationTurnstileSolveRequest{
		DX:                dx,
		RequirementsToken: requirementsToken,
		Reader:            zeroReader{},
		Now:               time.Now,
	})
	var compatibility *SentinelCompatibilityError
	if !errors.As(err, &compatibility) || compatibility.Kind != SentinelCompatibilityMissingEnvironment {
		t.Fatalf("Solve() error = %v, compatibility = %+v", err, compatibility)
	}
	if compatibility.Operation != "window.navigator.futureCapability" {
		t.Fatalf("operation = %q", compatibility.Operation)
	}
}

func TestGoConversationTurnstileSolverAllowsOptionalBrowserPropertyProbe(t *testing.T) {
	const requirementsToken = "requirements"
	dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{2, 40, "futureAPI"},
		[]any{6, 41, 10, 40},
		[]any{23, 41, 99},
	})
	_, err := (GoConversationTurnstileSolver{}).Solve(t.Context(), ConversationTurnstileSolveRequest{
		DX:                dx,
		RequirementsToken: requirementsToken,
		Reader:            zeroReader{},
		Now:               time.Now,
	})
	var compatibility *SentinelCompatibilityError
	if errors.As(err, &compatibility) {
		t.Fatalf("optional property probe error = %v", err)
	}
}

func TestGoConversationTurnstileSolverClassifiesChainedMissingBrowserProperty(t *testing.T) {
	const requirementsToken = "requirements"
	dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{2, 40, "futureAPI"},
		[]any{6, 41, 10, 40},
		[]any{2, 42, "method"},
		[]any{6, 43, 41, 42},
	})
	_, err := (GoConversationTurnstileSolver{}).Solve(context.Background(), ConversationTurnstileSolveRequest{
		DX:                dx,
		RequirementsToken: requirementsToken,
		Reader:            zeroReader{},
		Now:               time.Now,
	})
	var compatibility *SentinelCompatibilityError
	if !errors.As(err, &compatibility) || compatibility.Kind != SentinelCompatibilityMissingEnvironment {
		t.Fatalf("Solve() error = %v, compatibility = %+v", err, compatibility)
	}
	if compatibility.Operation != "window.futureAPI.method" {
		t.Fatalf("operation = %q", compatibility.Operation)
	}
}

func TestGoConversationTurnstileSolverClassifiesConsumedMissingBrowserProperty(t *testing.T) {
	const requirementsToken = "requirements"
	tests := []struct {
		name        string
		instruction []any
	}{
		{name: "append", instruction: []any{5, 41, 42}},
		{name: "comparison", instruction: []any{29, 43, 41, 42}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			program := []any{
				[]any{2, 40, "futureCapability"},
				[]any{6, 41, 10, 40},
				[]any{2, 42, "suffix"},
				test.instruction,
			}
			dx := encodeConversationTurnstileProgram(t, requirementsToken, program)
			_, err := (GoConversationTurnstileSolver{}).Solve(context.Background(), ConversationTurnstileSolveRequest{
				DX:                dx,
				RequirementsToken: requirementsToken,
				Reader:            zeroReader{},
				Now:               time.Now,
			})
			var compatibility *SentinelCompatibilityError
			if !errors.As(err, &compatibility) || compatibility.Kind != SentinelCompatibilityMissingEnvironment {
				t.Fatalf("Solve() error = %v, compatibility = %+v", err, compatibility)
			}
			if compatibility.Operation != "window.futureCapability" {
				t.Fatalf("operation = %q", compatibility.Operation)
			}
		})
	}
}

func TestConversationTurnstileDeletedKnownOpcodeIsNotCompatibilityError(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:                 context.Background(),
		values:              make(map[string]any),
		memoryBudget:        &conversationTurnstileMemoryBudget{},
		executionBudget:     &conversationTurnstileExecutionBudget{maxSteps: 100},
		compatibilityErrors: true,
		missingValues:       make(map[string]string),
	}
	vm.initialize([]any{[]any{7}}, "requirements")
	delete(vm.values, conversationTurnstileMapKey(7))
	err := vm.runQueue()
	var compatibility *SentinelCompatibilityError
	if err == nil || errors.As(err, &compatibility) {
		t.Fatalf("runQueue() error = %v, compatibility = %+v", err, compatibility)
	}
}

func TestConversationTurnstileMissingPropertyDoesNotWrapSafetyBudget(t *testing.T) {
	budget := &conversationTurnstileExecutionBudget{maxSteps: 100}
	vm := &conversationTurnstileVM{
		ctx:                 context.Background(),
		values:              make(map[string]any),
		memoryBudget:        &conversationTurnstileMemoryBudget{},
		executionBudget:     budget,
		compatibilityErrors: true,
		missingValues:       map[string]string{conversationTurnstileMapKey(40): "window.futureAPI"},
	}
	vm.set(40, conversationTurnstileObjectRef{path: "window.Object.keys"})
	vm.missingValues[conversationTurnstileMapKey(40)] = "window.futureAPI"
	budget.runtimeWork = conversationTurnstileMaxRuntimeWork
	_, err := vm.callReferenced(40, []any{"value"})
	var compatibility *SentinelCompatibilityError
	var fatal conversationTurnstileFatalError
	if !errors.As(err, &fatal) || errors.As(err, &compatibility) {
		t.Fatalf("callReferenced() error = %v, compatibility = %+v", err, compatibility)
	}
}

func TestConversationTurnstileClassifiesSupportedJavaScriptOnlyRegexp(t *testing.T) {
	valid := []string{
		"(?=future)future",
		"^foo.*(?=bar)$",
		"(foo)\\1",
		"(foo)\\2",
		"(?=a)+",
		"(?<word>foo)\\k<word>",
		"(?:foo|bar)(?!baz)",
		"\\u0061",
		"\\u00zz",
		"\\xzz",
		"\\c1",
		"\\k",
		"[]",
		"[^]",
		"a{1001}",
		"\\A",
		"\\z",
		"\\a",
		"\\Qquoted\\E",
		`\k<missing>`,
		`\k<1bad>`,
		`\k<>`,
		`\k<`,
		`(?<a\u0301>x)\k<á>`,
		`(?<x\u200cy>x)\k<x‌y>`,
		`(?<\u{10400}>x)\k<𐐀>`,
	}
	for _, pattern := range valid {
		vm := &conversationTurnstileVM{
			ctx:                 context.Background(),
			memoryBudget:        &conversationTurnstileMemoryBudget{},
			executionBudget:     &conversationTurnstileExecutionBudget{maxSteps: 100},
			compatibilityErrors: true,
			opcodeSignature:     "signature",
		}
		_, err := vm.compileRegexp(pattern)
		var compatibility *SentinelCompatibilityError
		if !errors.As(err, &compatibility) || compatibility.Kind != SentinelCompatibilityUnsupportedValue || !strings.HasPrefix(compatibility.Operation, "RegExp:") {
			t.Errorf("compileRegexp(%q) error = %v, compatibility = %+v", pattern, err, compatibility)
		}
	}
	invalid := []string{
		"(?=future",
		"(?<=a)*",
		"(?<word>foo)\\k<missing>",
		"[\\k](?<word>foo)",
		"(?<1bad>foo)\\k<1bad>",
		"[unterminated(?=value)",
		"(?=a)a{2,1}",
		`(?<\u0030>x)`,
		`(?<\uD800>x)`,
	}
	for _, pattern := range invalid {
		vm := &conversationTurnstileVM{
			ctx:                 context.Background(),
			memoryBudget:        &conversationTurnstileMemoryBudget{},
			executionBudget:     &conversationTurnstileExecutionBudget{maxSteps: 100},
			compatibilityErrors: true,
			opcodeSignature:     "signature",
		}
		_, err := vm.compileRegexp(pattern)
		var compatibility *SentinelCompatibilityError
		if errors.As(err, &compatibility) {
			t.Errorf("compileRegexp(%q) error = %v, compatibility = %+v", pattern, err, compatibility)
		}
	}
}

func TestConversationTurnstileProcessMapClearsMissingPropertySources(t *testing.T) {
	vm := &conversationTurnstileVM{
		ctx:                 context.Background(),
		values:              make(map[string]any),
		memoryBudget:        &conversationTurnstileMemoryBudget{},
		executionBudget:     &conversationTurnstileExecutionBudget{maxSteps: 100},
		compatibilityErrors: true,
		missingValues:       make(map[string]string),
	}
	vm.set(40, conversationTurnstileUndefined)
	key := conversationTurnstileMapKey(40)
	vm.missingValues[key] = "window.futureAPI"
	processMap := &conversationTurnstileProcessMapRef{vm: vm}
	remove := processMap.method("delete").(conversationTurnstileCallable)
	if _, err := remove.invoke([]any{40}); err != nil {
		t.Fatal(err)
	}
	if vm.missingValues[key] != "" {
		t.Fatalf("missing source after delete = %q", vm.missingValues[key])
	}
	vm.set(41, conversationTurnstileUndefined)
	vm.missingValues[conversationTurnstileMapKey(41)] = "window.futureAPI"
	clearMap := processMap.method("clear").(conversationTurnstileCallable)
	if _, err := clearMap.invoke(nil); err != nil {
		t.Fatal(err)
	}
	if len(vm.missingValues) != 0 {
		t.Fatalf("missing sources after clear = %#v", vm.missingValues)
	}
}

func TestConversationTurnstileMissingPropertyKeyErrorIsNotCompatibilityError(t *testing.T) {
	for _, operation := range []struct {
		name string
		call func(*conversationTurnstileVM, []any) (any, error)
	}{
		{name: "property", call: (*conversationTurnstileVM).opProperty},
		{name: "bound property", call: (*conversationTurnstileVM).opBoundProperty},
	} {
		t.Run(operation.name, func(t *testing.T) {
			vm := &conversationTurnstileVM{
				ctx:                 context.Background(),
				values:              make(map[string]any),
				memoryBudget:        &conversationTurnstileMemoryBudget{},
				executionBudget:     &conversationTurnstileExecutionBudget{maxSteps: 100},
				compatibilityErrors: true,
				missingValues:       make(map[string]string),
			}
			vm.set(40, conversationTurnstileUndefined)
			vm.missingValues[conversationTurnstileMapKey(40)] = "window.futureAPI"
			cyclicKey := []any{nil}
			cyclicKey[0] = cyclicKey
			vm.set(41, cyclicKey)
			_, err := operation.call(vm, []any{42, 40, 41})
			var compatibility *SentinelCompatibilityError
			if err == nil || errors.As(err, &compatibility) {
				t.Fatalf("operation error = %v, compatibility = %+v", err, compatibility)
			}
		})
	}
}

func TestConversationTurnstileMissingPropertyMetadataUsesMemoryBudget(t *testing.T) {
	budget := &conversationTurnstileMemoryBudget{used: conversationTurnstileMaxRuntimeBytes - 1}
	vm := &conversationTurnstileVM{
		ctx:                 context.Background(),
		values:              make(map[string]any),
		memoryBudget:        budget,
		executionBudget:     &conversationTurnstileExecutionBudget{maxSteps: 100},
		compatibilityErrors: true,
	}
	vm.setMissingValue(40, "window.futureAPI")
	var fatal conversationTurnstileFatalError
	if !errors.As(vm.fatalErr, &fatal) {
		t.Fatalf("fatalErr = %v", vm.fatalErr)
	}
	if len(vm.missingValues) != 0 {
		t.Fatalf("missingValues = %#v", vm.missingValues)
	}
}

func TestGoConversationTurnstileSolverDoesNotFallbackForMalformedLimitsOrCancellation(t *testing.T) {
	tests := []struct {
		name  string
		ctx   context.Context
		dx    string
		token string
	}{
		{name: "malformed", ctx: context.Background(), dx: "%%%", token: "requirements"},
		{name: "missing token", ctx: context.Background(), dx: "challenge"},
		{name: "canceled", ctx: canceledContext(), dx: "challenge", token: "requirements"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := (GoConversationTurnstileSolver{}).Solve(test.ctx, ConversationTurnstileSolveRequest{
				DX:                test.dx,
				RequirementsToken: test.token,
				Reader:            zeroReader{},
				Now:               time.Now,
			})
			if err == nil {
				t.Fatal("Solve() error = nil")
			}
			var compatibility *SentinelCompatibilityError
			if errors.As(err, &compatibility) {
				t.Fatalf("Solve() error = %v, want non-compatibility error", err)
			}
		})
	}
}

func TestConversationSentinelOpcodeSignatureTracksInstructionShape(t *testing.T) {
	const requirementsToken = "requirements"
	signature := func(program []any) string {
		t.Helper()
		signature, err := conversationSentinelProgramSignatureForDX(
			t.Context(),
			encodeConversationTurnstileProgram(t, requirementsToken, program),
			requirementsToken,
		)
		if err != nil {
			t.Fatal(err)
		}
		return signature
	}
	firstProgram := []any{
		[]any{2, 40, "secret-one"},
		[]any{99, []any{"value-one", 1}},
	}
	first := signature(firstProgram)
	if repeated := signature(firstProgram); repeated != first {
		t.Fatalf("stable signatures differ: %q != %q", repeated, first)
	}
	changed := signature([]any{
		[]any{2, 40, "secret-two"},
		[]any{99, []any{"value-one", 1}},
	})
	if changed != first {
		t.Fatalf("changed literal signature = %q, want %q", changed, first)
	}
	nested := signature([]any{
		[]any{2, 40, "secret-one"},
		[]any{99, []any{"value-one", 2}},
	})
	if nested == first {
		t.Fatalf("changed numeric operand signature = %q, want distinct value", nested)
	}
	reordered := signature([]any{
		[]any{99, []any{"value-one", 1}},
		[]any{2, 40, "secret-one"},
	})
	if reordered == first {
		t.Fatalf("reordered signature = %q, want distinct value", reordered)
	}
	changedOpcode := signature([]any{
		[]any{2, 40, "secret-one"},
		[]any{98, []any{"value-one", 1}},
	})
	if changedOpcode == first {
		t.Fatalf("changed opcode signature = %q, want distinct value", changedOpcode)
	}
	firstObject := signature([]any{[]any{99, map[string]any{"a": 1, "value": "dynamic-one"}}})
	reorderedObject := signature([]any{[]any{99, map[string]any{"value": "dynamic-two", "a": 1}}})
	if reorderedObject != firstObject {
		t.Fatalf("reordered object signature = %q, want stable %q", reorderedObject, firstObject)
	}
	changedObject := signature([]any{[]any{99, map[string]any{"b": 1, "value": "dynamic-one"}}})
	if changedObject == firstObject {
		t.Fatalf("changed object signature = %q, want distinct value", changedObject)
	}
	orderedObject := func(keys ...string) *conversationTurnstileOrderedMap {
		object := newConversationTurnstileOrderedMap()
		for _, key := range keys {
			object.set(key, "dynamic-value")
		}
		return object
	}
	orderedFirst, err := conversationSentinelProgramSignature(t.Context(), []any{[]any{99, orderedObject("a", "b")}})
	if err != nil {
		t.Fatal(err)
	}
	orderedSecond, err := conversationSentinelProgramSignature(t.Context(), []any{[]any{99, orderedObject("b", "a")}})
	if err != nil {
		t.Fatal(err)
	}
	if orderedFirst == orderedSecond {
		t.Fatalf("ordered object signatures = %q, want insertion order to differ", orderedFirst)
	}
}

func TestConversationSentinelOpcodeSignatureHonorsTraversalCancellation(t *testing.T) {
	program := make([]any, 300)
	for index := range program {
		program[index] = "dynamic-value"
	}
	ctx := &cancelAfterErrChecksContext{Context: t.Context(), cancelAt: 2}
	_, err := conversationSentinelProgramSignature(ctx, program)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("signature error = %v, want context.Canceled", err)
	}
}

func TestConversationSentinelOrderedMapSignatureChargesTemporaryMemory(t *testing.T) {
	values := newConversationTurnstileOrderedMap()
	values.set("a", 1)
	values.set("b", 2)
	budget := &conversationTurnstileMemoryBudget{used: conversationTurnstileMaxRuntimeBytes - 1}
	nodes := 0
	var output strings.Builder
	err := conversationSentinelWriteOrderedMapShape(t.Context(), &output, values, &nodes, budget, 0)
	if err == nil || !strings.Contains(err.Error(), "runtime allocation exceeds") {
		t.Fatalf("ordered map signature error = %v", err)
	}
}

func TestConversationSentinelOpcodeSignatureChecksInstructionAndMapWork(t *testing.T) {
	t.Run("instruction arguments", func(t *testing.T) {
		instruction := make([]any, 301)
		instruction[0] = 2
		for index := 1; index < len(instruction); index++ {
			instruction[index] = "dynamic-value"
		}
		ctx := &cancelAfterErrChecksContext{Context: t.Context(), cancelAt: 2}
		if _, err := conversationSentinelProgramSignature(ctx, []any{instruction}); !errors.Is(err, context.Canceled) {
			t.Fatalf("signature error = %v, want context.Canceled", err)
		}
	})
	t.Run("map key collection", func(t *testing.T) {
		values := make(map[string]any, 300)
		for index := 0; index < 300; index++ {
			values[fmt.Sprintf("key-%03d", index)] = "dynamic-value"
		}
		ctx := &cancelAfterErrChecksContext{Context: t.Context(), cancelAt: 2}
		if _, err := conversationSentinelProgramSignature(ctx, []any{values}); !errors.Is(err, context.Canceled) {
			t.Fatalf("signature error = %v, want context.Canceled", err)
		}
	})
	t.Run("map key sort", func(t *testing.T) {
		keys := make([]string, 1024)
		for index := range keys {
			keys[index] = fmt.Sprintf("key-%04d", len(keys)-index)
		}
		ctx := &cancelAfterErrChecksContext{Context: t.Context(), cancelAt: 2}
		if err := conversationSentinelStableSortStrings(ctx, keys); !errors.Is(err, context.Canceled) {
			t.Fatalf("sort error = %v, want context.Canceled", err)
		}
	})
	t.Run("ordered map key collection", func(t *testing.T) {
		values := newConversationTurnstileOrderedMap()
		for index := 0; index < 300; index++ {
			values.set(fmt.Sprintf("key-%03d", index), "dynamic-value")
		}
		ctx := &cancelAfterErrChecksContext{Context: t.Context(), cancelAt: 2}
		if _, err := conversationSentinelProgramSignature(ctx, []any{values}); !errors.Is(err, context.Canceled) {
			t.Fatalf("signature error = %v, want context.Canceled", err)
		}
	})
}

func TestConversationSentinelCompatibilityErrorUsesPreparedSignature(t *testing.T) {
	const requirementsToken = "requirements"
	prepared, expected, err := prepareConversationSentinelProgramSignature(
		t.Context(),
		encodeConversationTurnstileProgram(t, requirementsToken, []any{[]any{36, 1}}),
		requirementsToken,
	)
	if err != nil {
		t.Fatal(err)
	}
	instruction, ok := conversationTurnstileSlice(prepared.program[0])
	if !ok {
		t.Fatal("prepared instruction is not an array")
	}
	instruction[1] = 2

	_, err = solvePreparedConversationTurnstile(t.Context(), prepared, ConversationTurnstileSolveRequest{
		RequirementsToken: requirementsToken,
		Reader:            zeroReader{},
		Now:               time.Now,
	})
	var compatibility *SentinelCompatibilityError
	if !errors.As(err, &compatibility) {
		t.Fatalf("solve error = %v, want SentinelCompatibilityError", err)
	}
	if compatibility.OpcodeSignature != expected {
		t.Fatalf("compatibility signature = %q, want prepared %q", compatibility.OpcodeSignature, expected)
	}
	changed, err := conversationSentinelProgramSignature(t.Context(), prepared.program)
	if err != nil {
		t.Fatal(err)
	}
	if changed == expected {
		t.Fatalf("mutated program signature = %q, want distinct value", changed)
	}
}

type cancelAfterErrChecksContext struct {
	context.Context
	calls    int
	cancelAt int
}

func (ctx *cancelAfterErrChecksContext) Err() error {
	ctx.calls++
	if ctx.calls >= ctx.cancelAt {
		return context.Canceled
	}
	return nil
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
