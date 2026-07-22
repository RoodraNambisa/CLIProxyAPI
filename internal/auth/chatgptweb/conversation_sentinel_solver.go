package chatgptweb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
)

// SentinelProgramKind identifies the SDK program surface needed for a task.
type SentinelProgramKind string

const (
	SentinelProgramTurnstile        SentinelProgramKind = "turnstile"
	SentinelProgramObserverCollect  SentinelProgramKind = "observer_collector"
	SentinelProgramObserverSnapshot SentinelProgramKind = "observer_snapshot"
)

// SentinelCompatibilityKind identifies a Go VM limitation that may use the SDK fallback.
type SentinelCompatibilityKind string

const (
	SentinelCompatibilityUnknownOpcode      SentinelCompatibilityKind = "unknown_opcode"
	SentinelCompatibilityUnsupportedValue   SentinelCompatibilityKind = "unsupported_operation_or_value"
	SentinelCompatibilityMissingEnvironment SentinelCompatibilityKind = "missing_browser_environment_property"
)

// SentinelCompatibilityError is returned only when the decoded challenge is valid
// but the Go VM cannot faithfully execute an SDK capability.
type SentinelCompatibilityError struct {
	Kind            SentinelCompatibilityKind
	ProgramKind     SentinelProgramKind
	OpcodeSignature string
	Operation       string
	Err             error
}

func (err *SentinelCompatibilityError) Error() string {
	if err == nil {
		return "sentinel compatibility error"
	}
	message := "sentinel " + string(err.ProgramKind) + " compatibility error: " + string(err.Kind)
	if operation := strings.TrimSpace(err.Operation); operation != "" {
		message += " (" + operation + ")"
	}
	if err.Err != nil {
		message += ": " + err.Err.Error()
	}
	return message
}

func (err *SentinelCompatibilityError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

// ConversationTurnstileSolveRequest contains deterministic inputs shared by
// the Go VM and the SDK fallback.
type ConversationTurnstileSolveRequest struct {
	DX                string
	RequirementsToken string
	Environment       ConversationTurnstileEnvironment
	Reader            io.Reader
	Now               func() time.Time
}

// ConversationTurnstileSolver solves a chat requirements Turnstile program.
type ConversationTurnstileSolver interface {
	Solve(context.Context, ConversationTurnstileSolveRequest) (string, error)
}

// GoConversationTurnstileSolver executes the bounded in-process Go VM.
type GoConversationTurnstileSolver struct{}

func (GoConversationTurnstileSolver) Solve(ctx context.Context, request ConversationTurnstileSolveRequest) (string, error) {
	return solveConversationTurnstileTokenWithEnvironment(
		ctx,
		request.DX,
		request.RequirementsToken,
		request.Environment,
		request.Reader,
		request.Now,
	)
}

func conversationSentinelProgramSignatureForDX(ctx context.Context, dx, requirementsToken string) (string, error) {
	_, signature, err := prepareConversationSentinelProgramSignature(ctx, dx, requirementsToken)
	return signature, err
}

func prepareConversationSentinelProgramSignature(ctx context.Context, dx, requirementsToken string) (*conversationTurnstilePreparedProgram, string, error) {
	prepared, err := prepareConversationTurnstileProgram(ctx, dx, requirementsToken, nil)
	if err != nil {
		return nil, "", err
	}
	signature, err := conversationSentinelProgramSignature(ctx, prepared.program)
	if err != nil {
		return nil, "", err
	}
	prepared.opcodeSignature = signature
	return prepared, signature, nil
}

func solvePreparedConversationTurnstile(ctx context.Context, prepared *conversationTurnstilePreparedProgram, request ConversationTurnstileSolveRequest) (string, error) {
	return executeConversationTurnstileProgram(
		ctx,
		prepared,
		request.Environment,
		request.Reader,
		request.Now,
		defaultConversationTurnstileMaxSteps,
		0,
		nil,
		true,
	)
}

func conversationSentinelProgramSignature(ctx context.Context, program []any) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	digest := sha256.New()
	_, _ = io.WriteString(digest, "sentinel-opcode-shape-v2")
	nodes := 0
	memoryBudget := &conversationTurnstileMemoryBudget{}
	if err := conversationSentinelWriteProgramShape(ctx, digest, program, &nodes, memoryBudget, 0); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func conversationSentinelCompatibilitySignature(signature string, compatibility *SentinelCompatibilityError) string {
	if signature == "" || compatibility == nil || compatibility.Kind == SentinelCompatibilityUnknownOpcode {
		return signature
	}
	digest := sha256.New()
	_, _ = io.WriteString(digest, "sentinel-compatibility-v1\x00")
	_, _ = io.WriteString(digest, signature)
	_, _ = io.WriteString(digest, "\x00")
	_, _ = io.WriteString(digest, string(compatibility.Kind))
	_, _ = io.WriteString(digest, "\x00")
	_, _ = io.WriteString(digest, strings.TrimSpace(compatibility.Operation))
	return hex.EncodeToString(digest.Sum(nil))
}

func conversationSentinelWriteProgramShape(ctx context.Context, writer io.Writer, program []any, nodes *int, memoryBudget *conversationTurnstileMemoryBudget, depth int) error {
	if depth > conversationTurnstileMaxJSONDepth {
		return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile signature exceeds depth %d", conversationTurnstileMaxJSONDepth)}
	}
	_, _ = io.WriteString(writer, "[")
	for _, value := range program {
		instruction, ok := conversationTurnstileSlice(value)
		if !ok {
			if err := conversationSentinelWriteValueShape(ctx, writer, value, nodes, memoryBudget, depth+1); err != nil {
				return err
			}
			continue
		}
		if err := conversationSentinelChargeSignatureNode(ctx, nodes); err != nil {
			return err
		}
		_, _ = io.WriteString(writer, "I")
		if len(instruction) == 0 {
			_, _ = io.WriteString(writer, "empty;")
			continue
		}
		if err := conversationSentinelChargeSignatureNode(ctx, nodes); err != nil {
			return err
		}
		if opcode, okOpcode := conversationSentinelOpcodeNumber(instruction[0]); okOpcode {
			_, _ = io.WriteString(writer, "O")
			_, _ = io.WriteString(writer, strconv.FormatInt(opcode, 10))
		} else {
			if err := conversationSentinelWriteValueShape(ctx, writer, instruction[0], nodes, memoryBudget, depth+1); err != nil {
				return err
			}
		}
		_, _ = io.WriteString(writer, ":")
		_, _ = io.WriteString(writer, strconv.Itoa(len(instruction)-1))
		for _, argument := range instruction[1:] {
			if err := conversationSentinelWriteValueShape(ctx, writer, argument, nodes, memoryBudget, depth+1); err != nil {
				return err
			}
		}
		_, _ = io.WriteString(writer, ";")
	}
	_, _ = io.WriteString(writer, "]")
	return nil
}

func conversationSentinelWriteValueShape(ctx context.Context, writer io.Writer, value any, nodes *int, memoryBudget *conversationTurnstileMemoryBudget, depth int) error {
	if err := conversationSentinelChargeSignatureNode(ctx, nodes); err != nil {
		return err
	}
	if nested, ok := conversationTurnstileSlice(value); ok {
		return conversationSentinelWriteProgramShape(ctx, writer, nested, nodes, memoryBudget, depth)
	}
	switch typed := value.(type) {
	case nil, conversationTurnstileExplicitNullValue:
		_, _ = io.WriteString(writer, "N")
	case conversationTurnstileUndefinedValue:
		_, _ = io.WriteString(writer, "U")
	case string, conversationTurnstileJSString:
		_, _ = io.WriteString(writer, "S")
	case bool:
		_, _ = io.WriteString(writer, "B"+strconv.FormatBool(typed))
	case int:
		_, _ = io.WriteString(writer, "D"+strconv.FormatInt(int64(typed), 10))
	case int64:
		_, _ = io.WriteString(writer, "D"+strconv.FormatInt(typed, 10))
	case uint64:
		_, _ = io.WriteString(writer, "D"+strconv.FormatUint(typed, 10))
	case float64:
		_, _ = io.WriteString(writer, "D"+strconv.FormatFloat(typed, 'g', -1, 64))
	case float32:
		_, _ = io.WriteString(writer, "D"+strconv.FormatFloat(float64(typed), 'g', -1, 32))
	case int8, int16, int32, uint, uint8, uint16, uint32:
		_, _ = io.WriteString(writer, fmt.Sprintf("D%v", typed))
	case *conversationTurnstileOrderedMap:
		if typed == nil {
			_, _ = io.WriteString(writer, "M0")
			break
		}
		if err := conversationSentinelWriteOrderedMapShape(ctx, writer, typed, nodes, memoryBudget, depth+1); err != nil {
			return err
		}
	case map[string]any:
		if err := conversationSentinelWriteMapShape(ctx, writer, typed, nodes, memoryBudget, depth+1); err != nil {
			return err
		}
	default:
		_, _ = io.WriteString(writer, "X")
	}
	return nil
}

func conversationSentinelWriteOrderedMapShape(ctx context.Context, writer io.Writer, values *conversationTurnstileOrderedMap, nodes *int, memoryBudget *conversationTurnstileMemoryBudget, depth int) error {
	if depth > conversationTurnstileMaxJSONDepth {
		return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile signature exceeds depth %d", conversationTurnstileMaxJSONDepth)}
	}
	keys, err := values.jsKeys(ctx, memoryBudget.reserve, nil)
	if err != nil {
		return err
	}
	_, _ = io.WriteString(writer, "M{")
	for _, propertyKey := range keys {
		_, mapKey := conversationTurnstilePropertyKey(propertyKey)
		if err := conversationSentinelWriteMapEntry(ctx, writer, conversationTurnstileString(propertyKey), values.values[mapKey], nodes, memoryBudget, depth); err != nil {
			return err
		}
	}
	_, _ = io.WriteString(writer, "}")
	return nil
}

func conversationSentinelWriteMapShape(ctx context.Context, writer io.Writer, values map[string]any, nodes *int, memoryBudget *conversationTurnstileMemoryBudget, depth int) error {
	if depth > conversationTurnstileMaxJSONDepth {
		return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile signature exceeds depth %d", conversationTurnstileMaxJSONDepth)}
	}
	if err := memoryBudget.reserve(len(values) * 32); err != nil {
		return err
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		if len(keys)&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if len(keys) >= conversationTurnstileMaxJSONNodes {
			return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile signature exceeds %d nodes", conversationTurnstileMaxJSONNodes)}
		}
		keys = append(keys, key)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := conversationSentinelStableSortStrings(ctx, keys); err != nil {
		return err
	}
	_, _ = io.WriteString(writer, "M{")
	for _, key := range keys {
		if err := conversationSentinelWriteMapEntry(ctx, writer, key, values[key], nodes, memoryBudget, depth); err != nil {
			return err
		}
	}
	_, _ = io.WriteString(writer, "}")
	return nil
}

func conversationSentinelStableSortStrings(ctx context.Context, values []string) error {
	if len(values) < 2 {
		return ctx.Err()
	}
	scratch := make([]string, len(values))
	source, destination := values, scratch
	for width := 1; width < len(values); width *= 2 {
		operations := 0
		for start := 0; start < len(values); start += width * 2 {
			middle := min(start+width, len(values))
			end := min(start+width*2, len(values))
			left, right, output := start, middle, start
			for left < middle || right < end {
				if operations&255 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				operations++
				if right < end && (left >= middle || source[right] < source[left]) {
					destination[output] = source[right]
					right++
				} else {
					destination[output] = source[left]
					left++
				}
				output++
			}
		}
		source, destination = destination, source
	}
	if &source[0] != &values[0] {
		copy(values, source)
	}
	return nil
}

func conversationSentinelWriteMapEntry(ctx context.Context, writer io.Writer, key string, value any, nodes *int, memoryBudget *conversationTurnstileMemoryBudget, depth int) error {
	if err := conversationSentinelChargeSignatureNode(ctx, nodes); err != nil {
		return err
	}
	_, _ = io.WriteString(writer, strconv.Itoa(len(key)))
	_, _ = io.WriteString(writer, ":")
	_, _ = io.WriteString(writer, key)
	_, _ = io.WriteString(writer, "=")
	if err := conversationSentinelWriteValueShape(ctx, writer, value, nodes, memoryBudget, depth); err != nil {
		return err
	}
	_, _ = io.WriteString(writer, ";")
	return nil
}

func conversationSentinelChargeSignatureNode(ctx context.Context, nodes *int) error {
	(*nodes)++
	if *nodes > conversationTurnstileMaxJSONNodes {
		return conversationTurnstileFatalError{message: fmt.Sprintf("conversation turnstile signature exceeds %d nodes", conversationTurnstileMaxJSONNodes)}
	}
	if *nodes&255 == 0 {
		return ctx.Err()
	}
	return nil
}

func conversationSentinelOpcodeNumber(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		if typed >= 0 {
			return int64(typed), true
		}
	case int64:
		if typed >= 0 {
			return typed, true
		}
	case float64:
		if typed >= 0 && typed < float64(math.MaxInt64) && typed == float64(int64(typed)) {
			return int64(typed), true
		}
	case float32:
		converted := float64(typed)
		if converted >= 0 && converted < float64(math.MaxInt64) && converted == float64(int64(converted)) {
			return int64(converted), true
		}
	}
	return 0, false
}

func conversationSentinelSupportedOpcode(opcode int64) bool {
	switch opcode {
	case 0, 1, 2, 3, 4, 5, 6, 7, 8,
		11, 12, 13, 14, 15,
		17, 18, 19, 20, 21, 22, 23, 24,
		25, 26, 27, 28, 29, 30,
		33, 34, 35:
		return true
	default:
		return false
	}
}

func conversationSentinelReservedOpcode(opcode int64) bool {
	return opcode >= 0 && opcode < 40
}
