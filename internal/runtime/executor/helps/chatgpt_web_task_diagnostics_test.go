package helps

import (
	"reflect"
	"testing"
)

func TestChatGPTWebTaskPageDiagnosticsDistinguishesEmptyAndUnrecognized(t *testing.T) {
	// The authenticated web client returned this empty page on 2026-09-11.
	// It is valid and does not imply missing parser support or a failed task.
	for _, test := range []struct {
		payload    string
		recognized bool
		wantError  bool
	}{
		{`{"tasks":[],"cursor":null}`, true, false},
		{`{}`, false, false},
		{`{"tasks":null}`, false, false},
		{`{"tasks":{}}`, false, false},
		{`{"tasks":`, false, true},
	} {
		t.Run(test.payload, func(t *testing.T) {
			state, err := CaptureChatGPTWebImageTasks([]byte(test.payload), "current", &ChatGPTWebImageAccumulator{})
			if (err != nil) != test.wantError || state.Diagnostics.Recognized != test.recognized {
				t.Fatalf("state = %+v, err = %v", state, err)
			}
			if state.Matched != 0 || state.AllTerminal() || len(state.Targets) != 0 {
				t.Fatalf("empty/unrecognized page produced a task: %+v", state)
			}
		})
	}
}

func TestChatGPTWebTaskPageDiagnosticsPreservesTurnIsolation(t *testing.T) {
	payload := []byte(`{"tasks":[null,"invalid",{},
		{"task_id":"imagegen_other","conversation_id":"other"},
		{"task_id":"imagegen_old","conversation_id":"current","parent_message_id":"old"},
		{"task_id":"imagegen_exact","conversation_id":"current","parent_message_id":"request","response_message_id":"response","status":"running"},
		{"type":"image_gen","conversation_id":"current","parent_message_id":"request"},
		{"task_id":"imagegen_noresponse","conversation_id":"current","parent_message_id":"request"}
	]}`)
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "request"}}
	state, err := CaptureChatGPTWebImageTasks(payload, "current", accumulator)
	if err != nil {
		t.Fatal(err)
	}
	want := ChatGPTWebImageTaskPageDiagnostics{
		Recognized: true, Records: 8, InvalidRecords: 2, ImageRecords: 5, MatchedRecords: 3,
		OtherConversationRecords: 1, IdentityMismatchRecords: 1, MissingTaskIDRecords: 1, MissingResponseIDRecords: 1,
	}
	if state.Diagnostics != want {
		t.Fatalf("diagnostics = %+v, want %+v", state.Diagnostics, want)
	}
	if !reflect.DeepEqual(accumulator.TaskIDs, []string{"imagegen_exact", "imagegen_noresponse"}) || state.Matched != 3 {
		t.Fatalf("unexpected matched tasks: %+v, IDs = %v", state, accumulator.TaskIDs)
	}
}

func TestChatGPTWebConversationImageDoesNotRequireAsyncTaskID(t *testing.T) {
	// Field subset of a successful official conversation response observed on
	// 2026-09-11. Identities and text are replaced; no image bytes are retained.
	payload := []byte(`{"conversation_id":"current","async_status":null,"current_node":"assistant","mapping":{
		"user":{"id":"user","parent":null,"children":["tool"],"message":{"id":"user","author":{"role":"user"},"content":{"content_type":"text","parts":["redacted"]}}},
		"tool":{"id":"tool","parent":"user","children":["assistant"],"message":{"id":"tool","author":{"role":"tool","name":"image_tool.variant"},"status":"finished_successfully","metadata":{"image_gen_title":"redacted","parent_id":"user"},"content":{"content_type":"multimodal_text","parts":[{"content_type":"image_asset_pointer","asset_pointer":"sediment://fixture-output","mime_type":"image/png","width":1024,"height":1024}]}}},
		"assistant":{"id":"assistant","parent":"tool","children":[],"message":{"id":"assistant","author":{"role":"assistant"},"status":"finished_successfully","end_turn":true,"metadata":{"is_complete":true,"parent_id":"tool"},"content":{"content_type":"text","parts":[""]}}}
	}}`)
	accumulator := &ChatGPTWebImageAccumulator{ConversationID: "current", Turn: ChatGPTWebImageTurn{MessageID: "user"}}
	if err := CaptureChatGPTWebImageConversation(payload, accumulator); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(accumulator.SedimentIDs, []string{"fixture-output"}) || len(accumulator.TaskIDs) != 0 {
		t.Fatalf("expected image output without async task identity: %+v", accumulator)
	}
}
