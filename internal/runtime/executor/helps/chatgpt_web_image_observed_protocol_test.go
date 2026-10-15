package helps

import "testing"

func TestChatGPTWebImageAccumulatorObservedToolStream(t *testing.T) {
	// This reduced fixture preserves the 2026-10-15 web response structure.
	// Identifiers, continuation credentials and unrelated telemetry are replaced.
	events := []string{
		`{"type":"resume_conversation_token","kind":"topic","token":"fixture","conversation_id":"conversation-fixture"}`,
		`{"p":"","o":"add","v":{"message":{"id":"context-fixture","author":{"role":"assistant"},"content":{"content_type":"model_editable_context","model_set_context":""},"status":"finished_successfully","end_turn":null,"metadata":{"parent_id":"user-fixture"}},"conversation_id":"conversation-fixture","error":null,"error_code":null},"c":0}`,
		`{"v":{"message":{"id":"call-fixture","author":{"role":"assistant"},"content":{"content_type":"code","language":"python3","text":"{\"skipped_mainline\":true}"},"status":"in_progress","end_turn":false,"metadata":{"parent_id":"context-fixture"},"recipient":"t2uay3k.sj1i4kz"},"conversation_id":"conversation-fixture","error":null,"error_code":null},"c":1}`,
		`{"p":"/message/status","o":"replace","v":"finished_successfully"}`,
		`{"p":"","o":"add","v":{"message":{"id":"image-fixture","author":{"role":"tool","name":"t2uay3k.sj1i4kz"},"content":{"content_type":"multimodal_text","parts":[{"content_type":"image_asset_pointer","asset_pointer":"sediment://file_fixture","mime_type":"image/png","size_bytes":1919347,"width":1254,"height":1254,"metadata":{"generation":{"gen_size":"smimage","gen_size_v2":"24","width":1254,"height":1254,"orientation":"square","transparent_background":false},"is_no_auth_placeholder":null}}]},"status":"finished_successfully","end_turn":null,"metadata":{"image_gen_title":"Dog image","parent_id":"call-fixture"},"recipient":"all"},"conversation_id":"conversation-fixture","error":null,"error_code":null},"c":2}`,
		`{"type":"message_stream_complete","conversation_id":"conversation-fixture"}`,
		`[DONE]`,
	}
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "user-fixture"}}
	for index, event := range events {
		if _, err := accumulator.Apply([]byte(event)); err != nil {
			t.Fatalf("event %d: %v", index, err)
		}
	}
	if accumulator.ConversationID != "conversation-fixture" || len(accumulator.References) != 1 ||
		accumulator.References[0].Kind != "sediment" || accumulator.References[0].ID != "file_fixture" {
		t.Fatalf("image reference not captured: %#v", accumulator.References)
	}
	if !accumulator.StreamTerminal || accumulator.PendingOutput ||
		accumulator.FailureStatus != "" || accumulator.HasTerminalAssistantText() {
		t.Fatalf("unexpected final state: %#v", accumulator)
	}
}
