package responses

import "github.com/tidwall/sjson"

// noteOutputIndex only finalizes a closed block once a later output is known.
// Interleaved blocks that are still open keep accepting their own deltas.
func (st *claudeToResponsesState) noteOutputIndex(index int, nextSeq func() int) [][]byte {
	if index <= st.LastOutputIndex {
		return nil
	}
	var out [][]byte
	if st.blockClosed(st.LastOutputIndex) {
		out = st.finalizeBlock(st.LastOutputIndex, "completed", nextSeq)
	}
	st.LastOutputIndex = index
	return out
}

func (st *claudeToResponsesState) blockClosed(index int) bool {
	if block := st.TextBlocks[index]; block != nil {
		return block.Done
	}
	if block := st.ReasoningBlocks[index]; block != nil {
		return block.Done
	}
	return st.FuncDone[index]
}

func (st *claudeToResponsesState) closeBlock(index int) {
	if block := st.TextBlocks[index]; block != nil {
		block.Done = true
	} else if block := st.ReasoningBlocks[index]; block != nil {
		block.Done = true
	} else if st.FuncCallIDs[index] != "" {
		st.FuncDone[index] = true
	}
}

func (st *claudeToResponsesState) outputItem(index int, status string) []byte {
	var item []byte
	if block := st.TextBlocks[index]; block != nil {
		item = claudeResponsesTextItem(block)
	} else if block := st.ReasoningBlocks[index]; block != nil {
		item = claudeResponsesReasoningItem(block)
	} else {
		args := ""
		if buf := st.FuncArgsBuf[index]; buf != nil {
			args = buf.String()
		}
		return buildClaudeResponsesToolItem(st.ToolIdentities, st.FuncNames[index], st.FuncCallIDs[index], args, status)
	}
	item, _ = sjson.SetBytes(item, "status", status)
	return item
}

func (st *claudeToResponsesState) finalizeBlock(idx int, status string, nextSeq func() int) [][]byte {
	if st.ItemDone[idx] || (st.TextBlocks[idx] == nil && st.ReasoningBlocks[idx] == nil && st.FuncCallIDs[idx] == "") {
		return nil
	}
	st.ItemDone[idx] = true
	var out [][]byte
	if block := st.TextBlocks[idx]; block != nil {
		block.Done = true
		done := []byte(`{"type":"response.output_text.done","sequence_number":0,"item_id":"","output_index":0,"content_index":0,"text":"","logprobs":[]}`)
		done, _ = sjson.SetBytes(done, "sequence_number", nextSeq())
		done, _ = sjson.SetBytes(done, "output_index", idx)
		done, _ = sjson.SetBytes(done, "item_id", block.ID)
		done, _ = sjson.SetBytes(done, "text", block.Text.String())
		out = append(out, emitEvent("response.output_text.done", done))
		partDone := []byte(`{"type":"response.content_part.done","sequence_number":0,"item_id":"","output_index":0,"content_index":0,"part":{"type":"output_text","annotations":[],"logprobs":[],"text":""}}`)
		partDone, _ = sjson.SetBytes(partDone, "sequence_number", nextSeq())
		partDone, _ = sjson.SetBytes(partDone, "output_index", idx)
		partDone, _ = sjson.SetBytes(partDone, "item_id", block.ID)
		partDone, _ = sjson.SetBytes(partDone, "part.text", block.Text.String())
		out = append(out, emitEvent("response.content_part.done", partDone))
		final := []byte(`{"type":"response.output_item.done","sequence_number":0,"output_index":0,"item":{"id":"","type":"message","status":"completed","content":[{"type":"output_text","text":""}],"role":"assistant"}}`)
		final, _ = sjson.SetBytes(final, "sequence_number", nextSeq())
		final, _ = sjson.SetBytes(final, "output_index", idx)
		final, _ = sjson.SetRawBytes(final, "item", st.outputItem(idx, status))
		out = append(out, emitEvent("response.output_item.done", final))
	} else if _, exists := st.FuncCallIDs[idx]; exists {
		args := ""
		if buf := st.FuncArgsBuf[idx]; buf != nil {
			if buf.Len() > 0 {
				args = buf.String()
			}
		}
		name, callID := st.FuncNames[idx], st.FuncCallIDs[idx]
		if args == "" && status == "completed" && !st.ToolIdentities[name].custom {
			args = "{}"
		}
		itemID := claudeResponsesToolItemID(st.ToolIdentities, name, callID)
		kind, field, value := "response.function_call_arguments.done", "arguments", args
		if st.ToolIdentities[name].custom {
			kind, field, value = "response.custom_tool_call_input.done", "input", claudeResponsesCustomInput(args)
			// Unwrap completed input without publishing partial JSON escapes.
			if value != "" {
				delta := []byte(`{"type":"response.custom_tool_call_input.delta","sequence_number":0,"item_id":"","output_index":0,"delta":""}`)
				delta, _ = sjson.SetBytes(delta, "sequence_number", nextSeq())
				delta, _ = sjson.SetBytes(delta, "item_id", itemID)
				delta, _ = sjson.SetBytes(delta, "output_index", idx)
				delta, _ = sjson.SetBytes(delta, "delta", value)
				out = append(out, emitEvent("response.custom_tool_call_input.delta", delta))
			}
		}
		fcDone := []byte(`{"type":"","sequence_number":0,"item_id":"","output_index":0}`)
		fcDone, _ = sjson.SetBytes(fcDone, "type", kind)
		fcDone, _ = sjson.SetBytes(fcDone, "sequence_number", nextSeq())
		fcDone, _ = sjson.SetBytes(fcDone, "item_id", itemID)
		fcDone, _ = sjson.SetBytes(fcDone, "output_index", idx)
		fcDone, _ = sjson.SetBytes(fcDone, field, value)
		out = append(out, emitEvent(kind, fcDone))
		itemDone := []byte(`{"type":"response.output_item.done","sequence_number":0,"output_index":0}`)
		itemDone, _ = sjson.SetBytes(itemDone, "sequence_number", nextSeq())
		itemDone, _ = sjson.SetBytes(itemDone, "output_index", idx)
		itemDone, _ = sjson.SetRawBytes(itemDone, "item", st.outputItem(idx, status))
		out = append(out, emitEvent("response.output_item.done", itemDone))
		st.FuncDone[idx] = true
	} else if block := st.ReasoningBlocks[idx]; block != nil {
		block.Done = true
		if !block.Redacted {
			full := block.Text.String()
			textDone := []byte(`{"type":"response.reasoning_summary_text.done","sequence_number":0,"item_id":"","output_index":0,"summary_index":0,"text":""}`)
			textDone, _ = sjson.SetBytes(textDone, "sequence_number", nextSeq())
			textDone, _ = sjson.SetBytes(textDone, "item_id", block.ID)
			textDone, _ = sjson.SetBytes(textDone, "output_index", idx)
			textDone, _ = sjson.SetBytes(textDone, "text", full)
			out = append(out, emitEvent("response.reasoning_summary_text.done", textDone))
			partDone := []byte(`{"type":"response.reasoning_summary_part.done","sequence_number":0,"item_id":"","output_index":0,"summary_index":0,"part":{"type":"summary_text","text":""}}`)
			partDone, _ = sjson.SetBytes(partDone, "sequence_number", nextSeq())
			partDone, _ = sjson.SetBytes(partDone, "item_id", block.ID)
			partDone, _ = sjson.SetBytes(partDone, "output_index", idx)
			partDone, _ = sjson.SetBytes(partDone, "part.text", full)
			out = append(out, emitEvent("response.reasoning_summary_part.done", partDone))
		}
		itemDone := []byte(`{"type":"response.output_item.done"}`)
		itemDone, _ = sjson.SetBytes(itemDone, "sequence_number", nextSeq())
		itemDone, _ = sjson.SetBytes(itemDone, "output_index", idx)
		itemDone, _ = sjson.SetRawBytes(itemDone, "item", st.outputItem(idx, status))
		out = append(out, emitEvent("response.output_item.done", itemDone))
	}
	return out
}
