package responses

import (
	"fmt"
	"sort"
	"strings"

	"github.com/tidwall/sjson"
)

type responsesReasoningBlock struct {
	ID          string
	OutputIndex int
	Text        strings.Builder
	Done        bool
}

func (st *oaiToResponsesState) appendReasoning(choice int, text string, nextSeq func() int) [][]byte {
	var out [][]byte
	block := st.ReasoningBlocks[choice]
	if block == nil || block.Done {
		id := fmt.Sprintf("rs_%s_%d", st.ResponseID, choice)
		if block != nil {
			id += fmt.Sprintf("_%d", st.NextOutputIx)
		}
		block = &responsesReasoningBlock{ID: id, OutputIndex: st.NextOutputIx}
		st.NextOutputIx++
		st.ReasoningBlocks[choice] = block
		item := []byte(`{"type":"response.output_item.added","item":{"type":"reasoning","status":"in_progress","summary":[]}}`)
		item, _ = sjson.SetBytes(item, "sequence_number", nextSeq())
		item, _ = sjson.SetBytes(item, "output_index", block.OutputIndex)
		item, _ = sjson.SetBytes(item, "item.id", block.ID)
		out = append(out, emitRespEvent("response.output_item.added", item))
		part := []byte(`{"type":"response.reasoning_summary_part.added","summary_index":0,"part":{"type":"summary_text","text":""}}`)
		part, _ = sjson.SetBytes(part, "sequence_number", nextSeq())
		part, _ = sjson.SetBytes(part, "output_index", block.OutputIndex)
		part, _ = sjson.SetBytes(part, "item_id", block.ID)
		out = append(out, emitRespEvent("response.reasoning_summary_part.added", part))
	}
	block.Text.WriteString(text)
	delta := []byte(`{"type":"response.reasoning_summary_text.delta","summary_index":0}`)
	delta, _ = sjson.SetBytes(delta, "sequence_number", nextSeq())
	delta, _ = sjson.SetBytes(delta, "output_index", block.OutputIndex)
	delta, _ = sjson.SetBytes(delta, "item_id", block.ID)
	delta, _ = sjson.SetBytes(delta, "delta", text)
	return append(out, emitRespEvent("response.reasoning_summary_text.delta", delta))
}

func (st *oaiToResponsesState) finishReasoning(choice int, status string, nextSeq func() int) [][]byte {
	block := st.ReasoningBlocks[choice]
	if block == nil || block.Done {
		return nil
	}
	block.Done = true
	text := block.Text.String()
	done := []byte(`{"type":"response.reasoning_summary_text.done","summary_index":0}`)
	done, _ = sjson.SetBytes(done, "sequence_number", nextSeq())
	done, _ = sjson.SetBytes(done, "output_index", block.OutputIndex)
	done, _ = sjson.SetBytes(done, "item_id", block.ID)
	done, _ = sjson.SetBytes(done, "text", text)
	part := []byte(`{"type":"response.reasoning_summary_part.done","summary_index":0,"part":{"type":"summary_text","text":""}}`)
	part, _ = sjson.SetBytes(part, "sequence_number", nextSeq())
	part, _ = sjson.SetBytes(part, "output_index", block.OutputIndex)
	part, _ = sjson.SetBytes(part, "item_id", block.ID)
	part, _ = sjson.SetBytes(part, "part.text", text)
	item := []byte(`{"type":"response.output_item.done","item":{"type":"reasoning","encrypted_content":"","summary":[{"type":"summary_text","text":""}]}}`)
	item, _ = sjson.SetBytes(item, "sequence_number", nextSeq())
	item, _ = sjson.SetBytes(item, "output_index", block.OutputIndex)
	item, _ = sjson.SetBytes(item, "item.id", block.ID)
	item, _ = sjson.SetBytes(item, "item.status", status)
	item, _ = sjson.SetBytes(item, "item.summary.0.text", text)
	st.Reasonings = append(st.Reasonings, oaiToResponsesStateReasoning{ReasoningID: block.ID, ReasoningData: text, OutputIndex: block.OutputIndex, Status: status})
	return [][]byte{emitRespEvent("response.reasoning_summary_text.done", done), emitRespEvent("response.reasoning_summary_part.done", part), emitRespEvent("response.output_item.done", item)}
}

func (st *oaiToResponsesState) finishMessage(choice int, status string, nextSeq func() int) [][]byte {
	if !st.MsgItemAdded[choice] || st.MsgItemDone[choice] {
		return nil
	}
	text := ""
	if buffer := st.MsgTextBuf[choice]; buffer != nil {
		text = buffer.String()
	}
	id := st.MsgIDs[choice]
	index := st.MsgOutputIx[choice]
	done := []byte(`{"type":"response.output_text.done","content_index":0,"logprobs":[]}`)
	done, _ = sjson.SetBytes(done, "sequence_number", nextSeq())
	done, _ = sjson.SetBytes(done, "output_index", index)
	done, _ = sjson.SetBytes(done, "item_id", id)
	done, _ = sjson.SetBytes(done, "text", text)
	part := []byte(`{"type":"response.content_part.done","content_index":0,"part":{"type":"output_text","annotations":[],"logprobs":[],"text":""}}`)
	part, _ = sjson.SetBytes(part, "sequence_number", nextSeq())
	part, _ = sjson.SetBytes(part, "output_index", index)
	part, _ = sjson.SetBytes(part, "item_id", id)
	part, _ = sjson.SetBytes(part, "part.text", text)
	item := []byte(`{"type":"response.output_item.done","item":{"type":"message","role":"assistant","content":[{"type":"output_text","annotations":[],"logprobs":[],"text":""}]}}`)
	item, _ = sjson.SetBytes(item, "sequence_number", nextSeq())
	item, _ = sjson.SetBytes(item, "output_index", index)
	item, _ = sjson.SetBytes(item, "item.id", id)
	item, _ = sjson.SetBytes(item, "item.status", status)
	item, _ = sjson.SetBytes(item, "item.content.0.text", text)
	st.MsgItemDone[choice] = true
	st.Messages = append(st.Messages, oaiToResponsesStateMessage{ID: id, Text: text, OutputIndex: index, Status: status})
	return [][]byte{emitRespEvent("response.output_text.done", done), emitRespEvent("response.content_part.done", part), emitRespEvent("response.output_item.done", item)}
}

func (st *oaiToResponsesState) finishChoice(choice int, nextSeq func() int) [][]byte {
	status := responsesItemStatus(st.FinishReasons[choice])
	out := st.finishMessage(choice, status, nextSeq)
	out = append(out, st.finishReasoning(choice, status, nextSeq)...)
	var keys []string
	for key, owner := range st.FuncChoices {
		if owner == choice {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return st.FuncOutputIx[keys[i]] < st.FuncOutputIx[keys[j]] })
	for _, key := range keys {
		out = append(out, st.emitToolEvents(key, true, nextSeq)...)
	}
	return out
}

func (st *oaiToResponsesState) knownChoices() []int {
	choices := make(map[int]struct{})
	for choice := range st.MsgItemAdded {
		choices[choice] = struct{}{}
	}
	for choice := range st.ReasoningBlocks {
		choices[choice] = struct{}{}
	}
	for _, choice := range st.FuncChoices {
		choices[choice] = struct{}{}
	}
	for choice := range st.FinishReasons {
		choices[choice] = struct{}{}
	}
	result := make([]int, 0, len(choices))
	for choice := range choices {
		result = append(result, choice)
	}
	sort.Ints(result)
	return result
}

func (st *oaiToResponsesState) incompleteReason() string {
	for _, choice := range st.knownChoices() {
		if reason := responsesIncompleteReason(st.FinishReasons[choice]); reason != "" {
			return reason
		}
	}
	return ""
}
