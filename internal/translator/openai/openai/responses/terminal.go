package responses

import "github.com/tidwall/gjson"

func responsesIncompleteFromChoices(choices gjson.Result) string {
	if !choices.IsArray() {
		return ""
	}
	reason, firstIndex := "", 0
	choices.ForEach(func(position, choice gjson.Result) bool {
		index, valid := responsesStreamIndex(choice.Get("index"), int(position.Int()))
		if !valid {
			index = int(position.Int())
		}
		if candidate := responsesIncompleteReason(choice.Get("finish_reason").String()); candidate != "" && (reason == "" || index < firstIndex) {
			reason, firstIndex = candidate, index
		}
		return true
	})
	return reason
}

func responsesIncompleteReason(finishReason string) string {
	switch finishReason {
	case "length", "max_tokens":
		return "max_output_tokens"
	case "content_filter":
		return "content_filter"
	default:
		return ""
	}
}

func responsesItemStatus(finishReason string) string {
	if responsesIncompleteReason(finishReason) != "" {
		return "incomplete"
	}
	return "completed"
}
