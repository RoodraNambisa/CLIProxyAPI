package responses

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
