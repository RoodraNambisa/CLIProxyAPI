package helps

import (
	"bufio"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestSplitSSEDataEventsJoinsDataAcrossComments(t *testing.T) {
	input := "event: response.completed\r\ndata: {\"response\":{\"id\":\"a\",\r\n: keepalive\r\nid: transport-id\r\ndata: \"status\":\"completed\"},\"type\":\"response.completed\"}\r\n\r\n"
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Split(SplitSSEDataEvents)
	found := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data:") {
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if !gjson.Valid(data) {
				t.Fatalf("SSE data was split around a comment: %s", line)
			}
			found = gjson.Get(data, "type").Str == "response.completed"
		}
	}
	if scanner.Err() != nil || !found {
		t.Fatal("lost completed event")
	}
}

func TestSplitSSEDataEventsDoesNotJoinSeparateEvents(t *testing.T) {
	input := "data: {\"unfinished\":\n\ndata: {\"complete\":true}\n\n"
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Split(SplitSSEDataEvents)
	var lines []string
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data:") {
			lines = append(lines, scanner.Text())
		}
	}
	if scanner.Err() != nil || len(lines) != 2 || lines[0] != `data: {"unfinished":` || lines[1] != `data: {"complete":true}` {
		t.Fatalf("combined or lost separate events: %v, %v", lines, scanner.Err())
	}
}
