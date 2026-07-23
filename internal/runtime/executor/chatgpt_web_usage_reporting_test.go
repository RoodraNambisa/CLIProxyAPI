package executor

import (
	"context"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

type chatGPTWebUsageReportingPlugin struct {
	authID  string
	records chan coreusage.Record
}

func (plugin *chatGPTWebUsageReportingPlugin) HandleUsage(_ context.Context, record coreusage.Record) {
	if record.AuthID == plugin.authID {
		plugin.records <- record
	}
}

func TestPublishChatGPTWebTerminalUsageIncludesImageToolModel(t *testing.T) {
	const authID = "chatgpt-web-image-usage-reporting"
	records := make(chan coreusage.Record, 2)
	coreusage.RegisterPlugin(&chatGPTWebUsageReportingPlugin{authID: authID, records: records})
	reporter := helps.NewUsageReporter(context.Background(), "chatgpt-web", "gpt-5.4", &cliproxyauth.Auth{ID: authID})
	prepared := &chatGPTWebPreparedRequest{request: helps.ChatGPTWebRequest{Image: &helps.ChatGPTWebImageRequest{Model: "gpt-image-2"}}}
	completed := []byte(`{"response":{"usage":{"input_tokens":11,"output_tokens":2,"total_tokens":13},"tool_usage":{"image_gen":{"input_tokens":3,"output_tokens":7024,"total_tokens":7027}}}}`)

	publishChatGPTWebTerminalUsage(context.Background(), reporter, prepared, completed)

	got := make(map[string]coreusage.Record, 2)
	deadline := time.After(time.Second)
	for len(got) < 2 {
		select {
		case record := <-records:
			got[record.Model] = record
		case <-deadline:
			t.Fatalf("timed out waiting for usage records: %#v", got)
		}
	}
	if outer := got["gpt-5.4"]; outer.Detail.TotalTokens != 13 {
		t.Fatalf("outer usage = %#v", outer)
	}
	if image := got["gpt-image-2"]; image.Detail.OutputTokens != 7024 || image.Detail.TotalTokens != 7027 {
		t.Fatalf("image tool usage = %#v", image)
	}
}
