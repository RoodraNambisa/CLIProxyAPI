package helps

import (
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestOpenAIUsageCacheWriteAliasesAcrossEnvelopes(t *testing.T) {
	for _, path := range []string{
		"input_tokens_details.cache_write_tokens",
		"prompt_tokens_details.cached_creation_tokens",
		"input_tokens_details.cache_creation_tokens",
		"prompt_tokens_details.cache_creation_tokens",
		"prompt_tokens_details.cache_write_tokens",
	} {
		t.Run(path, func(t *testing.T) {
			for _, counts := range []string{`{}`, `{"input_tokens":10,"output_tokens":2,"total_tokens":12}`} {
				node, err := sjson.SetBytes([]byte(counts), path, 7)
				if err != nil {
					t.Fatal(err)
				}
				root := []byte(`{"usage":` + string(node) + `}`)
				httpDetail := ParseOpenAIUsage(root)
				streamDetail, streamOK := ParseOpenAIStreamUsage(append([]byte("data: "), root...))
				codexDetail, codexOK := ParseCodexUsage([]byte(`{"response":` + string(root) + `}`))
				if !streamOK || !codexOK || httpDetail.CacheCreationTokens != 7 || streamDetail != httpDetail || codexDetail != httpDetail || httpDetail.TotalTokens != gjson.Get(counts, "total_tokens").Int() {
					t.Fatal("cache-write alias was missed or changed the reported totals")
				}
			}
		})
	}
}

func TestCacheWriteAliasesPreserveExplicitZeroAndLocalPrecedence(t *testing.T) {
	node := []byte(`{"input_tokens_details":{"cache_write_tokens":0,"cache_creation_tokens":9},"prompt_tokens_details":{"cached_creation_tokens":1,"cache_creation_tokens":2,"cache_write_tokens":3}}`)
	for _, path := range []string{"input_tokens_details.cache_write_tokens", "prompt_tokens_details.cached_creation_tokens", "input_tokens_details.cache_creation_tokens", "prompt_tokens_details.cache_creation_tokens", "prompt_tokens_details.cache_write_tokens"} {
		want := gjson.GetBytes(node, path).Int()
		if got := openAIUsageCacheCreationNode(gjson.ParseBytes(node)).Int(); got != want {
			t.Fatal("an added alias overrode a higher-priority explicit field")
		}
		var err error
		node, err = sjson.DeleteBytes(node, path)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, field := range []string{"cache_creation_tokens", "cacheCreationTokens", "cache_write_tokens", "cacheWriteTokens"} {
		node := gjson.Parse(`{"input_tokens":10,"output_tokens":2,"total_tokens":12,"` + field + `":3}`)
		detail := parseInteractionsUsageDetail(node)
		if detail.CacheCreationTokens != 3 || detail.InputTokens != 10 || detail.OutputTokens != 2 || detail.TotalTokens != 12 {
			t.Fatal("Interactions cache alias changed inclusive token accounting")
		}
	}
	if detail := parseInteractionsUsageDetail(gjson.Parse(`{"cache_creation_tokens":0,"cache_write_tokens":3}`)); detail.CacheCreationTokens != 0 {
		t.Fatal("Interactions explicit zero was replaced")
	}
}
