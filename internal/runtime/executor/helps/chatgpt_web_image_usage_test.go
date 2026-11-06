package helps

import (
	"fmt"
	"testing"
)

func TestChatGPTWebImageUsageOfficialProfiles(t *testing.T) {
	for _, tc := range []struct {
		model, quality string
		width, height  int
		want           int64
	}{
		{"gpt-image-2", "low", 1024, 1024, 196},
		{"gpt-image-2", "medium", 1024, 1024, 1756},
		{"gpt-image-2", "high", 1024, 1024, 7024},
		{"gpt-image-2", "low", 1024, 800, 136},
		{"gpt-image-2", "low", 800, 1024, 136},
		{"gpt-image-2.5-sunburst", "low", 1024, 1024, 196},
		{"gpt-image-2.5-sunburst", "medium", 1024, 1024, 439},
		{"gpt-image-2.5-sunburst", "high", 1024, 1024, 1756},
		{"gpt-image-2.5-sunburst", "low", 1024, 800, 136},
		{"gpt-image-2.6-sunburst", "medium", 1024, 1024, 1449},
	} {
		t.Run(fmt.Sprintf("%s/%s/%dx%d", tc.model, tc.quality, tc.width, tc.height), func(t *testing.T) {
			got, err := ChatGPTWebImageOutputTokenCount(tc.model, tc.quality, "medium", tc.width, tc.height)
			if err != nil || got != tc.want {
				t.Fatalf("tokens = %d, error = %v; want %d", got, err, tc.want)
			}
		})
	}
}

func TestChatGPTWeb26OutputUsage(t *testing.T) {
	for _, tc := range []struct {
		quality       string
		width, height int
		want          int64
	}{
		{"low", 1024, 1024, 824}, {"medium", 1024, 1024, 1449}, {"high", 1024, 1024, 1756},
		{"medium", 1536, 1024, 1132}, {"low", 1024, 800, 572},
		{"auto", 1024, 1024, 1449},
	} {
		for _, model := range []string{"gpt-image-2.6", "gpt-image-2.6-sunburst", "gpt-image-2.6-flare"} {
			got, err := ChatGPTWebImageOutputTokenCount(model, tc.quality, "medium", tc.width, tc.height)
			if err != nil || got != tc.want {
				t.Fatalf("%s %s %dx%d: got %d err %v want %d", model, tc.quality, tc.width, tc.height, got, err, tc.want)
			}
		}
	}
	if got, err := ChatGPTWebImageOutputTokenCount("gpt-image-2", "high", "medium", 1024, 1024); err != nil || got != 7024 {
		t.Fatalf("legacy profile changed: %d %v", got, err)
	}
}

func TestChatGPTWebGIFSheetUsage(t *testing.T) {
	for _, tc := range []struct {
		frames, edge int
		quality      string
		want         int64
	}{
		{2, 512, "low", 555}, {4, 512, "low", 555}, {8, 512, "low", 555},
		{9, 512, "low", 1172}, {10, 512, "low", 1038}, {16, 512, "low", 1668}, {16, 256, "low", 824},
		{9, 512, "medium", 2073}, {10, 512, "high", 2223},
	} {
		image := ChatGPTWebUsageImage{Model: ChatGPTWebImageUsageModel, Width: tc.edge, Height: tc.edge, Quality: tc.quality, Animation: ChatGPTWebImageAnimation{FrameCount: tc.frames}}
		got, err := ChatGPTWebImageUsageOutputTokenCount(image, "medium")
		if err != nil || got != tc.want {
			t.Fatalf("%+v: got %d err %v", tc, got, err)
		}
	}
}

func TestChatGPTWebGIFSheetMetadataAndBounds(t *testing.T) {
	layout := ChatGPTWebImageAnimation{Rows: 2, Columns: 4, FrameCount: 4}
	if got := layout.ForFrames(4, 512, 1024); got != layout {
		t.Fatal("verified layout was replaced by inference")
	}
	if got := layout.ForFrames(9, 512, 512); got.Rows != 3 || got.Columns != 3 || got.FrameCount != 9 {
		t.Fatalf("mismatched metadata retained: %+v", got)
	}
	for frames := 2; frames <= ChatGPTWebMaxGIFFrames; frames++ {
		got := (ChatGPTWebImageAnimation{}).ForFrames(frames, 512, 1024)
		if !got.validLayout() || got.Rows*got.Columns < frames || got.FrameCount != frames {
			t.Fatalf("unbounded fallback: %+v", got)
		}
	}
	for _, image := range []ChatGPTWebUsageImage{
		{Model: ChatGPTWebImageUsageModel, Width: 0, Height: 512},
		{Model: ChatGPTWebImageUsageModel, Width: int(^uint(0) >> 1), Height: 2},
		{Model: ChatGPTWebImageUsageModel, Width: 512, Height: 512, Animation: ChatGPTWebImageAnimation{FrameCount: 65}},
		{Model: ChatGPTWebImageUsageModel, Width: int(^uint(0) >> 1), Height: 1, Animation: ChatGPTWebImageAnimation{FrameCount: 9}},
	} {
		if _, err := ChatGPTWebImageUsageOutputTokenCount(image, "medium"); err == nil {
			t.Fatalf("accepted invalid dimensions: %+v", image)
		}
	}
}

func TestChatGPTWebGIFUsageProjectionUsesDeliveredSheetOnce(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	request := chatGPTWebUsageTestRequest("draw a ball")
	request.Image = &ChatGPTWebImageRequest{Model: "custom-web-alias", Prompt: "draw a ball"}
	projection, err := cache.NewProjection("gpt-5.4", request, ChatGPTWebUsageCacheOptions{AutoOutputQuality: "medium"})
	if err != nil {
		t.Fatal(err)
	}
	image := ChatGPTWebUsageImage{Model: ChatGPTWebImageUsageModel, Width: 512, Height: 512, Quality: "auto", Animation: ChatGPTWebImageAnimation{Rows: 3, Columns: 3, FrameCount: 9}}
	one, errs := projection.Estimate("", []ChatGPTWebUsageImage{image})
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	two, errs := projection.Estimate("", []ChatGPTWebUsageImage{image, image})
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	a := one["tool_usage"].(map[string]any)["image_gen"].(map[string]any)
	b := two["tool_usage"].(map[string]any)["image_gen"].(map[string]any)
	if a["output_tokens"] != int64(2073) || b["output_tokens"] != int64(4146) || a["input_tokens"] != b["input_tokens"] {
		t.Fatalf("wrong aggregate: %v %v", a, b)
	}
	projection.Complete()
}

func TestChatGPTWebGIFUsageMetadataPatchAndMerge(t *testing.T) {
	acc := &ChatGPTWebImageAccumulator{}
	apply := func(body string) {
		t.Helper()
		if _, err := acc.Apply([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	apply(`{"message":{"id":"first","author":{"role":"tool","name":"image_gen"},"content":{"parts":[{"content_type":"image_asset_pointer","asset_pointer":"sediment://first"},{"content_type":"image_asset_pointer","asset_pointer":"file-service://second","metadata":{"generation":{"animation":{"rows":4,"columns":4,"frame_count":16}}}}]}}}`)
	for key, value := range map[string]int{"rows": 3, "columns": 3, "frame_count": 9} {
		apply(fmt.Sprintf(`{"p":"/message/content/parts/0/metadata/generation/animation/%s","o":"add","v":%d}`, key, value))
	}
	refs := acc.OutputReferences(0)
	if len(refs) != 2 || refs[0].Animation.Rows != 3 || refs[0].Animation.Columns != 3 || refs[0].Animation.FrameCount != 9 || refs[1].Animation.FrameCount != 16 {
		t.Fatalf("wrong metadata: %+v", refs)
	}
	merged, err := MergeChatGPTWebImageAccumulators(&ChatGPTWebImageAccumulator{References: []ChatGPTWebImageReference{{Kind: "sediment", ID: "first"}}}, acc)
	if err != nil || merged.References[0].Animation.Rows != 3 {
		t.Fatalf("merge lost metadata: %+v %v", merged, err)
	}
	apply(`{"p":"/message/id","o":"replace","v":"next"}`)
	apply(`{"p":"/message/content/parts/0/metadata/generation/animation","o":"add","v":{"rows":2,"columns":4,"frame_count":8}}`)
	if acc.References[0].Animation.Rows != 3 {
		t.Fatal("new message corrupted previous image")
	}
	apply(`{"p":"/message/content/parts/0/asset_pointer","o":"replace","v":"sediment://third"}`)
	if acc.References[2].Animation.validLayout() {
		t.Fatal("unbound patch assigned previous layout to a new pointer")
	}
}

func TestChatGPTWebGIFUsageFullMetadataAndReferenceSelection(t *testing.T) {
	acc := &ChatGPTWebImageAccumulator{}
	_, err := acc.Apply([]byte(`{"message":{"author":{"role":"tool","name":"image_gen"},"content":{"parts":[{"content_type":"image_asset_pointer","asset_pointer":"file-service://file_upload"},{"content_type":"image_asset_pointer","asset_pointer":"sediment://gif","metadata":{"generation":{"animation":{"rows":3,"columns":3,"frame_count":9},"spritesheet":{"asset_pointer":"sediment://sheet"}}}}]}}}`))
	if err != nil {
		t.Fatal(err)
	}
	refs := acc.OutputReferences(1)
	if len(refs) != 1 || refs[0].ID != "gif" || refs[0].Animation.FrameCount != 9 {
		t.Fatalf("layout did not follow selected output: %+v", refs)
	}
}

func TestChatGPTWebGIFUsageMetadataRejectsIncompleteAndUnboundedParts(t *testing.T) {
	part := map[string]any{
		"asset_pointer": "file-service://image", "height": float64(512),
		"metadata": map[string]any{"generation": map[string]any{
			"height": float64(128), "animation": map[string]any{"rows": 3, "columns": 3, "frame_count": 9},
		}},
	}
	if got := chatGPTWebImageUsagePartFromValue(part); got.id != "" {
		t.Fatal("incomplete pointer supplied accounting layout")
	}
	acc := &ChatGPTWebImageAccumulator{role: "tool", imageTool: true}
	for index := 0; index < 100; index++ {
		acc.captureImageUsagePatch(map[string]any{"p": fmt.Sprintf("/message/content/parts/%d/metadata/generation/animation", index), "v": map[string]any{"rows": 3, "columns": 3, "frame_count": 9}})
	}
	if len(acc.usageParts) > chatGPTWebMaxImageOutputReferences {
		t.Fatal("usage part cache is unbounded")
	}
}
