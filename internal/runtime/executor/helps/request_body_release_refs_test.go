package helps

import (
	"bytes"
	"strings"
	"testing"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

func TestRequestBodyRefsRetainExpandedToolIdentityAfterRelease(t *testing.T) {
	for _, mode := range []string{"unconfigured", "log-only", "release"} {
		t.Run(mode, func(t *testing.T) {
			original := []byte(`{"model":"client-model","instructions":"discarded instructions","tools":[{"type":"function","function":{"name":"plain","parameters":{"description":"discarded schema"}}}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"custom","name":"patch","description":"discarded description","format":{"definition":"discarded grammar"}}]}]},{"role":"user","content":"discarded prompt"}]}`)
			translated := bytes.ReplaceAll(original, []byte("client-model"), []byte("upstream-model"))
			controller := core.NewRequestBodyReleaseControllerWithMode(int64(len(original)), []byte("<released>"), mode == "log-only")
			opts := core.Options{}
			if mode != "unconfigured" {
				opts.Metadata = map[string]any{core.BodyReleaseControllerMetadataKey: controller}
			}
			originalRef, translatedRef, unregister := RequestBodyRefs(t.Context(), opts, original, translated)
			defer unregister()
			defer originalRef.Release()
			defer translatedRef.Release()
			controller.Release()
			if mode != "release" {
				if !bytes.Equal(originalRef.Bytes(), original) || !bytes.Equal(translatedRef.Bytes(), translated) || !RequestBodyReplayable(t.Context(), opts) {
					t.Fatal("unconfigured or log-only release changed execution bodies")
				}
				return
			}
			for _, ref := range []*core.ReleasableBytes{originalRef, translatedRef} {
				body := ref.Bytes()
				if strings.Contains(string(body), "discarded") || gjson.GetBytes(body, "input.#").Int() != 1 {
					t.Fatal("release retained prompt/schema data or changed additional tool grouping")
				}
				if gjson.GetBytes(body, "tools.0.function.name").String() != "plain" || gjson.GetBytes(body, "input.0.tools.0.name").String() != "editor" || gjson.GetBytes(body, "input.0.tools.0.tools.0.name").String() != "patch" || gjson.GetBytes(body, "input.0.tools.0.tools.0.type").String() != "custom" {
					t.Fatal("release lost ordinary, namespace or custom tool identity")
				}
			}
			if gjson.GetBytes(originalRef.Bytes(), "model").String() != "client-model" || gjson.GetBytes(translatedRef.Bytes(), "model").String() != "upstream-model" || RequestBodyReplayable(t.Context(), opts) {
				t.Fatal("release confused source/translated roles or enabled replay")
			}
		})
	}
}
