package executor

import (
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestRemoveCodexImageGenerationToolPrunesAllowedChoices(t *testing.T) {
	for _, additional := range []bool{false, true} {
		for _, choicePath := range []string{"tools", "allowed_tools.tools"} {
			for _, mixed := range []bool{false, true} {
				body := []byte(`{"tools":[{"type":"image_generation"},{"type":"function","name":"lookup"}],"tool_choice":{"type":"allowed_tools","mode":"required","allowed_tools":{"mode":"required"}},"input":[{"role":"user","content":"unchanged"}]}`)
				if additional {
					body, _ = sjson.SetRawBytes(body, "input.1", []byte(`{"type":"additional_tools","tools":[{"type":"image_generation"},{"type":"function","name":"lookup"}]}`))
					body, _ = sjson.DeleteBytes(body, "tools")
				}
				choices := `[{"type":"image_generation"},{"type":"function","function":{"name":"image_gen.imagegen"}},{"type":"namespace","name":"image_gen"}]`
				if mixed {
					choices = choices[:len(choices)-1] + `,{"type":"function","name":"lookup"}]`
				}
				body, _ = sjson.SetRawBytes(body, "tool_choice."+choicePath, []byte(choices))
				got := removeCodexImageGenerationTool(body)
				if cliproxyauth.PayloadHasImageGenerationTool(got) {
					t.Fatal("image declaration remains")
				}
				choice := gjson.GetBytes(got, "tool_choice")
				if mixed {
					retained := choice.Get(choicePath).Array()
					if len(retained) != 1 || retained[0].Get("name").String() != "lookup" {
						t.Errorf("additional=%t path=%s: removed image selections or retained non-image choice are incorrect", additional, choicePath)
					}
					if choice.Get("mode").String() != "required" || choice.Get("allowed_tools.mode").String() != "required" {
						t.Fatal("choice mode changed")
					}
				} else if choice.Exists() {
					t.Errorf("additional=%t path=%s: empty allowed choice remains", additional, choicePath)
				}
				if gjson.GetBytes(got, "input.0.content").String() != "unchanged" {
					t.Fatal("business content changed")
				}
			}
		}
	}
}
