package helps

import (
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func rewriteCodexCollaborationChoice(payload []byte, path, from, to string) ([]byte, error) {
	updated := payload
	pending := []codexProtocolNode{{value: gjson.GetBytes(payload, path), path: path}}
	set := func(path, value string) error {
		var errSet error
		updated, errSet = sjson.SetBytes(updated, path, value)
		return errSet
	}
	for len(pending) > 0 {
		index := len(pending) - 1
		node := pending[index]
		pending[index] = codexProtocolNode{}
		pending = pending[:index]
		if !node.value.IsObject() {
			continue
		}
		name := node.value.Get("name").String()
		switch node.value.Get("type").String() {
		case "allowed_tools":
			tools := node.value.Get("tools")
			if tools.IsArray() {
				for index, tool := range tools.Array() {
					pending = append(pending, codexProtocolNode{value: tool, path: fmt.Sprintf("%s.tools.%d", node.path, index)})
				}
			}
		case "namespace":
			if name == from {
				if err := set(node.path+".name", to); err != nil {
					return payload, err
				}
			}
		case "function", "custom":
			namespace := node.value.Get("namespace").String()
			if namespace != "" && namespace != from {
				continue
			}
			if namespace == from {
				if err := set(node.path+".namespace", to); err != nil {
					return payload, err
				}
			}
			if strings.HasPrefix(name, from+"__") {
				if err := set(node.path+".name", to+"__"+strings.TrimPrefix(name, from+"__")); err != nil {
					return payload, err
				}
			}
		}
	}
	return updated, nil
}
