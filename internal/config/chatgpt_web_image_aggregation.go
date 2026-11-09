package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// validateWebImageAggregationYAML prevents YAML float-to-int truncation,
// including values inherited through merge keys.
func validateWebImageAggregationYAML(data []byte) error {
	var root struct {
		Images struct {
			Web struct {
				Concurrency yaml.Node `yaml:"n-aggregation-concurrency"`
			} `yaml:"chatgpt-web"`
		} `yaml:"images"`
	}
	if err := yaml.Unmarshal(data, &root); err != nil {
		// Leave unrelated malformed YAML to the existing optional-load policy.
		return nil
	}
	node := root.Images.Web.Concurrency
	if node.Kind != 0 && (node.Kind != yaml.ScalarNode || node.Tag != "!!int") {
		return fmt.Errorf("images.chatgpt-web.n-aggregation-concurrency must be an integer between 1 and 10")
	}
	return nil
}
