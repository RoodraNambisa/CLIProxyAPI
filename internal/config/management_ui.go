package config

// ManagementUIConfig controls shared presentation preferences without changing admission policies.
// Nil values keep the corresponding live traffic panel visible.
type ManagementUIConfig struct {
	APIKeyTraffic     *bool `yaml:"api-key-traffic,omitempty" json:"api-key-traffic,omitempty"`
	CredentialTraffic *bool `yaml:"credential-traffic,omitempty" json:"credential-traffic,omitempty"`
}
