package config

import "fmt"

// AdmissionQueue configures waiting before an execution is admitted.
type AdmissionQueue struct {
	Enabled            bool `yaml:"enabled" json:"enabled"`
	MaxWaiting         int  `yaml:"max-waiting" json:"max-waiting"`
	WaitTimeoutSeconds int  `yaml:"wait-timeout-seconds" json:"wait-timeout-seconds"`
}

// AdmissionConfig bounds retained work across every admission queue.
type AdmissionConfig struct {
	MaxQueuedRequests int   `yaml:"max-queued-requests" json:"max-queued-requests"`
	MaxQueuedBytes    int64 `yaml:"max-queued-bytes" json:"max-queued-bytes"`
}

func (c AdmissionConfig) Limits() (int, int64) {
	n, bytes := c.MaxQueuedRequests, c.MaxQueuedBytes
	if n == 0 {
		n = 1024
	}
	if bytes == 0 {
		bytes = 256 << 20
	}
	return n, bytes
}

func (c *Config) ValidateAdmission() error {
	if c == nil {
		return nil
	}
	if c.Admission.MaxQueuedRequests < 0 || c.Admission.MaxQueuedBytes < 0 {
		return fmt.Errorf("admission queue budgets cannot be negative")
	}
	validate := func(limit, rpm int, queue AdmissionQueue) error {
		if limit < 0 || rpm < 0 || queue.MaxWaiting < 0 || queue.WaitTimeoutSeconds < 0 || queue.WaitTimeoutSeconds > 86400 {
			return fmt.Errorf("admission limits must be nonnegative and wait timeout cannot exceed 86400 seconds")
		}
		return nil
	}
	if err := validate(c.Routing.PerAuthConcurrency, 0, c.Routing.Queue); err != nil {
		return err
	}
	for _, rule := range c.Routing.PriorityOverrides {
		if rule.PerAuthConcurrency != nil && *rule.PerAuthConcurrency < 0 {
			return fmt.Errorf("routing concurrency cannot be negative")
		}
		if rule.Queue != nil {
			if err := validate(0, 0, *rule.Queue); err != nil {
				return err
			}
		}
		for _, sub := range rule.SubscriptionOverrides {
			if sub.PerAuthConcurrency != nil && *sub.PerAuthConcurrency < 0 {
				return fmt.Errorf("credential concurrency cannot be negative")
			}
		}
	}
	for _, key := range c.APIKeyGroups {
		if err := validate(key.ConcurrencyLimit, key.RPMLimit, key.Queue); err != nil {
			return err
		}
	}
	return nil
}
