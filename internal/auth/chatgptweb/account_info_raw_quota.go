package chatgptweb

import "time"

const (
	AccountInfoRawQuotaResponseCapacity = 20
	AccountInfoRawQuotaResponseMaxBytes = 8 << 20
)

// AccountInfoRawQuotaResponseEvent is one in-memory conversation/init response.
type AccountInfoRawQuotaResponseEvent struct {
	CapturedAt  time.Time
	AuthIndex   string
	Attempt     int
	HTTPStatus  int
	ContentType string
	Body        []byte
	Truncated   bool
	ParseError  string
	ParsedQuota *ImageQuota
}

// AccountInfoRawQuotaParsed records the value selected by the current parser.
type AccountInfoRawQuotaParsed struct {
	FeaturePresent bool       `json:"feature_present"`
	Present        bool       `json:"present"`
	Remaining      int        `json:"remaining"`
	ResetAt        *time.Time `json:"reset_at,omitempty"`
}

// AccountInfoRawQuotaResponseRecord exposes one detached raw response to management clients.
type AccountInfoRawQuotaResponseRecord struct {
	AuthIndex     string                     `json:"auth_index"`
	CapturedAt    time.Time                  `json:"captured_at"`
	Attempt       int                        `json:"attempt,omitempty"`
	HTTPStatus    int                        `json:"http_status,omitempty"`
	ContentType   string                     `json:"content_type,omitempty"`
	ResponseBytes int                        `json:"response_bytes"`
	Truncated     bool                       `json:"truncated"`
	ParseError    string                     `json:"parse_error,omitempty"`
	ParsedQuota   *AccountInfoRawQuotaParsed `json:"parsed_quota,omitempty"`
	Body          string                     `json:"body"`
}

// AccountInfoRawQuotaResponsesSnapshot is the bounded in-memory capture state.
type AccountInfoRawQuotaResponsesSnapshot struct {
	Enabled      bool                                `json:"enabled"`
	Capacity     int                                 `json:"capacity"`
	MaxBytes     int                                 `json:"max_bytes"`
	TotalBytes   int                                 `json:"total_bytes"`
	EvictedCount uint64                              `json:"evicted_count"`
	Records      []AccountInfoRawQuotaResponseRecord `json:"records"`
}
