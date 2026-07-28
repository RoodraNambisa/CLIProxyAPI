package chatgptweb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	AccountCheckPath      = "/backend-api/accounts/check/v4-2023-04-27"
	ConversationInitPath  = "/backend-api/conversation/init"
	AccountInfoMaxTargets = 500
)

// ErrAccountProfileIdentityMismatch means the requested account is absent from accounts/check.
var ErrAccountProfileIdentityMismatch = errors.New("account profile identity mismatch")

// AccountProfile is the non-sensitive account identity returned by accounts/check.
type AccountProfile struct {
	AccountID string
	PlanType  string
}

// ImageQuota is the image_gen capability state returned by conversation/init.
type ImageQuota struct {
	FeaturePresent bool
	Present        bool
	Remaining      int
	ResetAt        time.Time
}

type accountProfileFields struct {
	AccountID string `json:"account_id"`
	ID        string `json:"id"`
	PlanType  string `json:"plan_type"`
}

type accountProfileEntitlement struct {
	PlanType         string `json:"plan_type"`
	SubscriptionPlan string `json:"subscription_plan"`
}

type accountProfileEntry struct {
	Account                *accountProfileFields      `json:"account"`
	AccountID              string                     `json:"account_id"`
	ID                     string                     `json:"id"`
	PlanType               string                     `json:"plan_type"`
	Entitlement            *accountProfileEntitlement `json:"entitlement"`
	LastActiveSubscription *accountProfileEntitlement `json:"last_active_subscription"`
}

type accountProfileCandidates struct {
	byID             map[string]AccountProfile
	defaultAccountID string
}

// ParseAccountProfile extracts an account using the response selection metadata.
func ParseAccountProfile(payload []byte) (AccountProfile, error) {
	return ParseAccountProfileForAccount(payload, "")
}

// ParseAccountProfileForAccount extracts an account, preferring an exact account ID match.
func ParseAccountProfileForAccount(payload []byte, preferredAccountID string) (AccountProfile, error) {
	var document struct {
		Accounts         json.RawMessage `json:"accounts"`
		DefaultAccountID string          `json:"default_account_id"`
		AccountOrdering  []string        `json:"account_ordering"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := decoder.Decode(&document); err != nil {
		return AccountProfile{}, fmt.Errorf("decode account profile: %w", err)
	}
	if err := requireJSONDocumentEnd(decoder); err != nil {
		return AccountProfile{}, fmt.Errorf("decode account profile: %w", err)
	}
	if err := validateAccountIDSelector("preferred", preferredAccountID); err != nil {
		return AccountProfile{}, err
	}
	if err := validateAccountIDSelector("default_account_id", document.DefaultAccountID); err != nil {
		return AccountProfile{}, err
	}
	for _, accountID := range document.AccountOrdering {
		if err := validateAccountIDSelector("account_ordering", accountID); err != nil {
			return AccountProfile{}, err
		}
	}

	candidates := accountProfileCandidates{
		byID: make(map[string]AccountProfile),
	}
	if err := candidates.decode(document.Accounts); err != nil {
		return AccountProfile{}, err
	}
	return candidates.selectProfile(preferredAccountID, document.DefaultAccountID, document.AccountOrdering)
}

func (candidates *accountProfileCandidates) decode(raw json.RawMessage) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return fmt.Errorf("account profile is missing accounts")
	}
	switch trimmed[0] {
	case '{':
		var entries map[string]json.RawMessage
		if err := json.Unmarshal(trimmed, &entries); err != nil {
			return fmt.Errorf("decode account profile accounts: %w", err)
		}
		keys := make([]string, 0, len(entries))
		for key := range entries {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i] == "default" {
				return keys[j] != "default"
			}
			if keys[j] == "default" {
				return false
			}
			return keys[i] < keys[j]
		})
		for _, key := range keys {
			accountID, valid, err := candidates.add(entries[key], key)
			if err != nil {
				return err
			}
			if key == "default" && valid {
				candidates.defaultAccountID = accountID
			}
		}
	case '[':
		var entries []json.RawMessage
		if err := json.Unmarshal(trimmed, &entries); err != nil {
			return fmt.Errorf("decode account profile accounts: %w", err)
		}
		for _, entry := range entries {
			if _, _, err := candidates.add(entry, ""); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("account profile accounts must be an object or array")
	}
	return nil
}

func (candidates *accountProfileCandidates) add(raw json.RawMessage, objectKey string) (string, bool, error) {
	var entry accountProfileEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		return "", false, fmt.Errorf("decode account profile account: %w", err)
	}
	fields := accountProfileFields{
		AccountID: firstNonEmptyAccountID(entry.AccountID, entry.ID),
		PlanType:  entry.PlanType,
	}
	if entry.Account != nil {
		if accountID := firstNonEmptyAccountID(entry.Account.AccountID, entry.Account.ID); accountID != "" {
			fields.AccountID = accountID
		}
		if entry.Account.PlanType != "" {
			fields.PlanType = entry.Account.PlanType
		}
	}
	if strings.TrimSpace(fields.PlanType) == "" && entry.Entitlement != nil {
		fields.PlanType = firstNonEmptyString(
			entry.Entitlement.PlanType,
			entry.Entitlement.SubscriptionPlan,
		)
	}
	if strings.TrimSpace(fields.PlanType) == "" && entry.LastActiveSubscription != nil {
		fields.PlanType = firstNonEmptyString(
			entry.LastActiveSubscription.PlanType,
			entry.LastActiveSubscription.SubscriptionPlan,
		)
	}
	if fields.AccountID == "" && objectKey != "" && objectKey != "default" {
		fields.AccountID = objectKey
	}
	if fields.AccountID == "" {
		return "", false, nil
	}
	if strings.TrimSpace(fields.AccountID) != fields.AccountID {
		return "", false, fmt.Errorf("account profile has invalid account_id")
	}
	profile := AccountProfile{
		AccountID: fields.AccountID,
		PlanType:  normalizeAccountPlanType(fields.PlanType),
	}
	existing, ok := candidates.byID[profile.AccountID]
	if !ok || existing.PlanType == "" {
		candidates.byID[profile.AccountID] = profile
	}
	return profile.AccountID, true, nil
}

func firstNonEmptyAccountID(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func normalizeAccountPlanType(value string) string {
	planType := strings.TrimSpace(value)
	compact := strings.NewReplacer("-", "", "_", "", " ", "").Replace(strings.ToLower(planType))
	switch compact {
	case "chatgptfreeplan":
		return "free"
	case "chatgptplusplan":
		return "plus"
	case "chatgptproplan":
		return "pro"
	case "chatgptteamplan", "chatgptbusinessplan":
		return "team"
	case "chatgptenterpriseplan":
		return "enterprise"
	default:
		return planType
	}
}

func (candidates *accountProfileCandidates) selectProfile(preferredAccountID, defaultAccountID string, accountOrdering []string) (AccountProfile, error) {
	if preferredAccountID != "" {
		if profile, ok := candidates.byID[preferredAccountID]; ok {
			return profile, nil
		}
		return AccountProfile{}, fmt.Errorf("%w: preferred account is unavailable", ErrAccountProfileIdentityMismatch)
	}
	if profile, ok := candidates.byID[defaultAccountID]; defaultAccountID != "" && ok {
		return profile, nil
	}
	for _, accountID := range accountOrdering {
		if profile, ok := candidates.byID[accountID]; accountID != "" && ok {
			return profile, nil
		}
	}
	if profile, ok := candidates.byID[candidates.defaultAccountID]; candidates.defaultAccountID != "" && ok {
		return profile, nil
	}
	if len(candidates.byID) == 1 {
		for _, profile := range candidates.byID {
			return profile, nil
		}
	}
	if len(candidates.byID) == 0 {
		return AccountProfile{}, fmt.Errorf("account profile has no valid account")
	}
	return AccountProfile{}, fmt.Errorf("account profile has multiple accounts and no matching selector")
}

func validateAccountIDSelector(name, accountID string) error {
	if accountID != "" && strings.TrimSpace(accountID) != accountID {
		return fmt.Errorf("account profile has invalid %s", name)
	}
	return nil
}

// ParseImageQuota extracts image_gen while preserving a missing feature as unknown.
func ParseImageQuota(payload []byte) (ImageQuota, error) {
	var document struct {
		LimitsProgress []map[string]json.RawMessage `json:"limits_progress"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil {
		return ImageQuota{}, fmt.Errorf("decode conversation limits: %w", err)
	}
	if err := requireJSONDocumentEnd(decoder); err != nil {
		return ImageQuota{}, fmt.Errorf("decode conversation limits: %w", err)
	}
	for _, item := range document.LimitsProgress {
		var featureName string
		if raw := item["feature_name"]; len(raw) > 0 {
			if err := json.Unmarshal(raw, &featureName); err != nil {
				continue
			}
		}
		if !strings.EqualFold(strings.TrimSpace(featureName), "image_gen") {
			continue
		}
		if bytes.Equal(bytes.TrimSpace(item["remaining"]), []byte("null")) {
			return ImageQuota{FeaturePresent: true}, nil
		}
		remaining, errRemaining := parseQuotaRemaining(item["remaining"])
		if errRemaining != nil {
			return ImageQuota{}, errRemaining
		}
		resetAt, errReset := parseQuotaResetAt(item["reset_after"])
		if errReset != nil {
			return ImageQuota{}, errReset
		}
		return ImageQuota{
			FeaturePresent: true,
			Present:        true,
			Remaining:      remaining,
			ResetAt:        resetAt,
		}, nil
	}
	return ImageQuota{}, nil
}

func requireJSONDocumentEnd(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return fmt.Errorf("invalid trailing data: %w", err)
	}
	return fmt.Errorf("invalid trailing JSON value")
}

func parseQuotaRemaining(raw json.RawMessage) (int, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, fmt.Errorf("image quota remaining is missing")
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err == nil {
		value, errValue := strconv.ParseInt(number.String(), 10, 64)
		if errValue != nil || value < 0 || value > math.MaxInt {
			return 0, fmt.Errorf("image quota remaining is invalid")
		}
		return int(value), nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return 0, fmt.Errorf("image quota remaining is invalid")
	}
	value, errValue := strconv.ParseInt(strings.TrimSpace(text), 10, 64)
	if errValue != nil || value < 0 || value > math.MaxInt {
		return 0, fmt.Errorf("image quota remaining is invalid")
	}
	return int(value), nil
}

func parseQuotaResetAt(raw json.RawMessage) (time.Time, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return time.Time{}, nil
	}
	var text string
	if err := json.Unmarshal(trimmed, &text); err == nil {
		text = strings.TrimSpace(text)
		if text == "" {
			return time.Time{}, nil
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
			if parsed, errParse := time.Parse(layout, text); errParse == nil {
				return parsed.UTC(), nil
			}
		}
		if numeric, errNumber := strconv.ParseFloat(text, 64); errNumber == nil {
			return unixQuotaResetTime(numeric)
		}
		return time.Time{}, fmt.Errorf("image quota reset_after is invalid")
	}
	var number json.Number
	if err := json.Unmarshal(trimmed, &number); err != nil {
		return time.Time{}, fmt.Errorf("image quota reset_after is invalid")
	}
	value, errValue := strconv.ParseFloat(number.String(), 64)
	if errValue != nil {
		return time.Time{}, fmt.Errorf("image quota reset_after is invalid")
	}
	return unixQuotaResetTime(value)
}

func unixQuotaResetTime(value float64) (time.Time, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 {
		return time.Time{}, fmt.Errorf("image quota reset_after is invalid")
	}
	if value >= 1e12 {
		value /= 1000
	}
	seconds, fraction := math.Modf(value)
	if seconds > float64(math.MaxInt64) {
		return time.Time{}, fmt.Errorf("image quota reset_after is invalid")
	}
	resetAt := time.Unix(int64(seconds), int64(fraction*float64(time.Second))).UTC()
	if resetAt.Year() > 9999 {
		return time.Time{}, fmt.Errorf("image quota reset_after is invalid")
	}
	return resetAt, nil
}
