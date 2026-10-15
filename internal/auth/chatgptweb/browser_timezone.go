package chatgptweb

import (
	"fmt"
	"strings"
	"time"
)

// BrowserTimezone is a resolved, request-scoped browser clock presentation.
// OffsetMinutes has JavaScript's sign: Tokyo is -540, not +540.
type BrowserTimezone struct {
	Name          string
	OffsetMinutes int
}

// SetBrowserTimezone pins the clock before request-owned work starts.
func (client *Client) SetBrowserTimezone(zone BrowserTimezone) error {
	if client == nil {
		return fmt.Errorf("browser client is nil")
	}
	resolved, err := zone.normalized()
	if err != nil {
		return err
	}
	if client.browserTimezone.CompareAndSwap(nil, &resolved) {
		return nil
	}
	if previous := client.browserTimezone.Load(); previous != nil && *previous == resolved {
		return nil
	}
	return fmt.Errorf("browser timezone is already pinned")
}

func (client *Client) BrowserTimezone() (BrowserTimezone, bool) {
	if client != nil {
		if zone := client.browserTimezone.Load(); zone != nil {
			return *zone, true
		}
	}
	return BrowserTimezone{}, false
}

func (zone BrowserTimezone) normalized() (BrowserTimezone, error) {
	zone.Name = strings.TrimSpace(zone.Name)
	if zone.Name == "" {
		zone.Name = "UTC"
	}
	if zone.OffsetMinutes < -14*60 || zone.OffsetMinutes > 14*60 {
		return BrowserTimezone{}, fmt.Errorf("browser timezone offset is invalid")
	}
	if _, err := time.LoadLocation(zone.Name); err != nil {
		return BrowserTimezone{}, fmt.Errorf("browser timezone name is invalid")
	}
	return zone, nil
}

func (zone BrowserTimezone) displayName() string {
	switch zone.Name {
	case "UTC", "Etc/UTC":
		return "Coordinated Universal Time"
	case "Asia/Tokyo":
		return "Japan Standard Time"
	case "Asia/Shanghai", "Asia/Hong_Kong":
		return "China Standard Time"
	case "America/New_York", "America/Toronto":
		if zone.OffsetMinutes == 240 {
			return "Eastern Daylight Time"
		}
		if zone.OffsetMinutes == 300 {
			return "Eastern Standard Time"
		}
	case "America/Los_Angeles":
		if zone.OffsetMinutes == 420 {
			return "Pacific Daylight Time"
		}
		if zone.OffsetMinutes == 480 {
			return "Pacific Standard Time"
		}
	}
	return zone.Name
}

func (zone BrowserTimezone) dateString(at time.Time) string {
	local := at.In(time.FixedZone(zone.Name, -zone.OffsetMinutes*60))
	return local.Format("Mon Jan 02 2006 15:04:05 GMT-0700") + " (" + zone.displayName() + ")"
}

func (environment ConversationTurnstileEnvironment) browserTimezone() (BrowserTimezone, error) {
	return (BrowserTimezone{Name: environment.Timezone, OffsetMinutes: environment.TimezoneOffsetMinutes}).normalized()
}
