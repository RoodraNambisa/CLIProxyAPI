package proxyregion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	"golang.org/x/text/language"
)

func probeRegion(ctx context.Context, proxy, target string) ([]byte, int, string, error) {
	setting, errParse := proxyutil.Parse(proxy)
	if errParse != nil || setting.Mode != proxyutil.ModeProxy {
		return nil, 0, "", fmt.Errorf("region proxy required")
	}
	dialer, _, err := proxyutil.BuildContextDialer(proxy)
	if err != nil {
		return nil, 0, "", fmt.Errorf("region proxy unavailable")
	}
	transport := &http.Transport{DialContext: dialer.DialContext, DisableKeepAlives: true, MaxResponseHeaderBytes: maxBody}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, 0, "", fmt.Errorf("region URL invalid")
	}
	req.Header.Set("Accept", "application/json, text/plain")
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, "", fmt.Errorf("region network error")
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if len(body) > maxBody {
		return nil, resp.StatusCode, resp.Header.Get("Retry-After"), fmt.Errorf("region response too large")
	}
	return body, resp.StatusCode, resp.Header.Get("Retry-After"), err
}

func regionPublicIP(value string) string {
	ip, err := netip.ParseAddr(strings.TrimSpace(value))
	if err != nil {
		return ""
	}
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.Zone() != "" {
		return ""
	}
	return ip.String()
}
func regionResponse(body []byte, expected, source string, now time.Time) (Geo, string) {
	var response struct {
		Success  *bool  `json:"success"`
		IP       string `json:"ip"`
		Country  string `json:"country_code"`
		Region   string `json:"region_code"`
		Timezone struct {
			ID string `json:"id"`
		} `json:"timezone"`
	}
	if len(body) > maxBody || json.Unmarshal(body, &response) != nil || response.Success == nil || !*response.Success {
		return Geo{}, "lookup_invalid_response"
	}
	ip := regionPublicIP(response.IP)
	if ip == "" || (expected != "" && ip != expected) {
		return Geo{}, "lookup_ip_mismatch"
	}
	if len(response.Timezone.ID) > 128 || response.Timezone.ID == "" || response.Timezone.ID == "Local" {
		return Geo{}, "lookup_invalid_timezone"
	}
	if _, err := time.LoadLocation(response.Timezone.ID); err != nil {
		return Geo{}, "lookup_invalid_timezone"
	}
	if len(response.Country) != 2 || len(response.Region) > 128 {
		return Geo{}, "lookup_invalid_response"
	}
	return Geo{IP: ip, Country: strings.ToUpper(response.Country), Region: response.Region, Timezone: response.Timezone.ID, Source: source, At: now}, ""
}
func regionRetryAfter(raw string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil && seconds > 0 && seconds <= int64((1<<63-1)/time.Second) {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(raw); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}

func SuggestedLanguage(country string) string {
	region, err := language.ParseRegion(country)
	if err != nil || country == "" || !region.IsCountry() {
		return ""
	}
	tag, err := language.Parse("und-" + region.String())
	if err != nil {
		return ""
	}
	base, confidence := tag.Base()
	if confidence == language.No || base.String() == "und" {
		return ""
	}
	return language.Make(base.String() + "-" + region.String()).String()
}
