package chatgptweb

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
)

const browserRegionTraceURL = "https://chatgpt.com/cdn-cgi/trace"
const browserRegionMaxBody = 64 << 10

// Metadata probes own their transport and budget; no settings reach business clients.
func probeBrowserRegion(ctx context.Context, proxy, target string) ([]byte, int, string, error) {
	if strings.TrimSpace(proxy) == "" {
		proxy = "direct"
	}
	dialer, _, err := proxyutil.BuildContextDialer(proxy)
	if err != nil {
		return nil, 0, "", fmt.Errorf("region proxy unavailable")
	}
	transport := &http.Transport{DialContext: dialer.DialContext, DisableKeepAlives: true, MaxResponseHeaderBytes: browserRegionMaxBody}
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
	body, err := io.ReadAll(io.LimitReader(resp.Body, browserRegionMaxBody+1))
	if len(body) > browserRegionMaxBody {
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
func regionResponse(body []byte, expected, source string, now time.Time) (browserRegionGeo, string) {
	var response struct {
		Success  *bool  `json:"success"`
		IP       string `json:"ip"`
		Country  string `json:"country_code"`
		Region   string `json:"region_code"`
		Timezone struct {
			ID string `json:"id"`
		} `json:"timezone"`
	}
	if len(body) > browserRegionMaxBody || json.Unmarshal(body, &response) != nil || response.Success == nil || !*response.Success {
		return browserRegionGeo{}, "lookup_invalid_response"
	}
	ip := regionPublicIP(response.IP)
	if ip == "" || (expected != "" && ip != expected) {
		return browserRegionGeo{}, "lookup_ip_mismatch"
	}
	if len(response.Timezone.ID) > 128 || response.Timezone.ID == "" || response.Timezone.ID == "Local" {
		return browserRegionGeo{}, "lookup_invalid_timezone"
	}
	if _, err := time.LoadLocation(response.Timezone.ID); err != nil {
		return browserRegionGeo{}, "lookup_invalid_timezone"
	}
	if len(response.Country) != 2 || len(response.Region) > 128 {
		return browserRegionGeo{}, "lookup_invalid_response"
	}
	return browserRegionGeo{IP: ip, Country: strings.ToUpper(response.Country), Region: response.Region, Timezone: response.Timezone.ID, Source: source, At: now}, ""
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

func (r *BrowserRegionResolver) lookup(ctx context.Context, job *browserRegionJob) (browserRegionGeo, string, string, time.Duration) {
	observed := ""
	withIP := strings.Contains(job.settings.LookupURL, "{ip}")
	if withIP {
		body, status, retry, err := r.probe(ctx, job.proxy, browserRegionTraceURL)
		if err != nil {
			return browserRegionGeo{}, "", "trace_network_error", 0
		}
		if status != http.StatusOK {
			return browserRegionGeo{}, "", "trace_http_error", regionRetryAfter(retry, r.now())
		}
		for _, line := range strings.Split(string(body), "\n") {
			if value, ok := strings.CutPrefix(strings.TrimSpace(line), "ip="); ok {
				observed = regionPublicIP(value)
				break
			}
		}
		if observed == "" {
			return browserRegionGeo{}, "", "trace_invalid_ip", 0
		}
		r.mu.Lock()
		cached, ok := r.ips.get(observed, true)
		now := r.now()
		r.mu.Unlock()
		if ok && cached.Geo.IP != "" && now.Sub(cached.Geo.At) < 24*time.Hour && cached.Reason == "" {
			geo := cached.Geo
			geo.Source = "official_trace"
			return geo, observed, "", 0
		}
		if ok && now.Before(cached.RetryAt) {
			return cached.Geo, observed, cached.Reason, cached.RetryAt.Sub(now)
		}
	}
	type result struct {
		geo    browserRegionGeo
		reason string
		retry  time.Duration
	}
	fetch := func() (any, error) {
		// A second route may have populated the IP cache while this job waited.
		if withIP {
			r.mu.Lock()
			e, ok := r.ips.get(observed, true)
			now := r.now()
			r.mu.Unlock()
			if ok && e.Geo.IP != "" && e.Reason == "" && now.Sub(e.Geo.At) < 24*time.Hour {
				return result{geo: e.Geo}, nil
			}
			if ok && now.Before(e.RetryAt) {
				return result{geo: e.Geo, reason: e.Reason, retry: e.RetryAt.Sub(now)}, nil
			}
		}
		target := strings.ReplaceAll(job.settings.LookupURL, "{ip}", observed)
		body, status, retry, err := r.probe(ctx, job.proxy, target)
		if !withIP && status == http.StatusOK && err == nil {
			var exit struct {
				Success bool   `json:"success"`
				IP      string `json:"ip"`
			}
			if json.Unmarshal(body, &exit) == nil && exit.Success {
				observed = regionPublicIP(exit.IP)
			}
		}
		x := result{}
		if err != nil {
			x.reason = "lookup_network_error"
		} else if status != http.StatusOK {
			x.reason = "lookup_http_error"
			if status == http.StatusTooManyRequests {
				x.reason = "lookup_rate_limited"
				x.retry = regionRetryAfter(retry, r.now())
			}
		} else {
			source := "lookup_exit"
			if withIP {
				source = "official_trace"
			}
			x.geo, x.reason = regionResponse(body, observed, source, r.now())
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.closed || job.generation != r.generation {
			return result{reason: "configuration_changed"}, nil
		}
		key := observed
		if x.geo.IP != "" {
			key = x.geo.IP
		}
		if key != "" {
			e, _ := r.ips.get(key, false)
			if x.reason == "" {
				e = browserRegionEntry{Geo: x.geo}
			} else {
				e.Failures++
				e.Reason = x.reason
				delay := max(regionRetryDelay(e.Failures), x.retry)
				e.RetryAt = r.now().Add(delay)
				if e.Geo.IP == key {
					x.geo = e.Geo
				}
			}
			r.ips.put(key, e, r.policy.settings.IPCacheSize)
		}
		return x, nil
	}
	var value any
	if withIP {
		value, _, _ = r.group.Do(fmt.Sprintf("%d:%s", job.generation, observed), fetch)
	} else {
		value, _ = fetch()
	}
	x := value.(result)
	if withIP && x.geo.IP != "" {
		x.geo.Source = "official_trace"
	}
	return x.geo, observed, x.reason, x.retry
}
