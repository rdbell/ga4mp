// ga4mp implements the Google Analytics 4 Meauserment Protocol
package ga4mp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	CollectEndpoint = "https://www.google-analytics.com/mp/collect"
	DebugEndpoint   = "https://www.google-analytics.com/debug/mp/collect"
)

type ClientOptions struct {
	// Required: Admin > Data Streams > choose your stream > Measurement Protocol > Create
	ApiSecret string
	// Required: Admin > Data Streams > choose your stream > Measurement ID
	MeasurementID string
	// Perform client side validation fo the request before sending it
	Validate bool
	// HTTP Client for sending requests
	// defaults to http.DefaultClient if unset
	HttpClient *http.Client
}

type Client struct {
	query    string
	validate bool
	http     *http.Client
}

func New(o ClientOptions) *Client {
	v := make(url.Values)
	v.Set("api_secret", o.ApiSecret)
	v.Set("measurement_id", o.MeasurementID)

	if o.HttpClient == nil {
		o.HttpClient = http.DefaultClient
	}

	return &Client{
		query:    v.Encode(),
		validate: o.Validate,
		http:     o.HttpClient,
	}
}

func (c *Client) Send(ctx context.Context, r *Request) error {
	req, err := c.prepareRequest(ctx, r, CollectEndpoint+"?"+c.query)
	if err != nil {
		return err
	}
	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("ga4mp: post: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("ga4mp: %v: %q", res.Status, string(b))
	}
	return nil
}

func (c *Client) Debug(ctx context.Context, r *Request) (ValidationResponse, error) {
	var msg ValidationResponse

	req, err := c.prepareRequest(ctx, r, DebugEndpoint+"?"+c.query)
	if err != nil {
		return msg, err
	}
	res, err := c.http.Do(req)
	if err != nil {
		return msg, fmt.Errorf("ga4mp: post: %w", err)
	}
	defer res.Body.Close()

	err = json.NewDecoder(res.Body).Decode(&msg)
	if err != nil {
		return msg, fmt.Errorf("ga4mp: parse validaion response: %w", err)
	}
	return msg, nil
}

type ValidationResponse struct {
	ValidationMessages []ValidationMessage `json:"validationMessages"`
}

type ValidationMessage struct {
	FieldPath      string `json:"fieldPath"`
	Description    string `json:"description"`
	ValidationCode string `json:"validationCode"`
}

func (c *Client) prepareRequest(ctx context.Context, r *Request, url string) (*http.Request, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("ga4mp: marshal request: %w", err)
	}
	if c.validate {
		err := r.validate()
		if err != nil {
			return nil, fmt.Errorf("ga4mp: validate request: %w", err)
		}
		if len(b) > 130000 {
			return nil, fmt.Errorf("ga4mp: payload exceeds 130kb: %d", len(b))
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("ga4mp: prepare request: %w", err)
	}
	req.Header.Set("content-type", "application/json")

	return req, nil
}

type Request struct {
	// Required: A unique ID per user/instance combination
	ClientID string `json:"client_id"`
	// A unique cross platform ID for the user
	UserID string `json:"user_id"`
	// Backdate the event
	TimestampMicros    int64             `json:"timestamp_micros"`
	UserProperties     map[string]string `json:"user_properties"`
	NonPersonalizedAds bool              `json:"non_personalized_ads"`
	Events             []Event           `json:"events"`
}

func (r Request) validate() error {
	if len(r.ClientID) == 0 {
		return fmt.Errorf("ClientID must be set")
	}
	if d := time.Since(time.UnixMicro(r.TimestampMicros)); d > 3*72*time.Hour {
		return fmt.Errorf("timestamp from longer than 3 days back: %v", d)
	}
	if len(r.UserProperties) > 25 {
		return fmt.Errorf("request exceeds 25 user_properties: %d", len(r.UserProperties))
	}
	for k, v := range r.UserProperties {
		if err := validName(k, 24, reservedUserProperties, reservedUserPropertyPrefix); err != nil {
			return fmt.Errorf("invalid user property name: %w", err)
		}
		if len(v) > 36 {
			return fmt.Errorf("user property longer than 36: %q", v)
		}
	}

	if len(r.Events) > 25 {
		return fmt.Errorf("request exceeds 25 events: %d", len(r.Events))
	}
	for _, e := range r.Events {
		err := e.validate()
		if err != nil {
			return err
		}
	}
	return nil
}

type Event struct {
	Name   string         `json:"name"`
	Params map[string]any `json:"params"`
}

func (e Event) validate() error {
	if err := validName(e.Name, 40, reservedEventName, nil); err != nil {
		return fmt.Errorf("invalid event name: %w", err)
	}
	if len(e.Params) > 25 {
		return fmt.Errorf("event exceeds 25 params: %d", len(e.Params))
	}
	for k, v := range e.Params {
		if err := validName(k, 40, reservedParamNames, reservedParamPrefix); err != nil {
			return fmt.Errorf("invalid parameter name: %w", err)
		}
		switch vv := v.(type) {
		case string:
			if len(vv) > 100 {
				return fmt.Errorf("parameter longer than 100: %q", vv)
			}
		}
	}
	return nil
}

// reserved names
// https://developers.google.com/analytics/devguides/collection/protocol/ga4/reference?client_type=gtag#reserved_names
var (
	reservedEventName = map[string]struct{}{
		"ad_activeview":           {},
		"ad_click":                {},
		"ad_exposure":             {},
		"ad_impression":           {},
		"ad_query":                {},
		"adunit_exposure":         {},
		"app_clear_data":          {},
		"app_install":             {},
		"app_update":              {},
		"app_remove":              {},
		"error":                   {},
		"first_open":              {},
		"first_visit":             {},
		"in_app_purchase":         {},
		"notification_dismiss":    {},
		"notification_foreground": {},
		"notification_open":       {},
		"notification_receive":    {},
		"os_update":               {},
		"screen_view":             {},
		"session_start":           {},
		"user_engagement":         {},
	}

	reservedParamNames = map[string]struct{}{
		"firebase_conversion": {},
	}

	reservedParamPrefix = map[string]struct{}{
		"google_":   {},
		"ga_":       {},
		"firebase_": {},
	}

	reservedUserProperties = map[string]struct{}{
		"first_open_time":          {},
		"first_visit_time":         {},
		"last_deep_link_referrer":  {},
		"user_id":                  {},
		"first_open_after_install": {},
	}

	reservedUserPropertyPrefix = map[string]struct{}{
		"google_":   {},
		"ga_":       {},
		"firebase_": {},
	}
)

func validName(s string, l int, reservedNames, reservedPrefixes map[string]struct{}) error {
	if len(s) > l {
		return fmt.Errorf("name longer than %v: %q", l, s)
	}
	if _, ok := reservedNames[s]; ok {
		return fmt.Errorf("name is reserved: %q", s)
	}
	for prefix := range reservedPrefixes {
		if strings.HasPrefix(s, prefix) {
			return fmt.Errorf("name has reserved prefix %q: %q", prefix, s)
		}
	}
	for i, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			continue
		} else if (r >= '0' && r <= 'Z') || r == '_' {
			if i == 0 {
				return fmt.Errorf("name must begin with alphabetic char: %q", s)
			}
			continue
		}
		return fmt.Errorf("illegal char index %d: %q", i, s)
	}
	return nil
}
