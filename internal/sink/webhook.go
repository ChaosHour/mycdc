package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ChaosHour/mycdc/internal/event"
)

// WebhookConfig configures a WebhookSink.
type WebhookConfig struct {
	URL          string
	Headers      map[string]string
	Timeout      time.Duration
	MaxRetries   int
	RetryBackoff time.Duration

	// Client, if set, is used instead of a default *http.Client. Tests use
	// this to point at an httptest.Server without touching real DNS/TLS.
	Client *http.Client
}

// WebhookSink delivers events as an HTTP POST of the event's JSON
// representation. Transport errors and 5xx responses are retried up to
// MaxRetries times with a linear backoff; 4xx responses are treated as
// permanent failures and are not retried.
type WebhookSink struct {
	cfg    WebhookConfig
	client *http.Client
}

// NewWebhook returns a WebhookSink for cfg.
func NewWebhook(cfg WebhookConfig) *WebhookSink {
	client := cfg.Client
	if client == nil {
		timeout := cfg.Timeout
		if timeout == 0 {
			timeout = 10 * time.Second
		}
		client = &http.Client{Timeout: timeout}
	}
	return &WebhookSink{cfg: cfg, client: client}
}

func (s *WebhookSink) Write(ctx context.Context, e event.ChangeEvent) error {
	body, err := e.JSON()
	if err != nil {
		return fmt.Errorf("webhook sink: encoding event: %w", err)
	}

	var lastErr error
	attempts := s.cfg.MaxRetries + 1
	for attempt := range attempts {
		if attempt > 0 {
			if err := s.wait(ctx, attempt); err != nil {
				return err
			}
		}

		status, err := s.post(ctx, body)
		if err == nil && status >= 200 && status < 300 {
			return nil
		}
		if err != nil {
			lastErr = err
			continue
		}
		if status >= 500 {
			lastErr = fmt.Errorf("webhook sink: server returned %d", status)
			continue
		}
		// 4xx and other non-retryable statuses: give up immediately.
		return fmt.Errorf("webhook sink: request rejected with status %d", status)
	}
	return fmt.Errorf("webhook sink: giving up after %d attempt(s): %w", attempts, lastErr)
}

func (s *WebhookSink) post(ctx context.Context, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range s.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("sending request: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body) // drain so the connection can be reused
	return resp.StatusCode, nil
}

func (s *WebhookSink) wait(ctx context.Context, attempt int) error {
	backoff := s.cfg.RetryBackoff
	if backoff <= 0 {
		backoff = 500 * time.Millisecond
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(attempt) * backoff):
		return nil
	}
}

func (s *WebhookSink) Close() error { return nil }

var _ Sink = (*WebhookSink)(nil)
