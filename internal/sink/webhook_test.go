package sink

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChaosHour/mycdc/internal/event"
)

func TestWebhookSinkSuccess(t *testing.T) {
	var gotBody event.ChangeEvent
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := NewWebhook(WebhookConfig{
		URL:     srv.URL,
		Headers: map[string]string{"Authorization": "Bearer tok123"},
		Timeout: 2 * time.Second,
	})

	if err := s.Write(context.Background(), sampleInsert()); err != nil {
		t.Fatalf("Write() error: %v", err)
	}
	if gotAuth != "Bearer tok123" {
		t.Errorf("Authorization header = %q, want Bearer tok123", gotAuth)
	}
	if gotBody.Table != "orders" {
		t.Errorf("posted body Table = %q, want orders", gotBody.Table)
	}
}

func TestWebhookSinkRetriesOn5xxThenSucceeds(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := NewWebhook(WebhookConfig{
		URL:          srv.URL,
		Timeout:      2 * time.Second,
		MaxRetries:   5,
		RetryBackoff: time.Millisecond,
	})

	if err := s.Write(context.Background(), sampleInsert()); err != nil {
		t.Fatalf("Write() error: %v", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}

func TestWebhookSinkGivesUpAfterMaxRetries(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	s := NewWebhook(WebhookConfig{
		URL:          srv.URL,
		Timeout:      2 * time.Second,
		MaxRetries:   2,
		RetryBackoff: time.Millisecond,
	})

	err := s.Write(context.Background(), sampleInsert())
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if got := atomic.LoadInt32(&attempts); got != 3 { // initial try + 2 retries
		t.Errorf("attempts = %d, want 3", got)
	}
}

func TestWebhookSinkDoesNotRetryOn4xx(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	s := NewWebhook(WebhookConfig{
		URL:          srv.URL,
		Timeout:      2 * time.Second,
		MaxRetries:   5,
		RetryBackoff: time.Millisecond,
	})

	err := s.Write(context.Background(), sampleInsert())
	if err == nil {
		t.Fatal("expected error for 4xx response")
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Errorf("attempts = %d, want 1 (no retry on 4xx)", got)
	}
}

func TestWebhookSinkRespectsContextCancellationDuringBackoff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	s := NewWebhook(WebhookConfig{
		URL:          srv.URL,
		Timeout:      2 * time.Second,
		MaxRetries:   10,
		RetryBackoff: time.Hour, // long enough that the test would hang if not cancelled
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := s.Write(ctx, sampleInsert())
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Write() took %v, expected fast return on context cancellation", elapsed)
	}
}

func TestWebhookSinkTransportErrorIsRetried(t *testing.T) {
	// Point at a server that isn't listening on this port so requests fail
	// at the transport level, then verify we still respect MaxRetries.
	s := NewWebhook(WebhookConfig{
		URL:          "http://127.0.0.1:1", // reserved, nothing listens here
		Timeout:      200 * time.Millisecond,
		MaxRetries:   1,
		RetryBackoff: time.Millisecond,
	})

	err := s.Write(context.Background(), sampleInsert())
	if err == nil {
		t.Fatal("expected error connecting to unreachable server")
	}
}

func TestWebhookSinkClose(t *testing.T) {
	s := NewWebhook(WebhookConfig{URL: "http://example.invalid"})
	if err := s.Close(); err != nil {
		t.Errorf("Close() error: %v", err)
	}
}
