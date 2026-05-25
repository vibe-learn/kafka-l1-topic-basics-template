// Author-written tests for the Kafka producer assignment.
//
// These tests verify the producer.go contract using a MockWriter implementation
// of the MessageWriter interface. The student does NOT need real Kafka — pass
// the mock into NewProducer in tests.
//
// All tests must be GREEN for the homework to be accepted.
package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// MockWriter — fake MessageWriter for unit tests.
// ---------------------------------------------------------------------------

type mockWriter struct {
	mu              sync.Mutex
	writeCallCount  int
	flushCallCount  int
	closeCallCount  int
	failNextNTimes  int   // simulate transient failures on the first N calls
	failError       error // the error to return when failing
	partitionToGive int32
	offsetCounter   int64
	closed          bool
}

func (m *mockWriter) WriteMessage(ctx context.Context, key, value []byte) (int32, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeCallCount++
	if m.failNextNTimes > 0 {
		m.failNextNTimes--
		err := m.failError
		if err == nil {
			err = &fakeTempNetError{}
		}
		return 0, 0, err
	}
	m.offsetCounter++
	return m.partitionToGive, m.offsetCounter, nil
}

func (m *mockWriter) Flush(timeout time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushCallCount++
	return nil
}

func (m *mockWriter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCallCount++
	m.closed = true
	return nil
}

func (m *mockWriter) snapshot() (write, flush, closeC int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writeCallCount, m.flushCallCount, m.closeCallCount
}

// fakeTempNetError implements net.Error and is treated as transient.
type fakeTempNetError struct{}

func (e *fakeTempNetError) Error() string   { return "fake transient network error" }
func (e *fakeTempNetError) Timeout() bool   { return false }
func (e *fakeTempNetError) Temporary() bool { return true }

var _ net.Error = (*fakeTempNetError)(nil)

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestNewProducer_RejectsEmptyBrokers(t *testing.T) {
	w := &mockWriter{}
	p, err := NewProducer(nil, "orders", w)
	if err == nil {
		t.Fatalf("expected error for nil brokers, got nil")
	}
	if p != nil {
		t.Fatalf("expected nil producer on error, got %v", p)
	}
}

func TestNewProducer_AcceptsValidBrokers(t *testing.T) {
	w := &mockWriter{}
	p, err := NewProducer([]string{"localhost:9092"}, "orders", w)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if p == nil {
		t.Fatalf("expected non-nil producer")
	}
}

func TestSend_SuccessReturnsPartitionAndOffset(t *testing.T) {
	w := &mockWriter{partitionToGive: 2}
	p, err := NewProducer([]string{"localhost:9092"}, "orders", w)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	part, off, err := p.Send(context.Background(), []byte("k1"), []byte("v1"))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if part != 2 {
		t.Errorf("partition = %d, want 2", part)
	}
	if off != 1 {
		t.Errorf("offset = %d, want 1", off)
	}
}

func TestSend_RetriesOnTransientError(t *testing.T) {
	// Writer fails twice with a transient net error, then succeeds.
	w := &mockWriter{partitionToGive: 0, failNextNTimes: 2}
	p, err := NewProducer([]string{"localhost:9092"}, "orders", w)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	_, _, err = p.Send(context.Background(), []byte("k1"), []byte("v1"))
	if err != nil {
		t.Fatalf("Send should retry transient and succeed, got: %v", err)
	}
	writes, _, _ := w.snapshot()
	if writes != 3 {
		t.Errorf("expected 3 writer calls (2 fail + 1 success), got %d", writes)
	}
}

func TestSend_FailsAfter3Retries(t *testing.T) {
	// 4 transient failures — exceeds retry budget of 3.
	w := &mockWriter{failNextNTimes: 100, failError: &fakeTempNetError{}}
	p, err := NewProducer([]string{"localhost:9092"}, "orders", w)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	_, _, err = p.Send(context.Background(), []byte("k1"), []byte("v1"))
	if err == nil {
		t.Fatalf("expected error after retry budget exhausted")
	}
	writes, _, _ := w.snapshot()
	if writes < 3 || writes > 4 {
		t.Errorf("expected 3-4 writer calls (initial + 3 retries), got %d", writes)
	}
}

func TestSend_NoRetryOnNonTransient(t *testing.T) {
	// Permanent error (not net.Error.Temporary()).
	permanent := errors.New("authentication failed")
	w := &mockWriter{failNextNTimes: 1, failError: permanent}
	p, err := NewProducer([]string{"localhost:9092"}, "orders", w)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	_, _, err = p.Send(context.Background(), []byte("k1"), []byte("v1"))
	if err == nil {
		t.Fatalf("expected error to propagate")
	}
	writes, _, _ := w.snapshot()
	if writes != 1 {
		t.Errorf("expected 1 writer call (no retry on non-transient), got %d", writes)
	}
}

func TestClose_CallsFlushThenClose(t *testing.T) {
	w := &mockWriter{}
	p, _ := NewProducer([]string{"localhost:9092"}, "orders", w)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, flush, closeC := w.snapshot()
	if flush != 1 {
		t.Errorf("expected exactly 1 Flush call, got %d", flush)
	}
	if closeC != 1 {
		t.Errorf("expected exactly 1 Close call, got %d", closeC)
	}
}

func TestClose_Idempotent(t *testing.T) {
	w := &mockWriter{}
	p, _ := NewProducer([]string{"localhost:9092"}, "orders", w)
	if err := p.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close should not error, got: %v", err)
	}
	_, _, closeC := w.snapshot()
	if closeC > 1 {
		t.Errorf("expected at most 1 underlying Close call (idempotent), got %d", closeC)
	}
}

func TestRunUntilSignal_SendsNMessages(t *testing.T) {
	w := &mockWriter{}
	p, _ := NewProducer([]string{"localhost:9092"}, "orders", w)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := RunUntilSignal(ctx, p, "orders", 5)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected nil or context.Canceled, got: %v", err)
	}
	writes, _, _ := w.snapshot()
	if writes < 5 {
		t.Errorf("expected at least 5 writes for N=5, got %d", writes)
	}
}

func TestRunUntilSignal_StopsOnCancel(t *testing.T) {
	// Mock that artificially blocks each write briefly so cancellation can interrupt.
	w := &mockWriter{}
	p, _ := NewProducer([]string{"localhost:9092"}, "orders", w)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after 50ms — should stop sending before all 1000 are queued.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := RunUntilSignal(ctx, p, "orders", 1000)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
	writes, _, _ := w.snapshot()
	if writes >= 1000 {
		t.Errorf("RunUntilSignal should have stopped early; got %d writes", writes)
	}
	// Producer should have been Close()d (Flush + Close called).
	_, flush, closeC := w.snapshot()
	if flush == 0 || closeC == 0 {
		t.Errorf("expected Close() to flush+close; got flush=%d close=%d", flush, closeC)
	}
}
