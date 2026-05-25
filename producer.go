// Package main provides the Kafka producer the student must implement.
//
// PURPOSE: deliver an idempotent producer that writes N messages to topic `orders`,
//          retries on transient network errors with exponential backoff, closes
//          gracefully on SIGINT, and logs partition+offset per ack.
//
// The author tests in producer_test.go define the exact contract. The student
// fills in the TODO blocks below. Tests use mocks — no real Kafka required.
package main

import (
	"context"
	"errors"
	"time"
)

// MessageWriter is the abstraction the producer writes through.
// Author exposes this interface so tests can inject a mock without needing
// a real broker. Student should implement Producer.Send by delegating to it.
type MessageWriter interface {
	WriteMessage(ctx context.Context, key, value []byte) (partition int32, offset int64, err error)
	Flush(timeout time.Duration) error
	Close() error
}

// Producer wraps a MessageWriter with idempotence + retry policy.
//
// Required behaviour (verified by producer_test.go):
//   - NewProducer returns error if brokers is empty.
//   - Send retries up to 3 times on transient errors (net.Error.Temporary()
//     OR errors returned by Writer that match the "transient" predicate
//     below). Backoff: 50ms → 100ms → 200ms.
//   - Send returns (partition, offset, nil) on success.
//   - Close calls Flush(5s) then writer.Close(); idempotent.
type Producer struct {
	// TODO: declare internal fields. At minimum:
	// - writer MessageWriter
	// - closed bool
	// - logger func(format string, args ...any)
}

// NewProducer constructs a Producer.
//
// Inputs:
//   - brokers: list of "host:port" pairs; MUST be non-empty.
//   - topic:   target topic name (e.g., "orders").
//   - writer:  the MessageWriter the producer writes through (in production,
//              constructed from kafka.Writer with enable.idempotence=true,
//              acks=all, retries=3; in tests, a mock).
//
// Returns:
//   - *Producer on success.
//   - nil + error if brokers is empty.
func NewProducer(brokers []string, topic string, writer MessageWriter) (*Producer, error) {
	// TODO: validate inputs; construct Producer.
	_ = brokers
	_ = topic
	_ = writer
	return nil, errors.New("not implemented")
}

// Send writes one message to the topic.
//
// Retry policy:
//   - Up to 3 retries on transient errors.
//   - Backoff: 50ms, 100ms, 200ms.
//   - Honors ctx cancellation between retries.
//
// On every successful write, log:
//
//	[OK] topic=orders partition=<N> offset=<M>
//
// On final failure (3 retries exhausted), return the last error.
func (p *Producer) Send(ctx context.Context, key, value []byte) (partition int32, offset int64, err error) {
	// TODO: implement retry loop.
	_ = ctx
	_ = key
	_ = value
	return 0, 0, errors.New("not implemented")
}

// Close flushes pending writes (timeout 5s) and closes the underlying writer.
// Idempotent.
func (p *Producer) Close() error {
	// TODO: flush + close; remember idempotence.
	return errors.New("not implemented")
}

// RunUntilSignal sends `n` messages keyed "order-1", "order-2", ..., "order-N".
// On ctx cancellation (e.g., SIGINT trapped by the caller into ctx), stop
// sending new messages, call Close(), return ctx.Err().
//
// Used by main() to wire signal handling.
func RunUntilSignal(ctx context.Context, p *Producer, topic string, n int) error {
	// TODO: loop with ctx.Done() check between iterations.
	_ = topic
	_ = n
	return errors.New("not implemented")
}

// IsTransient returns true if err should trigger a retry.
//
// Suggested signals (student may extend):
//   - errors.Is(err, context.DeadlineExceeded)
//   - net.Error with Temporary() == true
//   - segmentio/kafka-go specific transient errors
//
// Auth errors, "topic does not exist", and other 4xx-like errors are NOT
// transient — return them immediately without retry.
//
// Exported (capitalized) so producer_test.go can verify classification.
func IsTransient(err error) bool {
	// TODO: classify.
	_ = err
	return false
}
