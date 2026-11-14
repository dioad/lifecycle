package lifecycle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock is a clock tests move by hand.
type fakeClock struct{ now atomic.Pointer[time.Time] }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.set(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	return c
}

func (c *fakeClock) set(t time.Time)         { c.now.Store(&t) }
func (c *fakeClock) Now() time.Time          { return *c.now.Load() }
func (c *fakeClock) advance(d time.Duration) { c.set(c.Now().Add(d)) }

func TestReadiness_IsNotReadyUntilStartedAndNotReadyOnceDraining(t *testing.T) {
	t.Parallel()
	r := NewReadiness()

	_, err := r.Ready(t.Context())
	require.ErrorIs(t, err, ErrStarting, "the start gate")

	r.MarkStarted()
	_, err = r.Ready(t.Context())
	require.NoError(t, err)

	r.BeginDrain()
	_, err = r.Ready(t.Context())
	require.ErrorIs(t, err, ErrDraining)

	r.MarkStarted()
	_, err = r.Ready(t.Context())
	require.ErrorIs(t, err, ErrDraining, "a drain is not undone")
}

func TestReadiness_ChecksRunInOrderAndAFailureMakesItNotReady(t *testing.T) {
	t.Parallel()
	r := NewReadiness()
	r.AddCheck("db", func(context.Context) error { return nil })
	r.AddCheck("cache", func(context.Context) error { return errors.New("connection refused") })
	r.AddCheck("queue", func(context.Context) error { return errors.New("full") })
	r.MarkStarted()

	status, err := r.Ready(t.Context())

	require.EqualError(t, err, "cache: connection refused; queue: full")
	assert.Equal(t, map[string]string{"db": "ok", "cache": "failed: connection refused", "queue": "failed: full"}, status)
}

func TestReadiness_ADegradedCheckIsReportedButTheProcessStaysReady(t *testing.T) {
	t.Parallel()
	r := NewReadiness()
	r.AddCheck("db", func(context.Context) error { return nil })
	r.AddCheck("search", func(context.Context) error { return Degraded(errors.New("slow")) })
	r.MarkStarted()

	status, err := r.Ready(t.Context())

	require.NoError(t, err)
	assert.Equal(t, map[string]string{"db": "ok", "search": "degraded: slow"}, status)
}

func TestDegraded(t *testing.T) {
	t.Parallel()

	require.NoError(t, Degraded(nil))

	err := Degraded(errBoom)
	require.ErrorIs(t, err, errBoom)
	assert.EqualError(t, err, "boom")
}

func TestReadiness_CachesAResultForItsTTL(t *testing.T) {
	t.Parallel()
	clock := newFakeClock()
	r := NewReadiness(WithClock(clock.Now))
	var runs atomic.Int32
	var failure atomic.Pointer[error]
	r.AddCheck("db", func(context.Context) error {
		runs.Add(1)
		if err := failure.Load(); err != nil {
			return *err
		}

		return nil
	}, WithCacheTTL(10*time.Second))
	r.MarkStarted()

	_, err := r.Ready(t.Context())
	require.NoError(t, err)
	failing := errors.New("down")
	failure.Store(&failing)
	clock.advance(9 * time.Second)
	_, err = r.Ready(t.Context())
	require.NoError(t, err, "the cached result is served inside the TTL")
	assert.Equal(t, int32(1), runs.Load())

	clock.advance(time.Second)
	_, err = r.Ready(t.Context())
	require.ErrorContains(t, err, "db: down", "the TTL has passed")
	assert.Equal(t, int32(2), runs.Load())

	failure.Store(nil)
	_, err = r.Ready(t.Context())
	require.ErrorContains(t, err, "db: down", "a failure is cached too")
	assert.Equal(t, int32(2), runs.Load())
}

func TestReadiness_WithoutATTLEveryReadyRunsTheCheck(t *testing.T) {
	t.Parallel()
	r := NewReadiness()
	var runs atomic.Int32
	r.AddCheck("db", func(context.Context) error { runs.Add(1); return nil })
	r.MarkStarted()

	_, _ = r.Ready(t.Context())
	_, _ = r.Ready(t.Context())

	assert.Equal(t, int32(2), runs.Load())
}

func TestReadiness_DoesNotCacheAResultFromACancelledProbe(t *testing.T) {
	t.Parallel()
	r := NewReadiness()
	var runs atomic.Int32
	r.AddCheck("db", func(ctx context.Context) error { runs.Add(1); return ctx.Err() }, WithCacheTTL(time.Hour))
	r.MarkStarted()
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := r.Ready(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	_, err = r.Ready(t.Context())

	require.NoError(t, err, "the next probe runs the check again")
	assert.Equal(t, int32(2), runs.Load())
}
