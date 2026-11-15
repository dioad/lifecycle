package lifecycle

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStopFunc_DoesNothingOnStartAndCallsTheFunctionOnShutdown(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	app := New()
	require.NoError(t, app.Add("store", StopFunc(func(context.Context) error {
		log.add("close store")

		return nil
	})))
	require.NoError(t, app.Add("api", &fakeService{name: "api", log: log}))
	require.NoError(t, app.Start(t.Context()))

	require.NoError(t, app.Stop(t.Context()))

	assert.Equal(t, []string{"start api", "stop api", "close store"}, log.snapshot(), "the store, added first, is closed last")
}

func TestStopFunc_ReturnsTheError(t *testing.T) {
	t.Parallel()
	app := New()
	require.NoError(t, app.Add("store", StopFunc(func(context.Context) error { return errBoom })))
	require.NoError(t, app.Start(t.Context()))

	require.ErrorIs(t, app.Stop(t.Context()), errBoom)
}

func TestWorkers_OutliveTheStartContextAndStopOnShutdown(t *testing.T) {
	t.Parallel()
	stopped := make(chan struct{})
	workers := NewWorkers(func(ctx context.Context, wg *sync.WaitGroup) error {
		wg.Go(func() {
			<-ctx.Done()
			close(stopped)
		})

		return nil
	})

	// An App cancels the context it starts services with when it drains; the
	// workers must keep running through that.
	startCtx, cancel := context.WithCancel(t.Context())
	require.NoError(t, workers.Start(startCtx))
	cancel()
	select {
	case <-stopped:
		t.Fatal("workers stopped when the start context was cancelled")
	default:
	}

	require.NoError(t, workers.Shutdown(t.Context()))
	<-stopped
}

func TestWorkers_KeepTheValuesOfTheStartContext(t *testing.T) {
	t.Parallel()
	type key struct{}
	got := make(chan any, 1)
	workers := NewWorkers(func(ctx context.Context, _ *sync.WaitGroup) error {
		got <- ctx.Value(key{})

		return nil
	})

	require.NoError(t, workers.Start(context.WithValue(t.Context(), key{}, "logger")))
	require.NoError(t, workers.Shutdown(t.Context()))

	assert.Equal(t, "logger", <-got)
}

func TestWorkers_ALaunchFailureStopsWhatStarted(t *testing.T) {
	t.Parallel()
	stopped := make(chan struct{})
	workers := NewWorkers(func(ctx context.Context, wg *sync.WaitGroup) error {
		wg.Go(func() {
			<-ctx.Done()
			close(stopped)
		})

		return errBoom
	})

	require.ErrorIs(t, workers.Start(t.Context()), errBoom)
	<-stopped
}

func TestWorkers_ShutdownReturnsWhenItsContextIsDone(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	workers := NewWorkers(func(_ context.Context, wg *sync.WaitGroup) error {
		wg.Go(func() { <-release })

		return nil
	})
	require.NoError(t, workers.Start(t.Context()))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, workers.Shutdown(ctx), context.Canceled)
}

func TestWorkers_ShutdownBeforeStartIsANoOp(t *testing.T) {
	t.Parallel()

	require.NoError(t, NewWorkers(nil).Shutdown(t.Context()))
}
