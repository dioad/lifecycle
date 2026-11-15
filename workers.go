package lifecycle

import (
	"context"
	"fmt"
	"sync"
)

// StopFunc is a Service that does nothing on Start and calls the function on
// Shutdown, for a resource that only has to be released, such as a database.
// Services stop in reverse registration order unless a stage says otherwise,
// so adding it before the services that use it makes it the last to stop.
type StopFunc func(ctx context.Context) error

// Start does nothing.
func (StopFunc) Start(context.Context) error { return nil }

// Shutdown calls f.
func (f StopFunc) Shutdown(ctx context.Context) error { return f(ctx) }

// Workers is a Service for background goroutines. Start calls the launch
// function, which starts them; Shutdown cancels their context and waits for
// them to return.
//
// The workers' context outlives the one passed to Start, which an App cancels
// when it begins to drain, so the workers keep running while the load balancer
// stops sending traffic and stop only when their stage does.
type Workers struct {
	launch func(ctx context.Context, wg *sync.WaitGroup) error

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWorkers returns a Workers that calls launch on Start. launch starts its
// goroutines on wg and returns without waiting for them; they must return when
// the context is done.
func NewWorkers(launch func(ctx context.Context, wg *sync.WaitGroup) error) *Workers {
	return &Workers{launch: launch}
}

// Start calls the launch function with a context that is cancelled by
// Shutdown, and that keeps the values, such as the logger, of ctx. If launch
// fails, what it started is stopped before Start returns.
func (w *Workers) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	w.mu.Lock()
	w.cancel = cancel
	w.mu.Unlock()

	if err := w.launch(runCtx, &w.wg); err != nil {
		cancel()
		w.wg.Wait()

		return err
	}

	return nil
}

// Shutdown cancels the workers' context and waits for them to return, or for
// ctx to be done. It does nothing if Start was never called.
func (w *Workers) Shutdown(ctx context.Context) error {
	w.mu.Lock()
	cancel := w.cancel
	w.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("workers did not stop: %w", ctx.Err())
	}
}
