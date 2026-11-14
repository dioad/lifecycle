package lifecycle

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// eventLog records what the fake services were asked to do, in order.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Clone(l.events)
}

// fakeService is a Service that records its calls and can be told to fail.
type fakeService struct {
	name        string
	log         *eventLog
	startErr    error
	shutdownErr error
	// onShutdown, if set, runs inside Shutdown, with the context it was given.
	onShutdown func(ctx context.Context) error
}

func (s *fakeService) Start(context.Context) error {
	if s.startErr != nil {
		s.log.add("start-failed " + s.name)

		return s.startErr
	}
	s.log.add("start " + s.name)

	return nil
}

func (s *fakeService) Shutdown(ctx context.Context) error {
	s.log.add("stop " + s.name)
	if s.onShutdown != nil {
		return s.onShutdown(ctx)
	}

	return s.shutdownErr
}

// failingService is a fakeService that can fail after it has started, and
// can report liveness and readiness.
type failingService struct {
	fakeService

	failed chan error
	live   error
	ready  error
}

func newFailingService(name string, log *eventLog) *failingService {
	return &failingService{name: name, log: log, failed: make(chan error, 1)}
}

func (s *failingService) Failed() <-chan error               { return s.failed }
func (s *failingService) Live(context.Context) error         { return s.live }
func (s *failingService) Ready(context.Context) (any, error) { return "checked", s.ready }

// newApp returns an App with the named fake services added.
func newApp(t *testing.T, log *eventLog, opts []Option, names ...string) *App {
	t.Helper()

	app := New(opts...)
	for _, name := range names {
		require.NoError(t, app.Add(name, &fakeService{name: name, log: log}))
	}

	return app
}

// within fails the test if fn does not return in time.
func within(t *testing.T, fn func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("did not return in time")
	}
}

var errBoom = errors.New("boom")
