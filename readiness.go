package lifecycle

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"
)

// CheckFunc reports whether one thing a process depends on is working. It
// returns nil when it is, and an error saying why not when it is not. Wrap the
// error in Degraded to report a problem that does not stop the process taking
// traffic.
type CheckFunc func(ctx context.Context) error

// Degraded marks err as a degradation: the check that returned it is reported
// by Readiness, but the process stays ready. It returns nil if err is nil.
func Degraded(err error) error {
	if err == nil {
		return nil
	}

	return &degradedError{err: err}
}

type degradedError struct{ err error }

func (e *degradedError) Error() string { return e.err.Error() }
func (e *degradedError) Unwrap() error { return e.err }

// Readiness reports whether a process should be sent traffic. It is not ready
// until MarkStarted, not ready once BeginDrain, and in between ready unless a
// check fails.
//
// It has the Ready method that dioad/net's http.Server.AddHealthSource looks
// for. The zero value is not usable; call NewReadiness.
type Readiness struct {
	now func() time.Time

	mu     sync.Mutex
	state  readinessState
	checks []*check
}

type readinessState int

const (
	stateStarting readinessState = iota
	stateReady
	stateDraining
)

// ReadinessOption configures a Readiness.
type ReadinessOption func(*Readiness)

// WithClock sets the clock that check results are cached against. It is for
// tests.
func WithClock(now func() time.Time) ReadinessOption {
	return func(r *Readiness) { r.now = now }
}

// NewReadiness returns a Readiness that is not ready until MarkStarted.
func NewReadiness(opts ...ReadinessOption) *Readiness {
	r := &Readiness{now: time.Now}
	for _, opt := range opts {
		opt(r)
	}

	return r
}

// DefaultCheckTimeout is how long a check may run before its context is
// cancelled, unless WithCheckTimeout says otherwise.
const DefaultCheckTimeout = 5 * time.Second

// CheckOption configures a check added with AddCheck.
type CheckOption func(*check)

// WithCheckTimeout bounds how long the check may run. When it expires the
// check's context is cancelled, so a wedged dependency fails the probe quickly
// instead of hanging the load balancer's request. The default is
// DefaultCheckTimeout; zero or less removes the limit. The check must return
// when its context is done.
func WithCheckTimeout(d time.Duration) CheckOption {
	return func(c *check) { c.timeout = d }
}

// WithCacheTTL caches the check's result for ttl, so that a health probe that
// arrives every second does not query a database every second. A result is
// not cached when the probe's context is done, since that says nothing about
// the thing checked. Without it every Ready runs the check.
func WithCacheTTL(ttl time.Duration) CheckOption {
	return func(c *check) { c.ttl = ttl }
}

// WithDegraded reports the check's failures as degradations: the check is
// reported, but the process stays ready. Use it for a dependency the process
// can run without. It is the same as the check returning Degraded(err).
func WithDegraded() CheckOption {
	return func(c *check) { c.degraded = true }
}

// AddCheck adds a named check. Checks run in the order they were added.
func (r *Readiness) AddCheck(name string, fn CheckFunc, opts ...CheckOption) {
	c := &check{name: name, fn: fn, timeout: DefaultCheckTimeout}
	for _, opt := range opts {
		opt(c)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks = append(r.checks, c)
}

// MarkStarted opens the start gate: from now on Ready runs the checks. App
// does this when every service has started.
func (r *Readiness) MarkStarted() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == stateStarting {
		r.state = stateReady
	}
}

// BeginDrain makes Ready report not ready from now on, so that traffic stops
// before listeners close. App does this when it drains or stops.
func (r *Readiness) BeginDrain() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = stateDraining
}

// Ready reports whether the process should be sent traffic. The first result
// is a status for each check, by name: "ok", "degraded: <reason>" or
// "failed: <reason>". The error is non-nil before MarkStarted (ErrStarting),
// after BeginDrain (ErrDraining), and when a check has failed; a degraded
// check does not make it non-nil.
func (r *Readiness) Ready(ctx context.Context) (any, error) {
	r.mu.Lock()
	state := r.state
	checks := slices.Clone(r.checks)
	r.mu.Unlock()

	switch state {
	case stateStarting:
		return nil, ErrStarting
	case stateDraining:
		return nil, ErrDraining
	case stateReady:
	}

	status := make(map[string]string, len(checks))
	var failures checkErrors
	for _, c := range checks {
		err := c.run(ctx, r.now)
		switch {
		case err == nil:
			status[c.name] = "ok"
		case isDegraded(err):
			status[c.name] = "degraded: " + err.Error()
		default:
			status[c.name] = "failed: " + err.Error()
			failures = append(failures, &namedError{name: c.name, err: err})
		}
	}
	if len(failures) > 0 {
		return status, failures
	}

	return status, nil
}

func isDegraded(err error) bool {
	_, ok := errors.AsType[*degradedError](err)

	return ok
}

// check is one named CheckFunc and its cached result.
type check struct {
	name     string
	fn       CheckFunc
	ttl      time.Duration
	timeout  time.Duration
	degraded bool

	mu     sync.Mutex
	cached bool
	at     time.Time
	err    error
}

// run returns the check's result, from the cache while it is fresh. It holds
// the check's lock while the check runs, so concurrent probes share one run.
func (c *check) run(ctx context.Context, now func() time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ttl > 0 && c.cached && now().Sub(c.at) < c.ttl {
		return c.err
	}

	runCtx := ctx
	if c.timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	err := c.fn(runCtx)
	if err != nil && c.degraded {
		err = Degraded(err)
	}
	if c.ttl > 0 && ctx.Err() == nil {
		c.cached, c.at, c.err = true, now(), err
	}

	return err
}

type namedError struct {
	name string
	err  error
}

func (e *namedError) Error() string { return e.name + ": " + e.err.Error() }
func (e *namedError) Unwrap() error { return e.err }

// checkErrors is the error Ready returns when checks have failed.
type checkErrors []error

func (f checkErrors) Error() string {
	messages := make([]string, len(f))
	for i, err := range f {
		messages[i] = err.Error()
	}

	return strings.Join(messages, "; ")
}

func (f checkErrors) Unwrap() []error { return f }
