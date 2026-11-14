package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

// defaultStopTimeout is the overall stop deadline when WithStopTimeout is not
// given.
const defaultStopTimeout = 30 * time.Second

// Stage is a set of services that Stop stops concurrently.
type Stage struct {
	// Names are the names the services were registered under.
	Names []string
	// Budget, if positive, is how long the stage may take. A service that has
	// not returned by then has its Shutdown context cancelled, and the next
	// stage starts. It is inside the overall stop deadline.
	Budget time.Duration
}

// Option configures an App.
type Option func(*App)

// WithDrainDelay sets how long Drain waits after reporting not ready, before
// the services are stopped. The default is no wait.
func WithDrainDelay(d time.Duration) Option {
	return func(a *App) { a.drainDelay = d }
}

// WithStopTimeout sets the deadline for draining and stopping together. The
// default is 30 seconds. Zero or less means no deadline other than the
// context given to Stop.
func WithStopTimeout(d time.Duration) Option {
	return func(a *App) { a.stopTimeout = d }
}

// WithStopStages sets the stages Stop stops services in. See the package
// documentation for what stops a service no stage names.
func WithStopStages(stages ...Stage) Option {
	return func(a *App) { a.stages = stages }
}

// App starts, watches and stops a set of services. See the package
// documentation.
type App struct {
	drainDelay  time.Duration
	stopTimeout time.Duration
	stages      []Stage
	readiness   *Readiness

	// op serialises Start and Stop; mu guards the fields below.
	op sync.Mutex

	mu       sync.Mutex
	entries  []*entry
	state    appState
	started  []*entry
	failed   chan error
	done     chan struct{}
	watchers sync.WaitGroup
}

type appState int

const (
	appNew appState = iota
	appStarted
	appStopped
)

type entry struct {
	name    string
	service Service
}

// New returns an App with no services.
func New(opts ...Option) *App {
	a := &App{
		stopTimeout: defaultStopTimeout,
		readiness:   NewReadiness(),
		done:        make(chan struct{}),
	}
	for _, opt := range opts {
		opt(a)
	}

	return a
}

// Readiness returns the App's readiness, for adding checks and for reporting
// as a health source.
func (a *App) Readiness() *Readiness {
	return a.readiness
}

// Add registers a service under name. Services start in the order they are
// added. It returns an error for an empty or repeated name, a nil service, or
// once the App has been started.
func (a *App) Add(name string, service Service) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch {
	case a.state != appNew:
		return ErrAlreadyStarted
	case name == "":
		return errors.New("lifecycle: service name is empty")
	case service == nil:
		return fmt.Errorf("lifecycle: service %q is nil", name)
	case slices.ContainsFunc(a.entries, func(e *entry) bool { return e.name == name }):
		return fmt.Errorf("lifecycle: service %q is already added", name)
	}

	a.entries = append(a.entries, &entry{name: name, service: service})
	if checker, ok := service.(readyChecker); ok {
		a.readiness.AddCheck(name, func(ctx context.Context) error {
			_, err := checker.Ready(ctx)

			return err
		})
	}

	return nil
}

// Start starts the services in the order they were added, then opens
// Readiness. If a service fails to start, the ones already started are
// stopped and Start returns the failure joined with any stop errors; the App
// cannot then be started again. An App can be started once.
func (a *App) Start(ctx context.Context) error {
	a.op.Lock()
	defer a.op.Unlock()

	a.mu.Lock()
	if a.state != appNew {
		a.mu.Unlock()

		return ErrAlreadyStarted
	}
	entries := slices.Clone(a.entries)
	if err := a.validateStages(entries); err != nil {
		a.mu.Unlock()

		return err
	}
	a.failed = make(chan error, len(entries))
	a.mu.Unlock()

	started := make([]*entry, 0, len(entries))
	for _, e := range entries {
		if err := e.service.Start(ctx); err != nil {
			a.mu.Lock()
			a.state = appStopped
			a.mu.Unlock()
			close(a.done)

			// ctx may be done, which may be why Start failed, and the
			// rollback must not inherit that.
			rollbackCtx, cancel := a.stopContext(context.WithoutCancel(ctx))
			defer cancel()

			return errors.Join(fmt.Errorf("start %s: %w", e.name, err), a.stopEntries(rollbackCtx, started))
		}
		started = append(started, e)
	}

	a.mu.Lock()
	a.started = started
	a.state = appStarted
	a.mu.Unlock()

	a.watch(started)
	a.readiness.MarkStarted()

	return nil
}

// watch fans the failures of the services that can fail into a.failed. Each
// watcher ends when its service fails or when the App stops, since a Failed
// channel is not required to close.
func (a *App) watch(started []*entry) {
	for _, e := range started {
		failer, ok := e.service.(Failer)
		if !ok {
			continue
		}
		failed := failer.Failed()
		a.watchers.Go(func() {
			select {
			case err := <-failed:
				// a.failed has room for every service, and each fails once.
				a.failed <- &ServiceError{Service: e.name, Err: err}
			case <-a.done:
			}
		})
	}
}

// Failed returns a channel that receives the failure of any service that
// implements Failer, as a *ServiceError, once Start has returned. It is nil
// before then, and never closes.
func (a *App) Failed() <-chan error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.failed
}

// Live returns nil while every started service that implements Liveness
// reports live, and otherwise the errors of those that do not, each naming its
// service.
func (a *App) Live(ctx context.Context) error {
	a.mu.Lock()
	started := slices.Clone(a.started)
	a.mu.Unlock()

	var errs []error
	for _, e := range started {
		live, ok := e.service.(Liveness)
		if !ok {
			continue
		}
		if err := live.Live(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", e.name, err))
		}
	}

	return errors.Join(errs...)
}

// Drain makes Readiness report not ready, then waits for the drain delay or
// for ctx to be done, so that traffic stops before the services do.
func (a *App) Drain(ctx context.Context) {
	a.readiness.BeginDrain()
	if a.drainDelay <= 0 {
		return
	}

	timer := time.NewTimer(a.drainDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

// Stop stops the started services in stages, within the stop timeout and
// ctx's deadline, whichever is sooner, and returns the errors joined. It does
// not drain; see Drain. It does nothing for an App that was never started,
// that failed to start, or that has already stopped.
func (a *App) Stop(ctx context.Context) error {
	a.op.Lock()
	defer a.op.Unlock()

	a.mu.Lock()
	if a.state != appStarted {
		a.mu.Unlock()

		return nil
	}
	a.state = appStopped
	started := a.started
	a.mu.Unlock()

	a.readiness.BeginDrain()
	close(a.done)

	ctx, cancel := a.stopContext(ctx)
	defer cancel()

	err := a.stopEntries(ctx, started)
	a.watchers.Wait()

	return err
}

// Run starts the services and blocks until ctx is done or a service fails,
// then stops them. When ctx is done it drains first and returns nil if the
// stop was clean. When a service fails it stops at once, without draining, and
// returns the *ServiceError joined with any stop errors. The drain and the
// stop share one stop timeout, and neither is cut short by ctx being done.
func (a *App) Run(ctx context.Context) error {
	if err := a.Start(ctx); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		stopCtx, cancel := a.stopContext(context.WithoutCancel(ctx))
		defer cancel()
		a.Drain(stopCtx)

		return a.Stop(stopCtx)
	case failure := <-a.Failed():
		stopCtx, cancel := a.stopContext(context.WithoutCancel(ctx))
		defer cancel()

		return errors.Join(failure, a.Stop(stopCtx))
	}
}

// stopContext returns ctx with the stop timeout applied.
func (a *App) stopContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if a.stopTimeout <= 0 {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, a.stopTimeout)
}

// validateStages checks that every stage names registered services, once.
func (a *App) validateStages(entries []*entry) error {
	seen := map[string]bool{}
	for _, stage := range a.stages {
		for _, name := range stage.Names {
			if !slices.ContainsFunc(entries, func(e *entry) bool { return e.name == name }) {
				return fmt.Errorf("lifecycle: stop stage names %q, which is not a service", name)
			}
			if seen[name] {
				return fmt.Errorf("lifecycle: service %q is in more than one stop stage", name)
			}
			seen[name] = true
		}
	}

	return nil
}

type plannedStage struct {
	entries []*entry
	budget  time.Duration
}

// plan returns the stages that stop started.
func (a *App) plan(started []*entry) []plannedStage {
	var stages []plannedStage
	planned := map[*entry]bool{}

	for _, stage := range a.stages {
		var entries []*entry
		for _, name := range stage.Names {
			i := slices.IndexFunc(started, func(e *entry) bool { return e.name == name })
			if i < 0 {
				continue
			}
			entries = append(entries, started[i])
			planned[started[i]] = true
		}
		if len(entries) > 0 {
			stages = append(stages, plannedStage{entries: entries, budget: stage.Budget})
		}
	}

	for _, e := range slices.Backward(started) {
		if !planned[e] {
			stages = append(stages, plannedStage{entries: []*entry{e}})
		}
	}

	return stages
}

// stopEntries stops started, a stage at a time.
func (a *App) stopEntries(ctx context.Context, started []*entry) error {
	stages := a.plan(started)
	errs := make([]error, 0, len(stages))
	for _, stage := range stages {
		errs = append(errs, stopStage(ctx, stage))
	}

	return errors.Join(errs...)
}

// stopStage stops the services of one stage concurrently.
func stopStage(ctx context.Context, stage plannedStage) error {
	if stage.budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, stage.budget)
		defer cancel()
	}

	errs := make([]error, len(stage.entries))
	var wg sync.WaitGroup
	for i, e := range stage.entries {
		wg.Go(func() {
			if err := e.service.Shutdown(ctx); err != nil {
				errs[i] = fmt.Errorf("stop %s: %w", e.name, err)
			}
		})
	}
	wg.Wait()

	return errors.Join(errs...)
}
