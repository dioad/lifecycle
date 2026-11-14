package lifecycle

import (
	"context"
	"errors"
	"fmt"
)

// Service is something an App starts and stops: an HTTP server, a worker, a
// publisher, a store.
type Service interface {
	// Start brings the service up and returns once it is serving. It must not
	// block for the life of the service.
	Start(ctx context.Context) error
	// Shutdown stops the service. It must return when ctx is done.
	Shutdown(ctx context.Context) error
}

// Failer is implemented by a Service that can stop working after Start has
// returned. Failed returns a channel that receives, at most once, the error
// that ended the service. It need not close it.
type Failer interface {
	Failed() <-chan error
}

// Liveness is implemented by a Service that can report that it has stopped
// working. Live returns nil while the service is working.
type Liveness interface {
	Live(ctx context.Context) error
}

// readyChecker is implemented by a Service that can report whether it is
// ready. It is the method dioad/net calls a readiness resource.
type readyChecker interface {
	Ready(ctx context.Context) (any, error)
}

var (
	// ErrAlreadyStarted is returned by Start, and by Add, once the App has
	// been started.
	ErrAlreadyStarted = errors.New("lifecycle: app already started")
	// ErrStarting is the reason Readiness reports before the App has started.
	ErrStarting = errors.New("starting")
	// ErrDraining is the reason Readiness reports once the App is draining or
	// stopping.
	ErrDraining = errors.New("draining")
)

// ServiceError is a failure of a service, as reported on App.Failed.
type ServiceError struct {
	// Service is the name the service was registered under.
	Service string
	// Err is the error the service reported.
	Err error
}

// Error implements error.
func (f *ServiceError) Error() string {
	return fmt.Sprintf("service %s failed: %v", f.Service, f.Err)
}

// Unwrap returns the error the service reported.
func (f *ServiceError) Unwrap() error {
	return f.Err
}
