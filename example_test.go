package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dioad/lifecycle"
)

// printer is a Service that prints what it is asked to do.
type printer struct {
	name     string
	startErr error
}

func (p printer) Start(context.Context) error {
	if p.startErr != nil {
		fmt.Println("start", p.name, "failed")

		return p.startErr
	}
	fmt.Println("start", p.name)

	return nil
}

func (p printer) Shutdown(context.Context) error {
	fmt.Println("stop", p.name)

	return nil
}

// Services start in the order they are added. When one fails to start, the
// services that had already started are stopped, newest first, and Start
// returns the failure.
func ExampleApp() {
	app := lifecycle.New()
	for _, service := range []printer{
		{name: "store"},
		{name: "queue"},
		{name: "api", startErr: errors.New("address in use")},
		{name: "metrics"},
	} {
		if err := app.Add(service.name, service); err != nil {
			fmt.Println(err)

			return
		}
	}

	fmt.Println("result:", app.Start(context.Background()))
	// Output:
	// start store
	// start queue
	// start api failed
	// stop queue
	// stop store
	// result: start api: address in use
}

// Stop stops the services in stages. The api stops first, so that nothing is
// sending the worker work; the store, which no stage names, stops last.
func ExampleApp_Stop() {
	app := lifecycle.New(lifecycle.WithStopStages(
		lifecycle.Stage{Names: []string{"api"}, Budget: time.Second},
		lifecycle.Stage{Names: []string{"worker"}},
	))
	for _, name := range []string{"store", "worker", "api"} {
		if err := app.Add(name, printer{name: name}); err != nil {
			fmt.Println(err)

			return
		}
	}
	if err := app.Start(context.Background()); err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println("result:", app.Stop(context.Background()))
	// Output:
	// start store
	// start worker
	// start api
	// stop api
	// stop worker
	// stop store
	// result: <nil>
}

// A process is not ready until it has started, stays ready while a check is
// degraded, is not ready when a check fails, and is not ready once it is
// draining, so that traffic stops before its listeners close.
func ExampleReadiness() {
	readiness := lifecycle.NewReadiness()
	var searchErr, dbErr error
	readiness.AddCheck("db", func(context.Context) error { return dbErr })
	readiness.AddCheck("search", func(context.Context) error { return lifecycle.Degraded(searchErr) })

	report := func(when string) {
		status, err := readiness.Ready(context.Background())
		fmt.Printf("%s: status=%v err=%v\n", when, status, err)
	}

	report("before start")
	readiness.MarkStarted()
	report("started")

	searchErr = errors.New("slow")
	report("search degraded")

	dbErr = errors.New("connection refused")
	report("db down")

	dbErr = nil
	readiness.BeginDrain()
	report("draining")
	// Output:
	// before start: status=<nil> err=starting
	// started: status=map[db:ok search:ok] err=<nil>
	// search degraded: status=map[db:ok search:degraded: slow] err=<nil>
	// db down: status=map[db:failed: connection refused search:degraded: slow] err=db: connection refused
	// draining: status=<nil> err=draining
}
