// Package lifecycle runs the services of a Dioad daemon: it starts them in
// order, rolling back on a failure; fans their failures into one channel;
// reports readiness; drains; and stops them in stages within a deadline.
//
// A daemon registers each service with an App, then calls Run:
//
//	app := lifecycle.New(lifecycle.WithDrainDelay(5 * time.Second))
//	if err := app.Add("store", store); err != nil {
//		return err
//	}
//	if err := app.Add("api", apiServer); err != nil {
//		return err
//	}
//	return app.Run(ctx)
//
// # Services
//
// A Service has Start and Shutdown. A service that can also fail after it has
// started implements Failer, and one that can report that it has stopped
// working implements Liveness. A service that implements Ready adds a check
// of that name to the App's Readiness. All three are discovered by interface
// assertion, so a service implements only what it has.
//
// The method set matches the one dioad/net's http.Server has (Start,
// Shutdown, Failed and Live), so a *http.Server is a Service with no adapter.
// It is Shutdown, not Stop, for that reason. A service's name is given when it
// is registered, not by the service.
//
// # Starting
//
// Start starts services in registration order. If one fails, the services
// that had already started are stopped, in the same way Stop would stop them,
// and Start returns the start error joined with any stop errors. A service
// whose Start fails is not stopped, so Start must clean up after itself.
//
// # Stopping
//
// Stop stops services in stages. A stage is a set of services stopped
// concurrently, optionally within a budget; the stages run one after another,
// all within one overall deadline (WithStopTimeout). With no stages
// configured every service is its own stage, stopped in reverse registration
// order. A service that no stage names is stopped after the stages, in reverse
// registration order. Errors are combined with errors.Join.
//
// A service's Shutdown must return when its context is done. The App cannot
// stop a Shutdown that ignores it.
//
// # Failure and drain
//
// A failure of a service that implements Failer is reported on App.Failed,
// wrapped in a ServiceError. Run, on a failure, stops the services at once;
// when its context is done instead, it first drains: Readiness reports not
// ready, and Run waits for the drain delay (WithDrainDelay) so that a load
// balancer stops sending requests before the listeners close.
//
// # Readiness
//
// Readiness is a health source: it has the Ready method that dioad/net's
// http.Server.AddHealthSource looks for, so it is reported by /health/ready
// without this package importing dioad/net. It is not ready until Start has
// returned (the start gate) and not ready once draining. In between it runs
// its named checks. A check can be cached for a time to live, and can report
// itself degraded, in which case the process stays ready and the check is
// reported.
package lifecycle
