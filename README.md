# lifecycle

Process lifecycle for Dioad daemons: ordered start with rollback, staged stop with budgets, failure fan-in, readiness and drain.

```go
app := lifecycle.New(
	lifecycle.WithDrainDelay(5*time.Second),
	lifecycle.WithStopStages(
		lifecycle.Stage{Names: []string{"api"}, Budget: 10 * time.Second},
		lifecycle.Stage{Names: []string{"worker"}},
	),
)
if err := app.Add("store", store); err != nil {
	return err
}
if err := app.Add("worker", worker); err != nil {
	return err
}
if err := app.Add("api", apiServer); err != nil {
	return err
}

// Report readiness wherever the platform looks for it, for example
// dioad/net's http.Server.AddHealthSource.
app.Readiness().AddCheck("store", store.Ping, lifecycle.WithCacheTTL(5*time.Second))

return app.Run(ctx) // returns when ctx is done or a service fails
```

| | |
|---|---|
| **Start** | In registration order. A failure stops the services that had started, in the same way Stop would, and is returned joined with any stop errors. |
| **Stop** | In stages: the services of a stage stop concurrently, each stage optionally within a budget (and optionally naming services that were not added), all within the drain delay plus a stop budget, 30 seconds by default. Without stages, in reverse registration order. Errors are joined. |
| **Failure** | A service that implements `Failed() <-chan error` has its failure reported on `App.Failed` as a `*ServiceError`. `Run` stops at once on a failure, without draining. |
| **Readiness** | Not ready until started, not ready once draining, and in between ready unless a named check fails. A check runs under a timeout (5 seconds by default), can be cached for a TTL, and can be `Degraded`: reported, but still ready. It is a health source for `dioad/net`'s HTTP server, without importing it. |
| **Drain** | When `Run`'s context is done, readiness reports not ready and `Run` waits for the drain delay before it stops the services, so the load balancer stops first. |

A `Service` is `Start(ctx) error` and `Shutdown(ctx) error`: the same method set as `dioad/net`'s `http.Server`, so
one is a `Service` with no adapter. `Failed`, `Live` and `Ready` are optional, found by interface assertion.

`StopFunc` releases a resource on stop, and `Workers` runs background goroutines through the drain.

See the package documentation for the details, and the examples for runnable versions.
