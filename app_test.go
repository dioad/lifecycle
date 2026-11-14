package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApp_Start_StartsInRegistrationOrder(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	app := newApp(t, log, nil, "store", "worker", "api")

	require.NoError(t, app.Start(t.Context()))

	assert.Equal(t, []string{"start store", "start worker", "start api"}, log.snapshot())
}

func TestApp_Start_RollsBackOnlyWhatStarted(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	app := New()
	require.NoError(t, app.Add("store", &fakeService{name: "store", log: log}))
	require.NoError(t, app.Add("worker", &fakeService{name: "worker", log: log, shutdownErr: errors.New("worker stuck")}))
	require.NoError(t, app.Add("api", &fakeService{name: "api", log: log, startErr: errBoom}))
	require.NoError(t, app.Add("never", &fakeService{name: "never", log: log}))

	err := app.Start(t.Context())

	require.ErrorIs(t, err, errBoom)
	require.ErrorContains(t, err, "start api")
	require.ErrorContains(t, err, "stop worker: worker stuck", "a rollback error is joined with the start error")
	assert.Equal(t, []string{
		"start store", "start worker", "start-failed api",
		"stop worker", "stop store",
	}, log.snapshot(), "only started services are stopped, in reverse, and the failed one is not")
	_, readyErr := app.Readiness().Ready(t.Context())
	require.Error(t, readyErr, "an App that failed to start never becomes ready")
}

func TestApp_Start_RollsBackWhenItsContextIsDone(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	ctx, cancel := context.WithCancel(t.Context())
	app := New()
	require.NoError(t, app.Add("store", &fakeService{name: "store", log: log, onShutdown: func(ctx context.Context) error {
		return ctx.Err()
	}}))
	require.NoError(t, app.Add("api", &fakeService{name: "api", log: log, startErr: context.Canceled}))
	cancel()

	err := app.Start(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.NotContains(t, err.Error(), "stop store", "the rollback does not inherit the cancelled context")
}

func TestApp_Start_OnlyOnce(t *testing.T) {
	t.Parallel()
	app := newApp(t, &eventLog{}, nil, "api")
	require.NoError(t, app.Start(t.Context()))

	require.ErrorIs(t, app.Start(t.Context()), ErrAlreadyStarted)
	require.ErrorIs(t, app.Add("late", &fakeService{log: &eventLog{}}), ErrAlreadyStarted)
}

func TestApp_Add_RejectsBadServices(t *testing.T) {
	t.Parallel()
	app := New()
	require.NoError(t, app.Add("api", &fakeService{log: &eventLog{}}))

	require.ErrorContains(t, app.Add("", &fakeService{log: &eventLog{}}), "name is empty")
	require.ErrorContains(t, app.Add("nil", nil), "is nil")
	require.ErrorContains(t, app.Add("api", &fakeService{log: &eventLog{}}), "already added")
}

func TestApp_Start_RejectsBadStages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stages  []Stage
		wantErr string
	}{
		{"unknown service", []Stage{{Names: []string{"missing"}}}, `"missing", which is not a service`},
		{"service in two stages", []Stage{{Names: []string{"api"}}, {Names: []string{"api"}}}, "more than one stop stage"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			log := &eventLog{}
			app := newApp(t, log, []Option{WithStopStages(tt.stages...)}, "api")

			require.ErrorContains(t, app.Start(t.Context()), tt.wantErr)
			assert.Empty(t, log.snapshot(), "nothing is started for a configuration that is wrong")
		})
	}
}

func TestApp_Stop_DefaultsToReverseRegistrationOrder(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	app := newApp(t, log, nil, "store", "worker", "api")
	require.NoError(t, app.Start(t.Context()))

	require.NoError(t, app.Stop(t.Context()))

	assert.Equal(t, []string{"stop api", "stop worker", "stop store"}, log.snapshot()[3:])
}

func TestApp_Stop_RunsStagesInOrderAndAStageConcurrently(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	// Each service in the first stage waits for the other to have begun
	// stopping, which only happens if they are stopped concurrently.
	var begun sync.WaitGroup
	begun.Add(2)
	meet := func(ctx context.Context) error {
		begun.Done()
		done := make(chan struct{})
		go func() { begun.Wait(); close(done) }()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	app := New(WithStopStages(
		Stage{Names: []string{"api", "grpc"}},
		Stage{Names: []string{"worker"}},
	))
	require.NoError(t, app.Add("store", &fakeService{name: "store", log: log}))
	require.NoError(t, app.Add("worker", &fakeService{name: "worker", log: log}))
	require.NoError(t, app.Add("api", &fakeService{name: "api", log: log, onShutdown: meet}))
	require.NoError(t, app.Add("grpc", &fakeService{name: "grpc", log: log, onShutdown: meet}))
	require.NoError(t, app.Start(t.Context()))

	within(t, func() { require.NoError(t, app.Stop(t.Context())) })

	stops := log.snapshot()[4:]
	assert.ElementsMatch(t, []string{"stop api", "stop grpc"}, stops[:2], "the first stage stops first")
	assert.Equal(t, []string{"stop worker", "stop store"}, stops[2:], "then the next stage, then what no stage names")
}

func TestApp_Stop_AStageBudgetCutsOffASlowServiceAndTheNextStageRuns(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	app := New(WithStopStages(
		Stage{Names: []string{"slow"}, Budget: 20 * time.Millisecond},
		Stage{Names: []string{"store"}},
	))
	require.NoError(t, app.Add("store", &fakeService{name: "store", log: log}))
	require.NoError(t, app.Add("slow", &fakeService{name: "slow", log: log, onShutdown: func(ctx context.Context) error {
		<-ctx.Done()

		return ctx.Err()
	}}))
	require.NoError(t, app.Start(t.Context()))

	var err error
	within(t, func() { err = app.Stop(t.Context()) })

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "stop slow")
	assert.Equal(t, []string{"stop slow", "stop store"}, log.snapshot()[2:])
}

func TestApp_Stop_TheStopTimeoutBoundsEveryStage(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	blockUntilDone := func(ctx context.Context) error {
		<-ctx.Done()

		return ctx.Err()
	}
	app := New(WithStopTimeout(30 * time.Millisecond))
	require.NoError(t, app.Add("a", &fakeService{name: "a", log: log, onShutdown: blockUntilDone}))
	require.NoError(t, app.Add("b", &fakeService{name: "b", log: log, onShutdown: blockUntilDone}))
	require.NoError(t, app.Start(t.Context()))

	var err error
	within(t, func() { err = app.Stop(t.Context()) })

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "stop a")
	require.ErrorContains(t, err, "stop b", "a later stage still runs, and finds the deadline passed")
}

func TestApp_Stop_JoinsTheErrorsOfEveryService(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	app := New()
	require.NoError(t, app.Add("a", &fakeService{name: "a", log: log, shutdownErr: errors.New("a failed")}))
	require.NoError(t, app.Add("b", &fakeService{name: "b", log: log, shutdownErr: errors.New("b failed")}))
	require.NoError(t, app.Start(t.Context()))

	err := app.Stop(t.Context())

	require.ErrorContains(t, err, "stop a: a failed")
	require.ErrorContains(t, err, "stop b: b failed")
}

func TestApp_Stop_IsANoOpUnlessStarted(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	app := newApp(t, log, nil, "api")

	require.NoError(t, app.Stop(t.Context()), "never started")
	assert.Empty(t, log.snapshot())

	require.NoError(t, app.Start(t.Context()))
	require.NoError(t, app.Stop(t.Context()))
	require.NoError(t, app.Stop(t.Context()), "already stopped")
	assert.Equal(t, []string{"start api", "stop api"}, log.snapshot(), "the service is stopped once")
}

func TestApp_Stop_ReturnsWhenNoServiceEverFails(t *testing.T) {
	t.Parallel()
	// A Failed channel does not receive or close for a service that does not
	// fail. Stop must still return, which means the watcher on that channel
	// has ended: a leaked watcher would hang the wait.
	app := New()
	svc := newFailingService("worker", &eventLog{})
	require.NoError(t, app.Add("worker", svc))
	require.NoError(t, app.Start(t.Context()))

	within(t, func() { require.NoError(t, app.Stop(t.Context())) })
}

func TestApp_Failed_ReportsAServiceErrorByName(t *testing.T) {
	t.Parallel()
	app := New()
	svc := newFailingService("worker", &eventLog{})
	require.NoError(t, app.Add("worker", svc))
	require.NoError(t, app.Add("api", &fakeService{name: "api", log: &eventLog{}}))
	require.NoError(t, app.Start(t.Context()))
	t.Cleanup(func() { _ = app.Stop(context.Background()) })

	svc.failed <- errBoom

	select {
	case err := <-app.Failed():
		failure, ok := errors.AsType[*ServiceError](err)
		require.True(t, ok)
		assert.Equal(t, "worker", failure.Service)
		require.ErrorIs(t, err, errBoom)
	case <-time.After(5 * time.Second):
		t.Fatal("the failure was not reported")
	}
}

func TestApp_Live_NamesTheServicesThatAreNotLive(t *testing.T) {
	t.Parallel()
	app := New()
	good := newFailingService("good", &eventLog{})
	bad := newFailingService("bad", &eventLog{})
	bad.live = errBoom
	require.NoError(t, app.Add("good", good))
	require.NoError(t, app.Add("bad", bad))
	require.NoError(t, app.Add("plain", &fakeService{name: "plain", log: &eventLog{}}))
	require.NoError(t, app.Start(t.Context()))
	t.Cleanup(func() { _ = app.Stop(context.Background()) })

	err := app.Live(t.Context())

	require.ErrorIs(t, err, errBoom)
	require.ErrorContains(t, err, "bad: boom")
	assert.NotContains(t, err.Error(), "good")
}

func TestApp_ServiceReadinessIsACheckOfThatName(t *testing.T) {
	t.Parallel()
	app := New()
	svc := newFailingService("store", &eventLog{})
	svc.ready = errors.New("replica behind")
	require.NoError(t, app.Add("store", svc))
	require.NoError(t, app.Start(t.Context()))
	t.Cleanup(func() { _ = app.Stop(context.Background()) })

	status, err := app.Readiness().Ready(t.Context())

	require.ErrorContains(t, err, "store: replica behind")
	assert.Equal(t, map[string]string{"store": "failed: replica behind"}, status)
}

func TestApp_Run_OnCancelDrainsThenStopsAndReturnsNil(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	app := New(WithDrainDelay(20 * time.Millisecond))
	var readyWhenStopping error
	require.NoError(t, app.Add("api", &fakeService{name: "api", log: log, onShutdown: func(ctx context.Context) error {
		_, readyWhenStopping = app.Readiness().Ready(ctx)

		return nil
	}}))
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- app.Run(ctx) }()

	require.Eventually(t, func() bool {
		_, err := app.Readiness().Ready(t.Context())

		return err == nil
	}, 5*time.Second, time.Millisecond, "the App becomes ready once started")
	cancel()

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	assert.Equal(t, []string{"start api", "stop api"}, log.snapshot())
	require.ErrorIs(t, readyWhenStopping, ErrDraining)
}

func TestApp_Run_OnFailureStopsAtOnceWithoutDraining(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	// A drain delay this long would fail the test if the failure were drained.
	app := New(WithDrainDelay(time.Hour))
	svc := newFailingService("worker", log)
	svc.shutdownErr = errors.New("worker stuck")
	require.NoError(t, app.Add("worker", svc))
	require.NoError(t, app.Add("api", &fakeService{name: "api", log: log}))
	result := make(chan error, 1)
	go func() { result <- app.Run(t.Context()) }()
	require.Eventually(t, func() bool { return len(log.snapshot()) == 2 }, 5*time.Second, time.Millisecond)

	svc.failed <- errBoom

	select {
	case err := <-result:
		failure, ok := errors.AsType[*ServiceError](err)
		require.True(t, ok, "Run returns the failure")
		assert.Equal(t, "worker", failure.Service)
		require.ErrorIs(t, err, errBoom)
		require.ErrorContains(t, err, "stop worker: worker stuck", "joined with the stop errors")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return: the failure was drained")
	}
	assert.Equal(t, []string{"start worker", "start api", "stop api", "stop worker"}, log.snapshot())
}

func TestApp_Run_ReturnsAStartFailure(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	app := New()
	require.NoError(t, app.Add("store", &fakeService{name: "store", log: log}))
	require.NoError(t, app.Add("api", &fakeService{name: "api", log: log, startErr: errBoom}))

	err := app.Run(t.Context())

	require.ErrorIs(t, err, errBoom)
	assert.Equal(t, []string{"start store", "start-failed api", "stop store"}, log.snapshot())
}

func TestApp_Run_StopIsNotCutShortByACancelledContext(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	app := New()
	var stopCtxErr error
	require.NoError(t, app.Add("api", &fakeService{name: "api", log: log, onShutdown: func(ctx context.Context) error {
		stopCtxErr = ctx.Err()

		return nil
	}}))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.NoError(t, app.Run(ctx))

	require.NoError(t, stopCtxErr, "Shutdown gets a live context although Run's was cancelled")
}

func TestApp_Drain_WaitsForTheDelayOrTheContext(t *testing.T) {
	t.Parallel()

	t.Run("the delay", func(t *testing.T) {
		t.Parallel()
		app := New(WithDrainDelay(30 * time.Millisecond))

		start := time.Now()
		app.Drain(t.Context())

		assert.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond)
		_, err := app.Readiness().Ready(t.Context())
		require.ErrorIs(t, err, ErrDraining)
	})

	t.Run("a context that is done", func(t *testing.T) {
		t.Parallel()
		app := New(WithDrainDelay(time.Hour))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		within(t, func() { app.Drain(ctx) })
	})
}
