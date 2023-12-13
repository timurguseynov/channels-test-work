package main

import (
    "context"
    "math/rand"
    "sync"
    "testing"
    "time"

    "github.com/stretchr/testify/assert"
)

var tests = []struct {
    name                  string
    totalTasks            int
    totalTasksInitialized int
    totalSucceeded        int
    totalFailed           int
    f                     failingFunc
}{
    {"no failingFunc provided", 10, 0, 0, 0, nil},
    {"no tasks", 0, 0, 0, 0, failingFuncAllFail},
    {"all tasks succeed", 10, 10, 10, 0, failingFuncAllSucceed},
    {"all tasks fail", 10, 10, 0, 10, failingFuncAllFail},
}

func TestStepByStep(t *testing.T) {
    t.Parallel()

    for _, tt := range tests {

        t.Run(tt.name, func(t *testing.T) {
            ctx, cancel, wg := setup(30 * time.Second)
            defer cancel()

            appErrCh := testAppErrorHandler(ctx, t)

            creatorCh := creator(ctx, wg, appErrCh, tt.totalTasks, tt.f)
            wg.Wait()
            assert.Equal(t, tt.totalTasksInitialized, len(creatorCh), "wrong number of creatorCh tasks")

            workerCh := worker(ctx, wg, creatorCh, tt.totalTasks)
            wg.Wait()
            assert.Equal(t, tt.totalTasksInitialized, len(workerCh), "wrong number of workerCh tasks")

            succeededCh, failedCh := sorter(ctx, wg, workerCh, tt.totalTasks)
            wg.Wait()
            assert.Equal(t, tt.totalSucceeded, len(succeededCh), "wrong number of succeededCh tasks")
            assert.Equal(t, tt.totalFailed, len(failedCh), "wrong number of failedCh errors")

            tasksCollectedCh := tasksCollector(ctx, wg, succeededCh)
            wg.Wait()
            assert.Equal(t, 1, len(tasksCollectedCh), "wrong number of collected tasks")

            errorsCollectedCh := errorsCollector(ctx, wg, failedCh)
            wg.Wait()
            assert.Equal(t, 1, len(errorsCollectedCh), "wrong number of collected errors")

            tasks := extractTasks(tasksCollectedCh)
            assert.Equal(t, tt.totalSucceeded, len(tasks), "wrong number of result tasks")

            errors := extractErrors(errorsCollectedCh)
            assert.Equal(t, tt.totalFailed, len(errors), "wrong number of result errors")
        })
    }
}

func TestFlow(t *testing.T) {
    t.Parallel()

    for _, tt := range tests {

        t.Run(tt.name, func(t *testing.T) {
            ctx, cancel, wg := setup(30 * time.Second)
            defer cancel()

            appErrCh := testAppErrorHandler(ctx, t)

            creatorCh := creator(ctx, wg, appErrCh, tt.totalTasks, tt.f)
            workerCh := worker(ctx, wg, creatorCh, tt.totalTasks)
            succeededCh, failedCh := sorter(ctx, wg, workerCh, tt.totalTasks)
            tasksCollectedCh := tasksCollector(ctx, wg, succeededCh)
            errorsCollectedCh := errorsCollector(ctx, wg, failedCh)

            tasks := extractTasks(tasksCollectedCh)
            assert.Equal(t, tt.totalSucceeded, len(tasks), "wrong number of result tasks")

            errors := extractErrors(errorsCollectedCh)
            assert.Equal(t, tt.totalFailed, len(errors), "wrong number of result errors")
        })
    }
}

func setup(timeout time.Duration) (context.Context, context.CancelFunc, *sync.WaitGroup) {
    rand.Seed(time.Now().UnixNano())

    ctx, cancel := context.WithTimeout(context.Background(), timeout)

    wg := sync.WaitGroup{}

    return ctx, cancel, &wg
}

func testAppErrorHandler(ctx context.Context, t *testing.T) chan<- error {
    appErrCh := make(chan error)

    go func() {
        select {
        case err := <-appErrCh:
            if err != nil {
                close(appErrCh)
                assert.Error(t, err)
            }
        case <-ctx.Done():
            return
        }
    }()

    return appErrCh
}

func failingFuncAllFail(t task) task {
    t.initialErr = errSomethingWentWrong
    return t
}

func failingFuncAllSucceed(t task) task {
    return t
}

func extractTasks(tasksCollectedInCh <-chan map[int]task) map[int]task {
    var tasks map[int]task

    for t := range tasksCollectedInCh {
        tasks = t
    }

    return tasks
}

func extractErrors(errorsCollectedInCh <-chan []error) []error {
    var errors []error

    for e := range errorsCollectedInCh {
        errors = e
    }

    return errors
}
