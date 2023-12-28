package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	errSomethingWentWrong     = errors.New("something went wrong")
	errFailingFuncNotProvided = errors.New("failingFunc isn't provided")
	errTaskIsTooOld           = errors.New("the task is more than 20 seconds old")
)

type task struct {
	id            int
	createdAt     time.Time
	initialErr    error
	processedAt   time.Time
	processingErr error
}

type failingFunc func(task) task

func newTask(f failingFunc) (task, error) {
	if f == nil {
		return task{}, errFailingFuncNotProvided
	}

	t := task{
		id:        rand.Int(), // making unique id
		createdAt: time.Now(),
	}

	t = f(t)

	return t, nil
}

func failingFuncProvided(t task) task {
	// preserving zero value date logic
	// logic: 20 seconds succeed and the rest fail
	if t.createdAt.Nanosecond()%2 > 0 {
		t.initialErr = errSomethingWentWrong
	}

	return t
}

func failingFuncRandom(t task) task {
	if t.createdAt.UnixMilli()%2 > 0 {
		t.initialErr = errSomethingWentWrong
	}
	return t
}

func appErrorHandler(ctx context.Context, cancel context.CancelFunc) chan<- error {
	appErrCh := make(chan error)

	go func() {
		defer close(appErrCh)

		select {
		case err := <-appErrCh:
			if err != nil {
				fmt.Println("processing failed:", err)
				cancel()
				return
			}
		case <-ctx.Done():
			return
		}
	}()

	return appErrCh
}

func creator(ctx context.Context, wg *sync.WaitGroup, appErrCh chan<- error, numberOfTasks int, f failingFunc) <-chan task {
	tasksOutCh := make(chan task, numberOfTasks)

	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(tasksOutCh)

		for i := 0; i < numberOfTasks; i++ {
			t, err := newTask(f)
			if err != nil {
				appErrCh <- err
				return
			}

			select {
			case tasksOutCh <- t:
			case <-ctx.Done():
				return
			}
		}
	}()

	return tasksOutCh
}

func worker(ctx context.Context, wg *sync.WaitGroup, tasksInCh <-chan task, bufferSize int) <-chan task {
	tasksOutCh := make(chan task, bufferSize)

	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(tasksOutCh)

		for {
			select {
			case t, ok := <-tasksInCh:
				if !ok {
					return
				}

				t.processedAt = time.Now()

				if t.initialErr != nil {
					t.processingErr = fmt.Errorf("initial error: %s", t.initialErr)
				} else if !t.createdAt.After(time.Now().Add(-20 * time.Second)) {
					t.processingErr = errTaskIsTooOld
				}

				time.Sleep(time.Millisecond * 150)

				tasksOutCh <- t

			case <-ctx.Done():
				return
			}
		}

	}()
	return tasksOutCh
}

func sorter(ctx context.Context, wg *sync.WaitGroup, tasksInCh <-chan task, bufferSize int) (<-chan task, <-chan error) {
	succeededOutCh := make(chan task, bufferSize)
	errorsOutCh := make(chan error, bufferSize)

	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(succeededOutCh)
		defer close(errorsOutCh)

		for {
			select {
			case t, ok := <-tasksInCh:
				if !ok {
					return
				}

				if t.processingErr != nil {
					errorsOutCh <- fmt.Errorf("task id: %d time: %s, error: %s", t.id, t.createdAt, t.processingErr)
				} else {
					succeededOutCh <- t
				}
			case <-ctx.Done():
				return
			}
		}

	}()
	return succeededOutCh, errorsOutCh
}

func tasksCollector(ctx context.Context, wg *sync.WaitGroup, succeededInCh <-chan task) <-chan map[int]task {
	tasksCollectedOutCh := make(chan map[int]task, 1)

	tasks := make(map[int]task)
	mu := sync.Mutex{}

	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(tasksCollectedOutCh)

		innerWg := sync.WaitGroup{}

	Loop:
		for {
			select {
			case t, ok := <-succeededInCh:
				if !ok {
					break Loop
				}

				innerWg.Add(1)

				go func(t task) {
					defer innerWg.Done()
					mu.Lock()
					defer mu.Unlock()

					tasks[t.id] = t
				}(t)
			case <-ctx.Done():
				return
			}
		}

		innerWg.Wait()

		tasksCollectedOutCh <- tasks

	}()

	return tasksCollectedOutCh
}

func errorsCollector(ctx context.Context, wg *sync.WaitGroup, errorsInCh <-chan error) <-chan []error {
	errorsCollectedOutCh := make(chan []error, 1)

	var errors []error
	mu := sync.Mutex{}

	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(errorsCollectedOutCh)

		innerWg := sync.WaitGroup{}

	Loop:
		for {
			select {
			case e, ok := <-errorsInCh:
				if !ok {
					break Loop
				}

				innerWg.Add(1)

				go func(e error) {
					defer innerWg.Done()
					mu.Lock()
					defer mu.Unlock()

					errors = append(errors, e)
				}(e)
			case <-ctx.Done():
				return
			}
		}

		innerWg.Wait()

		errorsCollectedOutCh <- errors
	}()

	return errorsCollectedOutCh
}

func printer(ctx context.Context, wg *sync.WaitGroup, tasksCollectedInCh <-chan map[int]task, errorsCollectedInCh <-chan []error) {
	wg.Add(2)

	go func() {
		defer wg.Done()

		for {
			select {
			case tasks, ok := <-tasksCollectedInCh:
				if !ok {
					return
				}

				for _, t := range tasks {
					fmt.Println("succeeded:", t)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		defer wg.Done()

		for {
			select {
			case errors, ok := <-errorsCollectedInCh:
				if !ok {
					return
				}

				for _, e := range errors {
					fmt.Println("error:", e)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func main() {
	rand.Seed(time.Now().UnixNano())

	totalTasks := 10

	timeout := 10 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	wg := sync.WaitGroup{}

	appErrCh := appErrorHandler(ctx, cancel)

	creatorCh := creator(ctx, &wg, appErrCh, totalTasks, failingFuncProvided)
	workerCh := worker(ctx, &wg, creatorCh, totalTasks)
	succeededCh, failedCh := sorter(ctx, &wg, workerCh, totalTasks)
	tasksCollectedCh := tasksCollector(ctx, &wg, succeededCh)
	errorsCollectedCh := errorsCollector(ctx, &wg, failedCh)
	printer(ctx, &wg, tasksCollectedCh, errorsCollectedCh)

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		select {
		case <-ctx.Done():
			fmt.Println("context done with:", ctx.Err())
		case sig := <-sigCh:
			fmt.Println("signal received:", sig)
			cancel()
		}
	}()

	wg.Wait()
}
