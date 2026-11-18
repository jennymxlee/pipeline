package semaphore

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	// stillBlocked is how long a test waits to confirm that a call is blocked.
	// A broken implementation could slip past it, but a correct one never fails because of it.
	stillBlocked = 50 * time.Millisecond
	// unblockTimeout is how long a test waits for a call that should return.
	// It is generous so slow or busy machines do not cause false failures.
	unblockTimeout = 5 * time.Second
)

// run calls f in a new goroutine and returns a channel that is closed when f returns
func run(f func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	return done
}

func assertBlocked(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
		t.Fatalf("%s returned, want it to block", what)
	case <-time.After(stillBlocked):
	}
}

func assertUnblocked(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(unblockTimeout):
		t.Fatalf("%s still blocked after %s, want it to return", what, unblockTimeout)
	}
}

func TestNew(t *testing.T) {
	t.Parallel()

	for _, limit := range []int{1, 2, 10} {
		s := New(limit)
		if got := cap(s); got != limit {
			t.Errorf("cap(New(%d)) = %d, want %d", limit, got, limit)
		}
		if got := len(s); got != 0 {
			t.Errorf("len(New(%d)) = %d, want 0", limit, got)
		}
	}
}

func TestSemaphore_AddAndDone(t *testing.T) {
	t.Parallel()

	s := New(3)
	steps := []struct {
		name string
		do   func()
		want int
	}{
		{"Add(1)", func() { s.Add(1) }, 1},
		{"Add(2)", func() { s.Add(2) }, 3},
		{"Done()", s.Done, 2},
		{"Add(-2)", func() { s.Add(-2) }, 0},
		{"Add(0)", func() { s.Add(0) }, 0},
	}
	for _, step := range steps {
		step.do()
		if got := len(s); got != step.want {
			t.Fatalf("after %s: len = %d, want %d", step.name, got, step.want)
		}
	}
}

func TestSemaphore_AddBlocksAtLimit(t *testing.T) {
	t.Parallel()

	s := New(2)
	s.Add(2)

	added := run(func() { s.Add(1) })
	assertBlocked(t, added, "Add(1) on a full semaphore")

	s.Done()
	assertUnblocked(t, added, "Add(1) after Done()")
	if got := len(s); got != 2 {
		t.Errorf("len = %d, want 2", got)
	}
}

func TestSemaphore_AddMoreThanLimitBlocks(t *testing.T) {
	t.Parallel()

	s := New(2)

	added := run(func() { s.Add(3) })
	assertBlocked(t, added, "Add(3) on a semaphore with limit 2")

	s.Done()
	assertUnblocked(t, added, "Add(3) after Done()")
	if got := len(s); got != 2 {
		t.Errorf("len = %d, want 2", got)
	}
}

func TestSemaphore_DoneBlocksWhenEmpty(t *testing.T) {
	t.Parallel()

	s := New(1)

	done := run(s.Done)
	assertBlocked(t, done, "Done() on an empty semaphore")

	s.Add(1)
	assertUnblocked(t, done, "Done() after Add(1)")
	if got := len(s); got != 0 {
		t.Errorf("len = %d, want 0", got)
	}
}

func TestSemaphore_WaitOnEmptyReturns(t *testing.T) {
	t.Parallel()

	s := New(3)
	assertUnblocked(t, run(s.Wait), "Wait() on an empty semaphore")
}

func TestSemaphore_WaitBlocksUntilAllDone(t *testing.T) {
	t.Parallel()

	s := New(3)
	s.Add(3)

	waited := run(s.Wait)
	assertBlocked(t, waited, "Wait() with 3 items outstanding")

	s.Done()
	s.Done()
	assertBlocked(t, waited, "Wait() with 1 item outstanding")

	s.Done()
	assertUnblocked(t, waited, "Wait() after all items are done")
}

func TestSemaphore_WaitClosesSemaphore(t *testing.T) {
	t.Parallel()

	s := New(2)
	s.Add(1)
	s.Done()
	s.Wait()

	if _, open := <-s; open {
		t.Fatal("semaphore is still open after Wait(), want it closed")
	}
}

// TestSemaphore_LimitsConcurrency checks that no more than limit goroutines run at once,
// that the limit is actually reached, and that Wait returns only after every goroutine is done.
func TestSemaphore_LimitsConcurrency(t *testing.T) {
	t.Parallel()

	const limit = 4
	const workers = 20

	var running, maxRunning, finished atomic.Int32
	release := make(chan struct{})
	started := make(chan struct{}, workers)

	s := New(limit)
	waited := run(func() {
		for range workers {
			s.Add(1)
			go func() {
				defer s.Done()
				n := running.Add(1)
				for {
					m := maxRunning.Load()
					if n <= m || maxRunning.CompareAndSwap(m, n) {
						break
					}
				}
				started <- struct{}{}
				<-release
				running.Add(-1)
				finished.Add(1)
			}()
		}
		s.Wait()
	})

	// Exactly limit workers can start before any are released
	for i := range limit {
		select {
		case <-started:
		case <-time.After(unblockTimeout):
			t.Fatalf("only %d workers started, want %d", i, limit)
		}
	}
	select {
	case <-started:
		t.Fatalf("more than %d workers started at once", limit)
	case <-time.After(stillBlocked):
	}
	assertBlocked(t, waited, "Wait() while workers are running")

	close(release)
	assertUnblocked(t, waited, "Wait() after all workers are released")

	if got := finished.Load(); got != workers {
		t.Errorf("finished = %d, want %d", got, workers)
	}
	if got := maxRunning.Load(); got != limit {
		t.Errorf("max concurrent workers = %d, want %d", got, limit)
	}
}

func TestSemaphore_ConcurrentAddAndDone(t *testing.T) {
	t.Parallel()

	const limit = 3
	const goroutines = 50

	s := New(limit)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			s.Add(1)
			s.Done()
		})
	}
	assertUnblocked(t, run(wg.Wait), "concurrent Add(1)/Done() pairs")

	if got := len(s); got != 0 {
		t.Errorf("len = %d, want 0", got)
	}
	assertUnblocked(t, run(s.Wait), "Wait() after all Add/Done pairs")
}
