package semaphore

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// notDoneWait is how long a test waits to confirm that a blocking call stays blocked.
// A short value can only hide a bug, never fail a correct implementation.
const notDoneWait = 50 * time.Millisecond

// doneWait is the upper bound for a call that should unblock; it's generous to avoid flakes on slow machines.
const doneWait = 5 * time.Second

// run calls f in a goroutine and returns a chan that closes when f returns
func run(f func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	return done
}

func requireBlocked(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
		t.Fatalf("%s returned, want it to block", what)
	case <-time.After(notDoneWait):
	}
}

func requireReturns(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(doneWait):
		t.Fatalf("%s did not return within %v", what, doneWait)
	}
}

func TestNew(t *testing.T) {
	t.Parallel()

	for _, limit := range []int{0, 1, 5, 100} {
		s := New(limit)
		if got := cap(s); got != limit {
			t.Errorf("cap(New(%d)) = %d, want %d", limit, got, limit)
		}
		if got := len(s); got != 0 {
			t.Errorf("len(New(%d)) = %d, want 0", limit, got)
		}
	}
}

func TestAdd(t *testing.T) {
	t.Parallel()

	s := New(5)
	s.Add(3)
	if got := len(s); got != 3 {
		t.Fatalf("len after Add(3) = %d, want 3", got)
	}
	s.Add(2)
	if got := len(s); got != 5 {
		t.Fatalf("len after Add(2) = %d, want 5", got)
	}
	s.Add(-4)
	if got := len(s); got != 1 {
		t.Fatalf("len after Add(-4) = %d, want 1", got)
	}
	s.Add(0)
	if got := len(s); got != 1 {
		t.Fatalf("len after Add(0) = %d, want 1", got)
	}
}

func TestDone(t *testing.T) {
	t.Parallel()

	s := New(3)
	s.Add(3)
	for want := 2; want >= 0; want-- {
		s.Done()
		if got := len(s); got != want {
			t.Fatalf("len after Done = %d, want %d", got, want)
		}
	}
}

func TestAddBlocksAtLimit(t *testing.T) {
	t.Parallel()

	s := New(2)
	s.Add(2)

	added := run(func() { s.Add(1) })
	requireBlocked(t, added, "Add over the limit")

	s.Done()
	requireReturns(t, added, "Add after Done freed a slot")
	if got := len(s); got != 2 {
		t.Fatalf("len = %d, want 2", got)
	}
}

func TestAddNegativeBlocksUntilPositive(t *testing.T) {
	t.Parallel()

	s := New(2)

	removed := run(func() { s.Add(-1) })
	requireBlocked(t, removed, "Add(-1) on an empty semaphore")

	s.Add(1)
	requireReturns(t, removed, "Add(-1) after Add(1)")
	if got := len(s); got != 0 {
		t.Fatalf("len = %d, want 0", got)
	}
}

func TestWaitEmpty(t *testing.T) {
	t.Parallel()

	s := New(3)
	requireReturns(t, run(s.Wait), "Wait on an empty semaphore")
}

func TestWaitBlocksUntilAllDone(t *testing.T) {
	t.Parallel()

	s := New(3)
	s.Add(3)

	waited := run(s.Wait)
	requireBlocked(t, waited, "Wait with 3 items outstanding")

	s.Done()
	s.Done()
	requireBlocked(t, waited, "Wait with 1 item outstanding")

	s.Done()
	requireReturns(t, waited, "Wait after all items are done")
}

func TestWaitClosesSemaphore(t *testing.T) {
	t.Parallel()

	s := New(1)
	s.Wait()

	if _, open := <-s; open {
		t.Fatal("semaphore chan is open after Wait, want closed")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Add after Wait did not panic, want a send on closed channel panic")
		}
	}()
	s.Add(1)
}

func TestConcurrencyLimit(t *testing.T) {
	t.Parallel()

	const (
		limit = 4
		tasks = 100
	)

	var (
		running   atomic.Int32
		maxActive atomic.Int32
		completed atomic.Int32
		// release lets the test hold workers open so the limit is actually reached
		release = make(chan struct{})
		started = make(chan struct{}, tasks)
	)

	s := New(limit)
	spawned := run(func() {
		for range tasks {
			s.Add(1)
			go func() {
				defer s.Done()
				n := running.Add(1)
				for {
					m := maxActive.Load()
					if n <= m || maxActive.CompareAndSwap(m, n) {
						break
					}
				}
				started <- struct{}{}
				<-release
				running.Add(-1)
				completed.Add(1)
			}()
		}
	})

	// Exactly limit workers start; the next Add is blocked until one finishes
	for range limit {
		requireReturns(t, chanOf(started), "worker start")
	}
	requireBlocked(t, spawned, "spawning more than limit workers")
	if got := running.Load(); got != limit {
		t.Fatalf("running workers = %d, want %d", got, limit)
	}

	close(release)
	requireReturns(t, spawned, "spawning all workers")
	requireReturns(t, run(s.Wait), "Wait for all workers")

	if got := completed.Load(); got != tasks {
		t.Errorf("completed = %d, want %d", got, tasks)
	}
	if got := maxActive.Load(); got > limit {
		t.Errorf("max concurrent workers = %d, want <= %d", got, limit)
	}
}

// chanOf converts a receive on c into a chan that closes once the receive happens
func chanOf(c <-chan struct{}) <-chan struct{} {
	return run(func() { <-c })
}

func TestConcurrentAddDone(t *testing.T) {
	t.Parallel()

	const (
		limit   = 3
		workers = 50
	)

	s := New(limit)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			s.Add(1)
			if n := len(s); n > limit {
				t.Errorf("len = %d, want <= %d", n, limit)
			}
			s.Done()
		})
	}
	requireReturns(t, run(wg.Wait), "all workers")
	if got := len(s); got != 0 {
		t.Fatalf("len = %d, want 0", got)
	}
	requireReturns(t, run(s.Wait), "Wait")
}
