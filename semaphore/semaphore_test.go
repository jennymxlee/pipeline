package semaphore

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockCheck is how long a call must stay blocked to be considered blocking.
// A slow machine can only make a blocking check pass when it should fail, never
// fail when it should pass, so a short value keeps the tests fast and reliable.
const blockCheck = 50 * time.Millisecond

// unblockTimeout is a generous upper bound for a call that must return.
const unblockTimeout = 5 * time.Second

// run calls f in a goroutine and returns a chan that is closed when f returns
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
	case <-time.After(blockCheck):
	}
}

func assertReturns(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(unblockTimeout):
		t.Fatalf("%s did not return within %s", what, unblockTimeout)
	}
}

func TestNew(t *testing.T) {
	t.Parallel()

	for _, limit := range []int{1, 2, 10} {
		s := New(limit)
		if got := cap(s); got != limit {
			t.Errorf("New(%d) cap = %d, want %d", limit, got, limit)
		}
		if got := len(s); got != 0 {
			t.Errorf("New(%d) len = %d, want 0", limit, got)
		}
	}
}

func TestAdd(t *testing.T) {
	t.Parallel()

	s := New(5)
	steps := []struct {
		delta int
		want  int
	}{
		{delta: 3, want: 3},
		{delta: 0, want: 3},
		{delta: -2, want: 1},
		{delta: 4, want: 5},
		{delta: -5, want: 0},
	}
	for _, step := range steps {
		s.Add(step.delta)
		if got := len(s); got != step.want {
			t.Fatalf("after Add(%d) len = %d, want %d", step.delta, got, step.want)
		}
	}
}

func TestDone(t *testing.T) {
	t.Parallel()

	s := New(3)
	s.Add(3)
	for want := 2; want >= 0; want-- {
		s.Done()
		if got := len(s); got != want {
			t.Fatalf("after Done len = %d, want %d", got, want)
		}
	}
}

func TestAddBlocksAtLimit(t *testing.T) {
	t.Parallel()

	s := New(2)
	s.Add(2)

	added := run(func() { s.Add(1) })
	assertBlocked(t, added, "Add above the limit")

	s.Done()
	assertReturns(t, added, "Add after Done freed a slot")
	if got := len(s); got != 2 {
		t.Errorf("len = %d, want 2", got)
	}
}

func TestAddLargerThanLimitBlocksUntilSpaceIsFreed(t *testing.T) {
	t.Parallel()

	s := New(2)
	added := run(func() { s.Add(3) })
	assertBlocked(t, added, "Add(3) on a semaphore with limit 2")

	s.Done()
	assertReturns(t, added, "Add(3) after Done freed a slot")
	if got := len(s); got != 2 {
		t.Errorf("len = %d, want 2", got)
	}
}

func TestDoneBlocksWhenEmpty(t *testing.T) {
	t.Parallel()

	s := New(1)
	done := run(s.Done)
	assertBlocked(t, done, "Done on an empty semaphore")

	s.Add(1)
	assertReturns(t, done, "Done after Add")
	if got := len(s); got != 0 {
		t.Errorf("len = %d, want 0", got)
	}
}

func TestWaitReturnsImmediatelyWhenEmpty(t *testing.T) {
	t.Parallel()

	s := New(3)
	assertReturns(t, run(s.Wait), "Wait on an empty semaphore")
}

func TestWaitBlocksUntilAllDone(t *testing.T) {
	t.Parallel()

	s := New(3)
	s.Add(2)

	waited := run(s.Wait)
	assertBlocked(t, waited, "Wait with 2 items outstanding")

	s.Done()
	assertBlocked(t, waited, "Wait with 1 item outstanding")

	s.Done()
	assertReturns(t, waited, "Wait after every item is done")
}

func TestWaitClosesSemaphore(t *testing.T) {
	t.Parallel()

	s := New(2)
	s.Wait()
	if _, open := <-s; open {
		t.Error("semaphore is open after Wait, want closed")
	}
}

func TestLimitsConcurrency(t *testing.T) {
	t.Parallel()

	const (
		limit   = 3
		workers = 50
	)

	var active, maxActive, finished atomic.Int64
	s := New(limit)
	for range workers {
		s.Add(1)
		go func() {
			defer s.Done()
			n := active.Add(1)
			for {
				m := maxActive.Load()
				if n <= m || maxActive.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			active.Add(-1)
			finished.Add(1)
		}()
	}
	s.Wait()

	if got := finished.Load(); got != workers {
		t.Errorf("finished = %d, want %d", got, workers)
	}
	if got := maxActive.Load(); got > limit {
		t.Errorf("max concurrent workers = %d, want <= %d", got, limit)
	}
}

func TestReachesLimit(t *testing.T) {
	t.Parallel()

	const limit = 3
	s := New(limit)
	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(limit)

	// Start more workers than the limit; each holds its slot until released
	var launched atomic.Int64
	launching := run(func() {
		for i := range limit + 2 {
			s.Add(1)
			launched.Add(1)
			go func() {
				defer s.Done()
				if i < limit {
					started.Done()
				}
				<-release
			}()
		}
	})

	assertReturns(t, run(started.Wait), "the first workers starting")
	assertBlocked(t, launching, "launching workers above the limit")
	if got := launched.Load(); got != limit {
		t.Errorf("launched = %d, want %d", got, limit)
	}

	close(release)
	assertReturns(t, launching, "launching after workers released their slots")
	assertReturns(t, run(s.Wait), "Wait after all workers finished")
}
