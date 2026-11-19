package semaphore

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	// blockedFor is how long a call must stay blocked to count as blocking.
	// A correct semaphore never unblocks early, so a slow machine can't make these tests fail.
	blockedFor = 50 * time.Millisecond
	// unblockWithin is the generous upper bound for a call that should return.
	unblockWithin = 5 * time.Second
)

// start runs f in a goroutine and returns a channel that is closed when f returns
func start(f func()) <-chan struct{} {
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
	case <-time.After(blockedFor):
	}
}

func assertReturns(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(unblockWithin):
		t.Fatalf("%s is still blocked after %s, want it to return", what, unblockWithin)
	}
}

func TestNew(t *testing.T) {
	t.Parallel()

	for _, limit := range []int{0, 1, 5} {
		s := New(limit)
		if cap(s) != limit {
			t.Errorf("cap(New(%d)) = %d, want %d", limit, cap(s), limit)
		}
		if len(s) != 0 {
			t.Errorf("len(New(%d)) = %d, want 0", limit, len(s))
		}
	}
}

func TestAdd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		deltas  []int
		wantLen int
	}{
		{name: "zero is a no-op", deltas: []int{0}, wantLen: 0},
		{name: "positive increments", deltas: []int{2}, wantLen: 2},
		{name: "up to the limit", deltas: []int{3}, wantLen: 3},
		{name: "many small adds", deltas: []int{1, 1, 1}, wantLen: 3},
		{name: "negative decrements", deltas: []int{3, -2}, wantLen: 1},
		{name: "back to empty", deltas: []int{2, -2}, wantLen: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := New(3)
			assertReturns(t, start(func() {
				for _, d := range tc.deltas {
					s.Add(d)
				}
			}), "Add")
			if len(s) != tc.wantLen {
				t.Errorf("len = %d, want %d", len(s), tc.wantLen)
			}
		})
	}
}

func TestAddBlocksAtLimit(t *testing.T) {
	t.Parallel()

	s := New(2)
	s.Add(2)

	added := start(func() { s.Add(1) })
	assertBlocked(t, added, "Add on a full semaphore")

	s.Done()
	assertReturns(t, added, "Add after Done")
	if len(s) != 2 {
		t.Errorf("len = %d, want 2", len(s))
	}
}

func TestAddBlocksWhenDeltaExceedsFreeSpace(t *testing.T) {
	t.Parallel()

	s := New(3)
	s.Add(2)

	added := start(func() { s.Add(2) })
	assertBlocked(t, added, "Add(2) with 1 free slot")

	s.Done()
	assertReturns(t, added, "Add(2) after Done")
	if len(s) != 3 {
		t.Errorf("len = %d, want 3", len(s))
	}
}

func TestAddNegativeBlocksWhenEmpty(t *testing.T) {
	t.Parallel()

	s := New(2)

	removed := start(func() { s.Add(-1) })
	assertBlocked(t, removed, "Add(-1) on an empty semaphore")

	s.Add(1)
	assertReturns(t, removed, "Add(-1) after Add(1)")
	if len(s) != 0 {
		t.Errorf("len = %d, want 0", len(s))
	}
}

func TestDone(t *testing.T) {
	t.Parallel()

	s := New(3)
	s.Add(3)
	s.Done()
	if len(s) != 2 {
		t.Errorf("len = %d, want 2", len(s))
	}
}

func TestWaitOnEmptySemaphore(t *testing.T) {
	t.Parallel()

	s := New(3)
	assertReturns(t, start(s.Wait), "Wait on an empty semaphore")
	if _, open := <-s; open {
		t.Error("Wait did not close the semaphore")
	}
}

func TestWaitWithZeroLimit(t *testing.T) {
	t.Parallel()

	s := New(0)
	assertReturns(t, start(s.Wait), "Wait on New(0)")
}

func TestWaitBlocksUntilAllDone(t *testing.T) {
	t.Parallel()

	s := New(3)
	s.Add(2)

	waited := start(s.Wait)
	assertBlocked(t, waited, "Wait with 2 items in progress")

	s.Done()
	assertBlocked(t, waited, "Wait with 1 item in progress")

	s.Done()
	assertReturns(t, waited, "Wait after every Done")
	if _, open := <-s; open {
		t.Error("Wait did not close the semaphore")
	}
}

func TestAddAfterWaitPanics(t *testing.T) {
	t.Parallel()

	s := New(1)
	s.Wait()

	defer func() {
		if recover() == nil {
			t.Error("Add after Wait did not panic")
		}
	}()
	s.Add(1)
}

func TestLimitsConcurrency(t *testing.T) {
	t.Parallel()

	const (
		limit   = 3
		workers = 50
	)

	s := New(limit)
	var (
		mu                  sync.Mutex
		running, maxRunning int
		finished            atomic.Int32
	)
	// release holds the first workers inside the semaphore until the limit is reached,
	// so the test always checks the limit is hit, not just that it isn't exceeded
	release := make(chan struct{})
	var once sync.Once

	for range workers {
		s.Add(1)
		go func() {
			defer s.Done()
			mu.Lock()
			running++
			maxRunning = max(maxRunning, running)
			if running == limit {
				once.Do(func() { close(release) })
			}
			mu.Unlock()

			<-release

			mu.Lock()
			running--
			mu.Unlock()
			finished.Add(1)
		}()
	}
	assertReturns(t, start(s.Wait), "Wait")

	if maxRunning != limit {
		t.Errorf("max concurrent workers = %d, want %d", maxRunning, limit)
	}
	if got := finished.Load(); got != workers {
		t.Errorf("finished workers = %d, want %d", got, workers)
	}
}
