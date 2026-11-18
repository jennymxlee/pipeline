package pipeline

import (
	"context"
	"errors"
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

var errTest = errors.New("test error")

// runAsync calls f in a new goroutine and returns a channel that is closed when f returns
func runAsync(f func()) <-chan struct{} {
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

// receive reads one item from in, failing the test if none arrives in time
func receive[Item any](t *testing.T, in <-chan Item) (Item, bool) {
	t.Helper()
	select {
	case i, open := <-in:
		return i, open
	case <-time.After(unblockTimeout):
		t.Fatalf("no item received after %s", unblockTimeout)
		var zero Item
		return zero, false
	}
}

// readAll reads every item from in until it is closed, failing the test if it stalls
func readAll[Item any](t *testing.T, in <-chan Item) []Item {
	t.Helper()
	var items []Item
	for {
		i, open := receive(t, in)
		if !open {
			return items
		}
		items = append(items, i)
	}
}

// recorder is a Processor that records its inputs and cancellations.
// It is only called synchronously from a single goroutine, so it needs no locking.
type recorder[In, Out any] struct {
	process  func(In) (Out, error)
	inputs   []In
	canceled []In
	errs     []error
}

func (r *recorder[In, Out]) Process(_ context.Context, i In) (Out, error) {
	r.inputs = append(r.inputs, i)
	return r.process(i)
}

func (r *recorder[In, Out]) Cancel(i In, err error) {
	r.canceled = append(r.canceled, i)
	r.errs = append(r.errs, err)
}
