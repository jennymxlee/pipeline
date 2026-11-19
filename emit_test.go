package pipeline

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestEmit(t *testing.T) {
	t.Parallel()

	var got []int
	for i := range Emit(1, 2, 3) {
		got = append(got, i)
	}
	if want := []int{1, 2, 3}; !slices.Equal(got, want) {
		t.Errorf("Emit output = %v, want %v", got, want)
	}

	if _, open := <-Emit[int](); open {
		t.Error("Emit() with no items did not close its output")
	}
}

func TestEmitter(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// next is only called from the Emitter goroutine, so it doesn't need a lock
	var n int
	out := Emitter(ctx, func() int {
		n++
		return n
	})

	for want := 1; want <= 5; want++ {
		if got := <-out; got != want {
			t.Fatalf("item = %d, want %d", got, want)
		}
	}

	cancel()
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		// At most one more item may already be in flight when the context is canceled
		Drain(out)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Emitter did not close its output after the context was canceled")
	}
}
