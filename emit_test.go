package pipeline

import (
	"context"
	"testing"
	"time"
)

func TestEmit(t *testing.T) {
	t.Parallel()

	var got []string
	for s := range Emit("a", "b", "c") {
		got = append(got, s)
	}
	if want := []string{"a", "b", "c"}; !equal(got, want) {
		t.Errorf("Emit = %v, want %v", got, want)
	}

	for s := range Emit[string]() {
		t.Errorf("Emit() emitted %q, want nothing", s)
	}
}

func TestEmitter(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var n int
	out := Emitter(ctx, func() int {
		n++
		return n
	})

	// Emitter should emit the values returned by next in order
	for want := 1; want <= 5; want++ {
		select {
		case got := <-out:
			if got != want {
				t.Fatalf("Emitter item = %d, want %d", got, want)
			}
		case <-time.After(unblockTimeout):
			t.Fatalf("Emitter did not emit item %d", want)
		}
	}

	// After the context is canceled, Emitter should stop and close its output
	cancel()
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		Drain(out)
	}()
	select {
	case <-closed:
	case <-time.After(unblockTimeout):
		t.Fatal("Emitter did not close its output after the context was canceled")
	}
}
