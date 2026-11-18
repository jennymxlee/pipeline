package pipeline

import (
	"testing"
	"time"
)

func TestDrain(t *testing.T) {
	t.Parallel()

	const items = 10
	in := make(chan int)
	sent := make(chan int)
	go func() {
		defer close(in)
		var n int
		for i := range items {
			in <- i
			n++
		}
		sent <- n
	}()

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		Drain(in)
	}()

	select {
	case n := <-sent:
		if n != items {
			t.Fatalf("sent %d items, want %d", n, items)
		}
	case <-time.After(testTimeout):
		t.Fatal("Drain did not consume all items")
	}

	select {
	case <-drained:
	case <-time.After(testTimeout):
		t.Fatal("Drain did not return after in was closed")
	}
}

func TestDrainBlocksUntilClosed(t *testing.T) {
	t.Parallel()

	in := make(chan int)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		Drain(in)
	}()

	select {
	case <-drained:
		t.Fatal("Drain returned before in was closed")
	case <-time.After(50 * time.Millisecond):
	}

	close(in)
	select {
	case <-drained:
	case <-time.After(testTimeout):
		t.Fatal("Drain did not return after in was closed")
	}
}
