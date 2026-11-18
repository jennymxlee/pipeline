package pipeline

import (
	"testing"
	"time"
)

func TestDrain(t *testing.T) {
	t.Parallel()

	in := make(chan int)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		Drain(in)
	}()

	// Drain should consume every item sent to it
	for i := range 5 {
		select {
		case in <- i:
		case <-time.After(unblockTimeout):
			t.Fatalf("Drain did not read item %d", i)
		}
	}

	// Drain should keep blocking until the channel is closed
	select {
	case <-drained:
		t.Fatal("Drain returned before the channel was closed")
	case <-time.After(blockCheck):
	}

	close(in)
	select {
	case <-drained:
	case <-time.After(unblockTimeout):
		t.Fatal("Drain did not return after the channel was closed")
	}
}
