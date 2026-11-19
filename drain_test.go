package pipeline

import (
	"testing"
	"time"
)

func TestDrain(t *testing.T) {
	t.Parallel()

	in := make(chan int)
	var sent int
	go func() {
		defer close(in)
		for i := range 5 {
			in <- i
			sent++
		}
	}()

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		Drain(in)
	}()
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("Drain did not return after in was closed")
	}
	// Drain only returns once in is closed, which happens after every send
	if sent != 5 {
		t.Errorf("sent = %d, want 5", sent)
	}
}
