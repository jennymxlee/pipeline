package pipeline

import "testing"

func TestDrain(t *testing.T) {
	t.Parallel()

	t.Run("reads every item", func(t *testing.T) {
		t.Parallel()

		in := make(chan int)
		sent := 0
		go func() {
			defer close(in)
			for i := range 5 {
				in <- i
				sent++
			}
		}()

		assertUnblocked(t, runAsync(func() { Drain(in) }), "Drain")
		// Drain returning means in was closed, which happens after the last send
		if sent != 5 {
			t.Errorf("sent = %d, want 5", sent)
		}
	})

	t.Run("blocks until the input is closed", func(t *testing.T) {
		t.Parallel()

		in := make(chan int)
		drained := runAsync(func() { Drain(in) })

		in <- 1
		in <- 2
		assertBlocked(t, drained, "Drain on an open channel")

		close(in)
		assertUnblocked(t, drained, "Drain after the input is closed")
	})
}
