package pipeline

import (
	"slices"
	"testing"
)

func TestBuffer(t *testing.T) {
	t.Parallel()

	t.Run("passes every item through in order and then closes", func(t *testing.T) {
		t.Parallel()

		want := []int{1, 2, 3, 4, 5}
		got := readAll(t, Buffer(2, Emit(want...)))
		if !slices.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("accepts items without a reader until the buffer is full", func(t *testing.T) {
		t.Parallel()

		const size = 3
		in := make(chan int)
		out := Buffer(size, in)

		// The buffer holds size items and the forwarding goroutine holds one more
		sent := runAsync(func() {
			for i := range size + 1 {
				in <- i
			}
		})
		assertUnblocked(t, sent, "sending size+1 items with no reader")

		// Once full, further sends block until something is read
		sentOneMore := runAsync(func() { in <- size + 1 })
		assertBlocked(t, sentOneMore, "sending to a full buffer")

		if i, _ := receive(t, out); i != 0 {
			t.Fatalf("first item = %d, want 0", i)
		}
		assertUnblocked(t, sentOneMore, "sending after reading one item")

		close(in)
		want := []int{1, 2, 3, 4}
		if got := readAll(t, out); !slices.Equal(got, want) {
			t.Errorf("remaining items = %v, want %v", got, want)
		}
	})

	t.Run("closes when the input is closed and empty", func(t *testing.T) {
		t.Parallel()

		in := make(chan int)
		close(in)
		if _, open := receive(t, Buffer(1, in)); open {
			t.Error("output is open, want it closed")
		}
	})
}
