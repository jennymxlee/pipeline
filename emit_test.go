package pipeline

import (
	"context"
	"slices"
	"testing"
)

func TestEmit(t *testing.T) {
	t.Parallel()

	t.Run("emits every item in order and then closes", func(t *testing.T) {
		t.Parallel()

		want := []string{"a", "b", "c"}
		if got := readAll(t, Emit(want...)); !slices.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("closes immediately with no items", func(t *testing.T) {
		t.Parallel()

		if got := readAll(t, Emit[int]()); len(got) != 0 {
			t.Errorf("got %v, want no items", got)
		}
	})
}

func TestEmitter(t *testing.T) {
	t.Parallel()

	t.Run("emits the values returned by next in order", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// next is only called from the Emitter's goroutine, so the counter needs no locking
		n := 0
		out := Emitter(ctx, func() int {
			n++
			return n
		})

		for want := 1; want <= 10; want++ {
			if got, _ := receive(t, out); got != want {
				t.Fatalf("got %d, want %d", got, want)
			}
		}
	})

	t.Run("closes after the context is canceled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		out := Emitter(ctx, func() int { return 1 })

		if _, open := receive(t, out); !open {
			t.Fatal("output closed before the context was canceled")
		}
		cancel()

		// The Emitter may already be waiting to send one more item, so keep reading until it closes
		readAll(t, out)
	})

	t.Run("emits nothing when the context is already canceled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		out := Emitter(ctx, func() int {
			t.Error("next was called after the context was canceled")
			return 0
		})

		if got := readAll(t, out); len(got) != 0 {
			t.Errorf("got %v, want no items", got)
		}
	})
}
