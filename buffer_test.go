package pipeline

import (
	"slices"
	"testing"
	"time"
)

func TestBuffer(t *testing.T) {
	t.Parallel()

	in := make(chan int)
	out := Buffer(3, in)
	if cap(out) != 3 {
		t.Fatalf("cap(Buffer(3, in)) = %d, want 3", cap(out))
	}

	// Nothing reads from out yet, so these sends only finish if Buffer is buffering them
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for i := range 3 {
			in <- i
		}
		close(in)
	}()
	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("sends to in blocked, want Buffer to absorb them")
	}

	var got []int
	for i := range out {
		got = append(got, i)
	}
	if want := []int{0, 1, 2}; !slices.Equal(got, want) {
		t.Errorf("Buffer output = %v, want %v", got, want)
	}
}
