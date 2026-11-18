package pipeline

import (
	"testing"
	"time"
)

// blockCheck is how long a call must stay blocked to be considered blocking.
// A slow machine can only make a blocking check pass when it should fail, never
// fail when it should pass, so a short value keeps the tests fast and reliable.
const blockCheck = 50 * time.Millisecond

// unblockTimeout is a generous upper bound for an operation that must finish.
const unblockTimeout = 5 * time.Second

func TestBuffer(t *testing.T) {
	t.Parallel()

	const size = 3
	in := make(chan int)
	out := Buffer(size, in)

	// The buffer should accept items without anyone reading from it
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for i := range size {
			in <- i
		}
		close(in)
	}()
	select {
	case <-sent:
	case <-time.After(unblockTimeout):
		t.Fatalf("sending %d items into a buffer of size %d blocked", size, size)
	}

	// Every item should come out in order, then the buffer should close
	var got []int
	for i := range out {
		got = append(got, i)
	}
	want := []int{0, 1, 2}
	if !equal(got, want) {
		t.Errorf("Buffer output = %v, want %v", got, want)
	}
}

func TestBufferBlocksWhenFull(t *testing.T) {
	t.Parallel()

	const size = 2
	in := make(chan int)
	out := Buffer(size, in)

	// size items fill the buffer and one more is held by the buffer's goroutine
	for i := range size + 1 {
		in <- i
	}
	select {
	case in <- size + 1:
		t.Fatal("sent into a full buffer, want it to block")
	case <-time.After(blockCheck):
	}

	// Reading one item frees space for one more
	if got := <-out; got != 0 {
		t.Fatalf("first item = %d, want 0", got)
	}
	select {
	case in <- size + 1:
	case <-time.After(unblockTimeout):
		t.Fatal("send blocked after reading from the buffer")
	}
	close(in)

	var got []int
	for i := range out {
		got = append(got, i)
	}
	if want := []int{1, 2, 3}; !equal(got, want) {
		t.Errorf("remaining items = %v, want %v", got, want)
	}
}

func equal[Item comparable](a, b []Item) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
