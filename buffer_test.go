package pipeline

import (
	"testing"
	"time"
)

// testTimeout bounds how long a test waits for something that should happen;
// it's generous so slow CI machines don't cause flakes.
const testTimeout = 5 * time.Second

func TestBuffer(t *testing.T) {
	t.Parallel()

	const size = 3
	in := make(chan int)
	out := Buffer(size, in)

	// Without anyone reading out, the buffer accepts at least size items
	for i := range size {
		select {
		case in <- i:
		case <-time.After(testTimeout):
			t.Fatalf("send %d blocked, want Buffer(%d) to accept it", i, size)
		}
	}
	close(in)

	var got []int
	timeout := time.After(testTimeout)
	for {
		select {
		case i, open := <-out:
			if !open {
				if len(got) != size {
					t.Fatalf("got %v, want %d items", got, size)
				}
				for idx := range got {
					if got[idx] != idx {
						t.Fatalf("got %v, want items in order [0 1 2]", got)
					}
				}
				return
			}
			got = append(got, i)
		case <-timeout:
			t.Fatalf("out was not closed after in was closed, got %v", got)
		}
	}
}
