package pipeline

import (
	"context"
	"testing"
	"time"
)

func TestEmit(t *testing.T) {
	t.Parallel()

	want := []string{"a", "b", "c"}
	var got []string
	for s := range Emit(want...) {
		got = append(got, s)
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}

	// No items closes immediately
	for s := range Emit[string]() {
		t.Fatalf("Emit() emitted %q, want nothing", s)
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

	// Items come from next in the order it generates them
	const reads = 5
	for want := 1; want <= reads; want++ {
		select {
		case got := <-out:
			if got != want {
				t.Fatalf("got %d, want %d", got, want)
			}
		case <-time.After(testTimeout):
			t.Fatalf("Emitter did not emit item %d", want)
		}
	}

	// After cancel, at most one more item (already generated) is sent, then out closes
	cancel()
	timeout := time.After(testTimeout)
	var extra int
	for {
		select {
		case _, open := <-out:
			if !open {
				if extra > 1 {
					t.Fatalf("got %d items after cancel, want at most 1", extra)
				}
				return
			}
			extra++
		case <-timeout:
			t.Fatal("Emitter did not close out after the context was canceled")
		}
	}
}

func TestEmitterCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out := Emitter(ctx, func() int {
		t.Error("next called with an already canceled context")
		return 0
	})
	select {
	case _, open := <-out:
		if open {
			t.Fatal("Emitter emitted an item, want out closed")
		}
	case <-time.After(testTimeout):
		t.Fatal("Emitter did not close out")
	}
}
