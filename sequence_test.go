package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
)

func TestSequence(t *testing.T) {
	t.Parallel()

	// Create a step that increments the integer by 1
	inc := NewProcessor(func(_ context.Context, i int) (int, error) {
		i++
		return i, nil
	}, nil)
	// Add 5 to every number
	inc5 := Sequence(inc, inc, inc, inc, inc)
	// Verify the sequence ads 5 to each number
	var i int
	want := []int{5, 6, 7, 8, 9}
	for o := range Process(context.Background(), inc5, Emit(0, 1, 2, 3, 4)) {
		if want[i] != o {
			t.Fatalf("[%d] = %d, want %d", i, o, want[i])
		}
		i++
	}
}

func ExampleSequence() {
	// Create a step that increments the integer by 1
	inc := NewProcessor(func(_ context.Context, i int) (int, error) {
		i++
		return i, nil
	}, nil)
	// Add 5 to every input
	inputs := Emit(0, 1, 2, 3, 4)
	addFive := Process(context.Background(), Sequence(inc, inc, inc, inc, inc), inputs)
	// Print the output
	for o := range addFive {
		fmt.Println(o)
	}
	// Output:
	// 5
	// 6
	// 7
	// 8
	// 9
}

func TestSequence_errors(t *testing.T) {
	t.Parallel()

	t.Run("stops and cancels the input of the processor that fails", func(t *testing.T) {
		t.Parallel()

		inc := func(i int) (int, error) { return i + 1, nil }
		first := &recorder[int, int]{process: inc}
		failing := &recorder[int, int]{process: func(int) (int, error) { return 99, errTest }}
		last := &recorder[int, int]{process: inc}

		got, err := Sequence[int](first, failing, last).Process(context.Background(), 1)
		if !errors.Is(err, errTest) {
			t.Fatalf("err = %v, want %v", err, errTest)
		}
		if got != 0 {
			t.Errorf("output = %d, want 0", got)
		}
		if len(first.canceled) != 0 {
			t.Errorf("first processor canceled %v, want nothing canceled", first.canceled)
		}
		// The failing processor received the first processor's output
		if !slices.Equal(failing.canceled, []int{2}) || !errors.Is(failing.errs[0], errTest) {
			t.Errorf("failing processor canceled %v with %v, want [2] with %v", failing.canceled, failing.errs, errTest)
		}
		if len(last.inputs) != 0 {
			t.Errorf("last processor was called with %v, want it not called", last.inputs)
		}
	})

	t.Run("returns the input unchanged with no processors", func(t *testing.T) {
		t.Parallel()

		got, err := Sequence[int]().Process(context.Background(), 5)
		if err != nil || got != 5 {
			t.Errorf("got %d, %v, want 5, nil", got, err)
		}
	})

	t.Run("Cancel does not call the sequenced processors", func(t *testing.T) {
		t.Parallel()

		p := &recorder[int, int]{}
		Sequence[int](p, p).Cancel(1, errTest)
		if len(p.canceled) != 0 {
			t.Errorf("canceled %v, want nothing canceled", p.canceled)
		}
	})
}
