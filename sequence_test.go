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

func TestSequenceErrors(t *testing.T) {
	t.Parallel()

	errFail := errors.New("fail")
	inc := func(i int) int { return i + 1 }
	first := &recorder[int, int]{process: inc}
	// second fails when its input is 11, i.e. when the sequence input is 10
	second := &recorder[int, int]{process: inc, failOn: func(i int) bool { return i == 11 }, err: errFail}
	third := &recorder[int, int]{process: inc}
	s := Sequence[int](first, second, third)

	got, err := s.Process(context.Background(), 0)
	if err != nil || got != 3 {
		t.Fatalf("Process(0) = %d, %v, want 3, nil", got, err)
	}

	got, err = s.Process(context.Background(), 10)
	if !errors.Is(err, errFail) {
		t.Fatalf("err = %v, want %v", err, errFail)
	}
	if got != 0 {
		t.Errorf("got %d on error, want zero value", got)
	}
	// Only the failing processor is canceled, with the input it received
	if !slices.Equal(second.canceled, []int{11}) {
		t.Errorf("failing processor canceled %v, want [11]", second.canceled)
	}
	if len(first.canceled) != 0 || len(third.canceled) != 0 {
		t.Errorf("other processors canceled %v and %v, want none", first.canceled, third.canceled)
	}

	// Sequence's own Cancel is a no-op
	s.Cancel(10, errFail)
	if len(first.canceled)+len(second.canceled)+len(third.canceled) != 1 {
		t.Error("Sequence.Cancel canceled inner processors, want no-op")
	}
}
