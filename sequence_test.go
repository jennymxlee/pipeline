package pipeline

import (
	"context"
	"errors"
	"fmt"
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

func SequenceExample() {
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

func TestSequenceError(t *testing.T) {
	t.Parallel()

	errTooBig := errors.New("too big")
	var processed []int
	var canceled canceledItems[int]
	// incUpTo2 increments the integer unless it is already 2 or more
	incUpTo2 := NewProcessor(func(_ context.Context, i int) (int, error) {
		processed = append(processed, i)
		if i >= 2 {
			return 0, errTooBig
		}
		return i + 1, nil
	}, canceled.cancel)

	got, err := Sequence(incUpTo2, incUpTo2, incUpTo2, incUpTo2).Process(context.Background(), 0)
	if !errors.Is(err, errTooBig) {
		t.Errorf("err = %v, want %v", err, errTooBig)
	}
	if got != 0 {
		t.Errorf("output = %d, want 0", got)
	}
	// The processor after the failing one should not run
	if want := []int{0, 1, 2}; !equal(processed, want) {
		t.Errorf("processed = %v, want %v", processed, want)
	}
	// Only the failing processor is canceled, with the input it failed on
	if !equal(canceled.items, []int{2}) || !errors.Is(canceled.errs[0], errTooBig) {
		t.Errorf("canceled %v with %v, want [2] with %v", canceled.items, canceled.errs, errTooBig)
	}
}

func TestSequenceEmpty(t *testing.T) {
	t.Parallel()

	got, err := Sequence[int]().Process(context.Background(), 42)
	if err != nil || got != 42 {
		t.Errorf("empty Sequence = (%d, %v), want (42, nil)", got, err)
	}
}

func TestSequenceCancelDoesNotCancelInnerProcessors(t *testing.T) {
	t.Parallel()

	inc := NewProcessor(func(_ context.Context, i int) (int, error) {
		return i + 1, nil
	}, func(int, error) { t.Error("inc.Cancel called, want it not to be") })

	Sequence(inc, inc).Cancel(1, errors.New("canceled"))
}
