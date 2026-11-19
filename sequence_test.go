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

	wantErr := errors.New("step 2 failed")
	var calls []string
	var canceled []int
	step := func(name string, err error) Processor[int, int] {
		return NewProcessor(func(_ context.Context, i int) (int, error) {
			calls = append(calls, name)
			return i + 1, err
		}, func(i int, err error) {
			if err != wantErr {
				t.Errorf("%s.Cancel err = %v, want %v", name, err, wantErr)
			}
			canceled = append(canceled, i)
		})
	}

	got, err := Sequence(step("1", nil), step("2", wantErr), step("3", nil)).Process(context.Background(), 10)
	if err != wantErr {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
	if got != 0 {
		t.Errorf("output = %d, want 0", got)
	}
	// Step 2 is canceled with its own input: the output of step 1
	if want := []int{11}; !slices.Equal(canceled, want) {
		t.Errorf("canceled = %v, want %v", canceled, want)
	}
	if want := []string{"1", "2"}; !slices.Equal(calls, want) {
		t.Errorf("calls = %v, want %v", calls, want)
	}
}
