package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
)

func TestJoin(t *testing.T) {
	t.Parallel()

	// Emit 10 numbers
	want := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	ins := Emit(want...)
	// Join two steps, one that converts the number to a string, the other that converts it back to a number
	join := Join(NewProcessor(func(_ context.Context, i int) (string, error) {
		return strconv.Itoa(i), nil
	}, nil), NewProcessor(func(_ context.Context, i string) (int, error) {
		return strconv.Atoi(i)
	}, nil))
	// Compare inputs and outputs
	var idx int
	for got := range Process(context.Background(), join, ins) {
		if want[idx] != got {
			t.Fatalf("[%d] = %d, want = %d", idx, got, want[idx])
		}
		idx++
	}
}

func JoinExample() {
	// Emit 10 numbers
	inputs := Emit(0, 1, 2, 3, 4, 5)
	// Join two steps, one that converts the number to a string, the other that converts it back to a number
	convertToStringThenBackToInt := Process(context.Background(), Join(NewProcessor(func(_ context.Context, i int) (string, error) {
		return strconv.Itoa(i), nil
	}, nil), NewProcessor(func(_ context.Context, i string) (int, error) {
		return strconv.Atoi(i)
	}, nil)), inputs)
	// Print the output
	for o := range convertToStringThenBackToInt {
		fmt.Println(o)
	}
	// Output:
	// 0
	// 1
	// 2
	// 3
	// 4
	// 5
}

func TestJoinFirstProcessorError(t *testing.T) {
	t.Parallel()

	errA := errors.New("a failed")
	var aCanceled canceledItems[int]
	a := NewProcessor(func(_ context.Context, _ int) (string, error) {
		return "", errA
	}, aCanceled.cancel)

	var bCalled bool
	b := NewProcessor(func(_ context.Context, s string) (int, error) {
		bCalled = true
		return strconv.Atoi(s)
	}, func(string, error) { t.Error("b.Cancel called, want it not to be") })

	got, err := Join(a, b).Process(context.Background(), 7)
	if !errors.Is(err, errA) {
		t.Errorf("err = %v, want %v", err, errA)
	}
	if got != 0 {
		t.Errorf("output = %d, want 0", got)
	}
	if bCalled {
		t.Error("second processor was called after the first one failed")
	}
	if !equal(aCanceled.items, []int{7}) || !errors.Is(aCanceled.errs[0], errA) {
		t.Errorf("first processor canceled %v with %v, want [7] with %v", aCanceled.items, aCanceled.errs, errA)
	}
}

func TestJoinSecondProcessorError(t *testing.T) {
	t.Parallel()

	a := NewProcessor(func(_ context.Context, i int) (string, error) {
		return "not a number " + strconv.Itoa(i), nil
	}, func(int, error) { t.Error("a.Cancel called, want it not to be") })

	var bCanceled canceledItems[string]
	b := NewProcessor(func(_ context.Context, s string) (int, error) {
		return strconv.Atoi(s)
	}, bCanceled.cancel)

	got, err := Join(a, b).Process(context.Background(), 7)
	if err == nil {
		t.Fatal("err = nil, want an error")
	}
	if got != 0 {
		t.Errorf("output = %d, want 0", got)
	}
	if !equal(bCanceled.items, []string{"not a number 7"}) || bCanceled.errs[0] != err {
		t.Errorf("second processor canceled %v with %v, want [not a number 7] with %v", bCanceled.items, bCanceled.errs, err)
	}
}

func TestJoinCancelDoesNotCancelInnerProcessors(t *testing.T) {
	t.Parallel()

	a := NewProcessor(func(_ context.Context, i int) (string, error) {
		return strconv.Itoa(i), nil
	}, func(int, error) { t.Error("a.Cancel called, want it not to be") })
	b := NewProcessor(func(_ context.Context, s string) (int, error) {
		return strconv.Atoi(s)
	}, func(string, error) { t.Error("b.Cancel called, want it not to be") })

	Join(a, b).Cancel(1, errors.New("canceled"))
}
