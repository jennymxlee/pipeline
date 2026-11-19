package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

func TestJoinErrors(t *testing.T) {
	t.Parallel()

	errA := errors.New("a failed")
	errB := errors.New("b failed")
	tests := []struct {
		name         string
		errA, errB   error
		wantErr      error
		wantCanceled []string
		wantBCalled  bool
	}{
		{name: "first fails", errA: errA, wantErr: errA, wantCanceled: []string{"a:1"}},
		{name: "second fails", errB: errB, wantErr: errB, wantCanceled: []string{"b:1"}, wantBCalled: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var canceled []string
			var bCalled bool
			a := NewProcessor(func(_ context.Context, i int) (string, error) {
				return strconv.Itoa(i), tc.errA
			}, func(i int, err error) {
				if err != tc.errA {
					t.Errorf("a.Cancel err = %v, want %v", err, tc.errA)
				}
				canceled = append(canceled, fmt.Sprintf("a:%d", i))
			})
			b := NewProcessor(func(_ context.Context, _ string) (int, error) {
				bCalled = true
				return 0, tc.errB
			}, func(s string, err error) {
				if err != tc.errB {
					t.Errorf("b.Cancel err = %v, want %v", err, tc.errB)
				}
				canceled = append(canceled, "b:"+s)
			})

			got, err := Join(a, b).Process(context.Background(), 1)
			if err != tc.wantErr {
				t.Errorf("err = %v, want %v", err, tc.wantErr)
			}
			if got != 0 {
				t.Errorf("output = %d, want 0", got)
			}
			if !slices.Equal(canceled, tc.wantCanceled) {
				t.Errorf("canceled = %v, want %v", canceled, tc.wantCanceled)
			}
			if bCalled != tc.wantBCalled {
				t.Errorf("b called = %t, want %t", bCalled, tc.wantBCalled)
			}
		})
	}
}
