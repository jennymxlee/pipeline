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

func ExampleJoin() {
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
		name          string
		in            int
		wantErr       error
		wantCanceledA []int
		wantCanceledB []string
	}{
		{name: "first processor fails", in: 1, wantErr: errA, wantCanceledA: []int{1}},
		{name: "second processor fails", in: 2, wantErr: errB, wantCanceledB: []string{"2"}},
		{name: "no errors", in: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a := &recorder[int, string]{process: strconv.Itoa, failOn: func(i int) bool { return i == 1 }, err: errA}
			b := &recorder[string, int]{
				process: func(s string) int { n, _ := strconv.Atoi(s); return n * 10 },
				failOn:  func(s string) bool { return s == "2" },
				err:     errB,
			}
			j := Join[int, string, int](a, b)

			got, err := j.Process(context.Background(), tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil && got != 0 {
				t.Errorf("got %d on error, want zero value", got)
			}
			if tt.wantErr == nil && got != tt.in*10 {
				t.Errorf("got %d, want %d", got, tt.in*10)
			}
			if !slices.Equal(a.canceled, tt.wantCanceledA) {
				t.Errorf("first processor canceled %v, want %v", a.canceled, tt.wantCanceledA)
			}
			if !slices.Equal(b.canceled, tt.wantCanceledB) {
				t.Errorf("second processor canceled %v, want %v", b.canceled, tt.wantCanceledB)
			}
			for _, e := range append(a.errs, b.errs...) {
				if !errors.Is(e, tt.wantErr) {
					t.Errorf("Cancel got err %v, want %v", e, tt.wantErr)
				}
			}

			// Join's own Cancel is a no-op: the inner processors were already canceled
			j.Cancel(tt.in, errA)
			if len(a.canceled) != len(tt.wantCanceledA) || len(b.canceled) != len(tt.wantCanceledB) {
				t.Errorf("Join.Cancel canceled inner processors, want no-op")
			}
		})
	}
}
