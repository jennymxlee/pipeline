package pipeline

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestLoopApply(t *testing.T) {
	t.Parallel()

	transform := NewProcessor(func(_ context.Context, s string) ([]string, error) {
		return strings.Split(s, ","), nil
	}, nil)

	double := NewProcessor(func(_ context.Context, s string) (string, error) {
		return s + s, nil
	}, nil)

	addLeadingZero := NewProcessor(func(_ context.Context, s string) (string, error) {
		return "0" + s, nil
	}, nil)

	looper := Apply(
		transform,
		Sequence(
			double,
			addLeadingZero,
			double,
		),
	)

	gotCount := 0
	input := "1,2,3,4,5"
	want := []string{"011011", "022022", "033033", "044044", "055055"}

	for out := range Process(context.Background(), looper, Emit(input)) {
		for j := range out {
			gotCount++
			if !contains(want, out[j]) {
				t.Errorf("does not contains got=%v, want=%v", out[j], want)
			}
		}
	}

	if gotCount != len(want) {
		t.Errorf("total results got=%v, want=%v", gotCount, len(want))
	}
}

func contains(s []string, e string) bool {
	for i := range s {
		if s[i] == e {
			return true
		}
	}

	return false
}

func TestApplyErrors(t *testing.T) {
	t.Parallel()

	errSplit := errors.New("split failed")
	errUpper := errors.New("upper failed")

	tests := []struct {
		name              string
		in                string
		want              []string
		wantErr           error
		wantCanceledSplit []string
		wantCanceledUpper []string
	}{
		{name: "first processor fails", in: "fail", want: []string{}, wantErr: errSplit, wantCanceledSplit: []string{"fail"}},
		// Items processed before the failure are returned
		{name: "second processor fails", in: "a,x,b", want: []string{"A"}, wantErr: errUpper, wantCanceledUpper: []string{"x"}},
		{name: "no errors", in: "a,b", want: []string{"A", "B"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			split := &recorder[string, []string]{
				process: func(s string) []string { return strings.Split(s, ",") },
				failOn:  func(s string) bool { return s == "fail" },
				err:     errSplit,
			}
			upper := &recorder[string, string]{
				process: strings.ToUpper,
				failOn:  func(s string) bool { return s == "x" },
				err:     errUpper,
			}
			a := Apply[string, string, string](split, upper)

			got, err := a.Process(context.Background(), tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
			if !slices.Equal(split.canceled, tt.wantCanceledSplit) {
				t.Errorf("first processor canceled %v, want %v", split.canceled, tt.wantCanceledSplit)
			}
			if !slices.Equal(upper.canceled, tt.wantCanceledUpper) {
				t.Errorf("second processor canceled %v, want %v", upper.canceled, tt.wantCanceledUpper)
			}

			// Apply's own Cancel is a no-op
			a.Cancel(tt.in, errSplit)
			if len(split.canceled) != len(tt.wantCanceledSplit) || len(upper.canceled) != len(tt.wantCanceledUpper) {
				t.Error("Apply.Cancel canceled inner processors, want no-op")
			}
		})
	}
}
