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

func TestApply_errors(t *testing.T) {
	t.Parallel()

	split := func(s string) ([]string, error) { return strings.Split(s, ","), nil }

	t.Run("cancels the input of the first processor when it fails", func(t *testing.T) {
		t.Parallel()

		a := &recorder[string, []string]{process: func(string) ([]string, error) { return []string{"x"}, errTest }}
		b := &recorder[string, string]{process: func(s string) (string, error) { return s, nil }}

		got, err := Apply[string, string, string](a, b).Process(context.Background(), "1,2")
		if !errors.Is(err, errTest) {
			t.Fatalf("err = %v, want %v", err, errTest)
		}
		if len(got) != 0 {
			t.Errorf("output = %v, want empty", got)
		}
		if !slices.Equal(a.canceled, []string{"1,2"}) || !errors.Is(a.errs[0], errTest) {
			t.Errorf("first processor canceled %v with %v, want [1,2] with %v", a.canceled, a.errs, errTest)
		}
		if len(b.inputs) != 0 {
			t.Errorf("second processor was called with %v, want it not called", b.inputs)
		}
	})

	t.Run("returns the outputs so far and cancels the item that fails", func(t *testing.T) {
		t.Parallel()

		a := &recorder[string, []string]{process: split}
		b := &recorder[string, string]{process: func(s string) (string, error) {
			if s == "2" {
				return "", errTest
			}
			return s + s, nil
		}}

		got, err := Apply[string, string, string](a, b).Process(context.Background(), "1,2,3")
		if !errors.Is(err, errTest) {
			t.Fatalf("err = %v, want %v", err, errTest)
		}
		if !slices.Equal(got, []string{"11"}) {
			t.Errorf("output = %v, want [11]", got)
		}
		if len(a.canceled) != 0 {
			t.Errorf("first processor canceled %v, want nothing canceled", a.canceled)
		}
		if !slices.Equal(b.canceled, []string{"2"}) || !errors.Is(b.errs[0], errTest) {
			t.Errorf("second processor canceled %v with %v, want [2] with %v", b.canceled, b.errs, errTest)
		}
		// Items after the failing one are not processed
		if !slices.Equal(b.inputs, []string{"1", "2"}) {
			t.Errorf("second processor was called with %v, want [1 2]", b.inputs)
		}
	})

	t.Run("Cancel does not call the applied processors", func(t *testing.T) {
		t.Parallel()

		a := &recorder[string, []string]{}
		b := &recorder[string, string]{}
		Apply[string, string, string](a, b).Cancel("1,2", errTest)
		if len(a.canceled) != 0 || len(b.canceled) != 0 {
			t.Errorf("canceled %v and %v, want nothing canceled", a.canceled, b.canceled)
		}
	})
}
