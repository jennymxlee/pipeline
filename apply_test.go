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

func TestApplyFirstProcessorError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("split failed")
	var canceled []string
	split := NewProcessor(func(_ context.Context, _ string) ([]string, error) {
		return nil, wantErr
	}, func(s string, err error) {
		if err != wantErr {
			t.Errorf("split.Cancel err = %v, want %v", err, wantErr)
		}
		canceled = append(canceled, s)
	})
	var upperCalled bool
	upper := NewProcessor(func(_ context.Context, s string) (string, error) {
		upperCalled = true
		return strings.ToUpper(s), nil
	}, nil)

	got, err := Apply(split, upper).Process(context.Background(), "a,b")
	if err != wantErr {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("output = %#v, want an empty slice", got)
	}
	if want := []string{"a,b"}; !slices.Equal(canceled, want) {
		t.Errorf("canceled = %v, want %v", canceled, want)
	}
	if upperCalled {
		t.Error("second processor was called after the first failed")
	}
}

func TestApplySecondProcessorError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("upper failed")
	split := NewProcessor(func(_ context.Context, s string) ([]string, error) {
		return strings.Split(s, ","), nil
	}, nil)
	var processed, canceled []string
	upper := NewProcessor(func(_ context.Context, s string) (string, error) {
		processed = append(processed, s)
		if s == "b" {
			return "", wantErr
		}
		return strings.ToUpper(s), nil
	}, func(s string, err error) {
		if err != wantErr {
			t.Errorf("upper.Cancel err = %v, want %v", err, wantErr)
		}
		canceled = append(canceled, s)
	})

	got, err := Apply(split, upper).Process(context.Background(), "a,b,c")
	if err != wantErr {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
	// Items processed before the failure are still returned
	if want := []string{"A"}; !slices.Equal(got, want) {
		t.Errorf("output = %v, want %v", got, want)
	}
	if want := []string{"b"}; !slices.Equal(canceled, want) {
		t.Errorf("canceled = %v, want %v", canceled, want)
	}
	// Processing stops at the first failure, so "c" is never processed
	if want := []string{"a", "b"}; !slices.Equal(processed, want) {
		t.Errorf("processed = %v, want %v", processed, want)
	}
}
