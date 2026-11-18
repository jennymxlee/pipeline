package pipeline

import (
	"context"
	"errors"
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

// canceledItems records the items passed to a Processor's Cancel func
type canceledItems[Item any] struct {
	items []Item
	errs  []error
}

func (c *canceledItems[Item]) cancel(i Item, err error) {
	c.items = append(c.items, i)
	c.errs = append(c.errs, err)
}

func TestApplyFirstProcessorError(t *testing.T) {
	t.Parallel()

	errSplit := errors.New("split failed")
	var splitCanceled canceledItems[string]
	split := NewProcessor(func(_ context.Context, _ string) ([]string, error) {
		return nil, errSplit
	}, splitCanceled.cancel)

	var doubleCalled bool
	double := NewProcessor(func(_ context.Context, s string) (string, error) {
		doubleCalled = true
		return s + s, nil
	}, func(string, error) { t.Error("double.Cancel called, want it not to be") })

	got, err := Apply(split, double).Process(context.Background(), "1,2")
	if !errors.Is(err, errSplit) {
		t.Errorf("err = %v, want %v", err, errSplit)
	}
	if len(got) != 0 {
		t.Errorf("output = %v, want empty", got)
	}
	if doubleCalled {
		t.Error("second processor was called after the first one failed")
	}
	if !equal(splitCanceled.items, []string{"1,2"}) || !errors.Is(splitCanceled.errs[0], errSplit) {
		t.Errorf("first processor canceled %v with %v, want [1,2] with %v", splitCanceled.items, splitCanceled.errs, errSplit)
	}
}

func TestApplySecondProcessorError(t *testing.T) {
	t.Parallel()

	split := NewProcessor(func(_ context.Context, s string) ([]string, error) {
		return strings.Split(s, ","), nil
	}, func(string, error) { t.Error("split.Cancel called, want it not to be") })

	errBad := errors.New("bad item")
	var doubleCanceled canceledItems[string]
	var doubled []string
	double := NewProcessor(func(_ context.Context, s string) (string, error) {
		doubled = append(doubled, s)
		if s == "bad" {
			return "", errBad
		}
		return s + s, nil
	}, doubleCanceled.cancel)

	got, err := Apply(split, double).Process(context.Background(), "1,bad,3")
	if !errors.Is(err, errBad) {
		t.Errorf("err = %v, want %v", err, errBad)
	}
	// Items processed before the failure are returned; items after it are not processed
	if want := []string{"11"}; !equal(got, want) {
		t.Errorf("output = %v, want %v", got, want)
	}
	if want := []string{"1", "bad"}; !equal(doubled, want) {
		t.Errorf("second processor processed %v, want %v", doubled, want)
	}
	if !equal(doubleCanceled.items, []string{"bad"}) || !errors.Is(doubleCanceled.errs[0], errBad) {
		t.Errorf("second processor canceled %v with %v, want [bad] with %v", doubleCanceled.items, doubleCanceled.errs, errBad)
	}
}

func TestApplyCancelDoesNotCancelInnerProcessors(t *testing.T) {
	t.Parallel()

	split := NewProcessor(func(_ context.Context, s string) ([]string, error) {
		return []string{s}, nil
	}, func(string, error) { t.Error("split.Cancel called, want it not to be") })
	double := NewProcessor(func(_ context.Context, s string) (string, error) {
		return s + s, nil
	}, func(string, error) { t.Error("double.Cancel called, want it not to be") })

	Apply(split, double).Cancel("1", errors.New("canceled"))
}
