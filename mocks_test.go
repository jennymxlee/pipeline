package pipeline

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

// mockProcess is a mock of the Processor interface
type mockProcessor[Item any] struct {
	processDuration    time.Duration
	cancelDuration     time.Duration
	processReturnsErrs bool
	processed          []Item
	canceled           []Item
	errs               []string
	// mu guards processed, canceled and errs, which concurrent processors append to
	mu sync.Mutex
}

// Process waits processDuration before returning its input as its output
func (m *mockProcessor[Item]) Process(ctx context.Context, i Item) (Item, error) {
	var zero Item
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-time.After(m.processDuration):
		break
	}
	if m.processReturnsErrs {
		return zero, fmt.Errorf("process error: %v", i)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processed = append(m.processed, i)
	return i, nil
}

// Cancel collects all inputs that were canceled in m.canceled
func (m *mockProcessor[Item]) Cancel(i Item, err error) {
	time.Sleep(m.cancelDuration)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.canceled = append(m.canceled, i)
	m.errs = append(m.errs, err.Error())
}

// Processed returns a copy of the processed inputs
func (m *mockProcessor[Item]) Processed() []Item {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.processed)
}

// Canceled returns a copy of the canceled inputs
func (m *mockProcessor[Item]) Canceled() []Item {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.canceled)
}

// Errs returns a copy of the errors passed to Cancel
func (m *mockProcessor[Item]) Errs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.errs)
}

// containsAll returns true if a and b contain all of the same elements
// in any order or if both are empty / nil
func containsAll[Item comparable](a, b []Item) bool {
	if len(a) != len(b) {
		return false
	} else if len(a) == 0 {
		return true
	}
	aMap := make(map[Item]bool)
	for _, i := range a {
		aMap[i] = true
	}
	for _, i := range b {
		if !aMap[i] {
			return false
		}
	}
	return true
}
