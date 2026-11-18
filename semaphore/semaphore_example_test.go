package semaphore_test

import (
	"fmt"
	"sort"
	"sync"

	"github.com/deliveryhero/pipeline/v2/semaphore"
)

func ExampleSemaphore() {
	// Multiply up to 2 numbers at once
	sem := semaphore.New(2)

	var mu sync.Mutex
	var results []int
	for _, i := range []int{1, 2, 3, 4, 5} {
		sem.Add(1)
		go func() {
			defer sem.Done()
			mu.Lock()
			defer mu.Unlock()
			results = append(results, i*10)
		}()
	}
	// Wait for all multiplications to finish
	sem.Wait()

	sort.Ints(results)
	fmt.Println(results)
	// Output:
	// [10 20 30 40 50]
}
