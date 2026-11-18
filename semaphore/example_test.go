package semaphore_test

import (
	"fmt"
	"slices"

	"github.com/deliveryhero/pipeline/v2/semaphore"
)

// startMultiplying is a pipeline step that concurrently multiplies input numbers by a factor
func startMultiplying(concurrency, factor int, in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		sem := semaphore.New(concurrency)
		for i := range in {
			// Multiply up to 'concurrency' inputs at once
			sem.Add(1)
			go func() {
				out <- factor * i
				sem.Done()
			}()
		}
		// Wait for all multiplications to finish before closing the output chan
		sem.Wait()
		close(out)
	}()
	return out
}

func Example() {
	in := make(chan int)
	go func() {
		defer close(in)
		for i := 1; i <= 5; i++ {
			in <- i
		}
	}()

	var results []int
	for r := range startMultiplying(2, 10, in) {
		results = append(results, r)
	}
	// Results arrive in any order, so sort them before printing
	slices.Sort(results)
	fmt.Println(results)
	// Output:
	// [10 20 30 40 50]
}
