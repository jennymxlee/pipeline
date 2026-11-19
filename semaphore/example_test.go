package semaphore_test

import (
	"fmt"
	"sync/atomic"

	"github.com/deliveryhero/pipeline/v2/semaphore"
)

func Example() {
	// Square the numbers 1 to 10, at most 3 at a time
	var sum atomic.Int64
	sem := semaphore.New(3)
	for i := 1; i <= 10; i++ {
		sem.Add(1)
		go func() {
			defer sem.Done()
			sum.Add(int64(i * i))
		}()
	}
	// Wait for every square to be added before reading the sum
	sem.Wait()
	fmt.Println(sum.Load())
	// Output:
	// 385
}
