// Package pipeline implements a concurrent number generator and even filter.
package pipeline

import "math/rand"

// Generate sends n random numbers in [1, 100], then closes its channel.
func Generate(n int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for i := 0; i < n; i++ {
			out <- rand.Intn(100) + 1
		}
	}()
	return out
}

// Filter forwards even numbers until in is drained, then closes its output.
func Filter(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for n := range in {
			if n%2 == 0 {
				out <- n
			}
		}
	}()
	return out
}
