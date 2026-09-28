// Package deadlock demonstrates a channel handoff without circular waiting.
package deadlock

// Run receives 42 from a goroutine. The channel itself synchronizes the handoff.
func Run() int {
	ch := make(chan int)
	go func() {
		ch <- 42
	}()
	return <-ch
}
