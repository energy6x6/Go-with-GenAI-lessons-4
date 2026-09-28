package workerpool

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A timed-out job must observe cancellation and free the worker for the next job.
func TestRunPool_TimeoutReleasesWorker(t *testing.T) {
	jobs := make(chan Job, 2)
	stopped := make(chan struct{})
	jobs <- Job{ID: "slow", Fetch: func(ctx context.Context) (int, error) {
		<-ctx.Done()
		close(stopped)
		return 0, ctx.Err()
	}}
	jobs <- Job{ID: "next", Fetch: func(ctx context.Context) (int, error) {
		return 42, ctx.Err()
	}}
	close(jobs)

	results := RunPool(jobs, 1, 50*time.Millisecond)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	got := make(map[string]Result)
	for {
		select {
		case r, ok := <-results:
			if !ok {
				if len(got) != 2 {
					t.Fatalf("got %d results, want 2", len(got))
				}
				if !errors.Is(got["slow"].Err, context.DeadlineExceeded) {
					t.Fatalf("slow error = %v, want DeadlineExceeded", got["slow"].Err)
				}
				select {
				case <-stopped:
				default:
					t.Fatal("Fetch did not finish after cancellation")
				}
				if r := got["next"]; r.Err != nil || r.Size != 42 {
					t.Fatalf("next result = %+v, want size 42 and no error", r)
				}
				return
			}
			if _, duplicate := got[r.JobID]; duplicate {
				t.Fatalf("duplicate result for %q", r.JobID)
			}
			got[r.JobID] = r
		case <-deadline.C:
			t.Fatal("worker did not finish after job timeout")
		}
	}
}
