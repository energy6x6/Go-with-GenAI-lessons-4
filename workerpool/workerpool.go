// Package workerpool реалізує Частину 3 домашньої роботи: пул
// воркерів з обмеженим часом на кожне завдання, що симулює
// конкурентне отримання розміру URL.
package workerpool

import (
	"context"
	"sync"
	"time"
)

// Job — одна одиниця роботи для пулу. Fetch отримує контекст із
// дедлайном і має сам його поважати (кооперативне скасування) —
// саме так уникають витоку горутин при тайм-ауті.
type Job struct {
	ID    string
	Fetch func(ctx context.Context) (int, error)
}

// Result — результат виконання одного Job.
type Result struct {
	JobID string
	Size  int
	Err   error
}

// RunPool запускає рівно numWorkers горутин-воркерів, які беруть
// завдання з jobs і надсилають Result у повернутий канал. Кожному
// Job надається не більше timeout часу — якщо job.Fetch не встигає,
// Result.Err міститиме помилку тайм-ауту (context.DeadlineExceeded).
//
// Fetch must honor ctx.Done(): cancellation is cooperative. The caller must
// close jobs and drain results. numWorkers must be positive.
func RunPool(jobs <-chan Job, numWorkers int, timeout time.Duration) <-chan Result {
	results := make(chan Result)
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for job := range jobs {
				size, err := func() (int, error) {
					ctx, cancel := context.WithTimeout(context.Background(), timeout)
					defer cancel()
					return job.Fetch(ctx)
				}()
				results <- Result{JobID: job.ID, Size: size, Err: err}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	return results
}
