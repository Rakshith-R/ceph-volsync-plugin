// Package client provides worker pool utilities for parallel processing.
package workerpool

import (
	"sync"
)

// WorkerPool manages a pool of worker goroutines.
type WorkerPool struct {
	wg    sync.WaitGroup
	tasks chan func()
}

// NewWorkerPool creates a new WorkerPool with n workers.
func NewWorkerPool(n int) *WorkerPool {
	pool := &WorkerPool{
		tasks: make(chan func()),
	}
	pool.wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer pool.wg.Done()
			for task := range pool.tasks {
				task()
			}
		}()
	}
	return pool
}

// Submit adds a task to the pool.
func (p *WorkerPool) Submit(task func()) {
	p.tasks <- task
}

// Shutdown waits for all workers to finish.
func (p *WorkerPool) Shutdown() {
	close(p.tasks)
	p.wg.Wait()
}
