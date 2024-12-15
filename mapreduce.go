package mapreduce

import (
	"sync"
)

type (
	GenerateFunc[T any] func(source chan<- T)
	MapFunc[T, U any]   func(item T, write chan<- U)
	ReduceFunc[V any]   func(reduceCh <-chan V)
)

const WorkCnt = 10

func MapReduce[T, U any](generate GenerateFunc[T], mapper MapFunc[T, U], reduce ReduceFunc[U]) {

	// generate
	source := make(chan T, WorkCnt)
	go func() {
		// stop
		defer close(source)
		generate(source)
	}()

	// map
	worker := make(chan struct{}, WorkCnt)
	var workerWaitGroup sync.WaitGroup
	reduceChan := make(chan U, WorkCnt)
	go func() {
		defer func() {
			workerWaitGroup.Wait()
			close(worker)
			close(reduceChan)

		}()

		for {
			select {
			// exist free worker
			case worker <- struct{}{}:
				// check source
				item, ok := <-source
				// source closed
				if !ok {
					<-worker
					return
				}
				workerWaitGroup.Add(1)
				go func() {
					defer func() {
						workerWaitGroup.Done()
						<-worker
					}()
					mapper(item, reduceChan)
				}()
			}
		}
	}()
	reduce(reduceChan)
}
