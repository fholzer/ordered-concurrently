package orderedconcurrently

import (
	"container/heap"
	"context"
	"sync"
)

// Options options for Process
type Options struct {
	PoolSize         int
	OutChannelBuffer int
}

// OrderedOutput is the output channel type from Process
type OrderedOutput struct {
	Value     interface{}
	Remaining func() int
}

// WorkFunction interface
// type WorkFunction func(ctx context.Context) interface{}
type ProcessFunc func(interface{}) interface{}
type processFuncGenerator func(int) (ProcessFunc, error)

// Process processes work function based on input.
// It Accepts an WorkFunction read channel, work function and concurrent go routine pool size.
// It Returns an interface{} channel.
func Process(ctx context.Context, inputChan <-chan interface{}, processFuncGenerator processFuncGenerator, options *Options) (<-chan OrderedOutput, error) {

	outputChan := make(chan OrderedOutput, options.OutChannelBuffer)

	if options.PoolSize < 1 {
		// Set a minimum number of processors
		options.PoolSize = 1
	}

	processors := make([]ProcessFunc, options.PoolSize)
	for i := 0; i < options.PoolSize; i++ {
		var err error
		processors[i], err = processFuncGenerator(i)
		if err != nil {
			return nil, err
		}
	}

	go func() {
		processChan := make(chan *processInput, options.PoolSize)
		aggregatorChan := make(chan *processInput, options.PoolSize)

		// Go routine to print data in order
		go func() {
			var current uint64
			outputHeap := &processInputHeap{}
			defer func() {
				close(outputChan)
			}()
			remaining := func() int {
				return outputHeap.Len()
			}
			for item := range aggregatorChan {
				heap.Push(outputHeap, item)
				for top, ok := outputHeap.Peek(); ok && top.order == current; {
					outputChan <- OrderedOutput{Value: heap.Pop(outputHeap).(*processInput).value, Remaining: remaining}
					current++
				}
			}

			for outputHeap.Len() > 0 {
				outputChan <- OrderedOutput{Value: heap.Pop(outputHeap).(*processInput).value, Remaining: remaining}
			}
		}()

		poolWg := sync.WaitGroup{}
		poolWg.Add(options.PoolSize)
		// Create a goroutine pool
		for i := 0; i < options.PoolSize; i++ {
			go func(process ProcessFunc) {
				defer poolWg.Done()
				for input := range processChan {
					input.value = process(input.input)
					aggregatorChan <- input
				}
			}(processors[i])
		}

		go func() {
			poolWg.Wait()
			close(aggregatorChan)
		}()

		go func() {
			defer func() {
				close(processChan)
			}()
			var order uint64
			for input := range inputChan {
				processChan <- &processInput{input: input, order: order}
				order++
			}
		}()
	}()
	return outputChan, nil
}
