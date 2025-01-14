package orderedconcurrently

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

type zeroLoadWorker int

func zeroLoadWorkerRun(w interface{}) interface{} {
	return w.(zeroLoadWorker) * 2
}

type loadWorker int

func loadWorkerRun(w interface{}) interface{} {
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
	return w.(loadWorker) * 2
}

func processGeneratorGenerator(wf ProcessFunc) processFuncGenerator {
	return func(int) (ProcessFunc, error) {
		return wf, nil
	}
}

func Test1(t *testing.T) {
	t.Run("Test with Preset Pool Size", func(t *testing.T) {
		ctx := context.Background()
		max := 10
		inputChan := make(chan interface{})
		wg := &sync.WaitGroup{}

		outChan, err := Process(ctx, inputChan, processGeneratorGenerator(loadWorkerRun), &Options{PoolSize: 10})
		if err != nil {
			panic(err)
		}
		counter := 0
		go func(t *testing.T) {
			for out := range outChan {
				if _, ok := out.Value.(loadWorker); !ok {
					t.Error("Invalid output")
				} else {
					counter++
				}
				wg.Done()
			}
		}(t)

		// Create work and the associated order
		for work := 0; work < max; work++ {
			wg.Add(1)
			inputChan <- loadWorker(work)
		}
		close(inputChan)
		wg.Wait()
		if counter != max {
			t.Error("Input count does not match output count")
		}
		t.Log("Test with Preset Pool Size Completed")
	})
}

func Test2(t *testing.T) {
	t.Run("Test with default Pool Size", func(t *testing.T) {
		ctx := context.Background()

		max := 10
		inputChan := make(chan interface{})
		wg := &sync.WaitGroup{}

		outChan, err := Process(ctx, inputChan, processGeneratorGenerator(loadWorkerRun), &Options{OutChannelBuffer: 2})
		if err != nil {
			panic(err)
		}
		counter := 0
		go func(t *testing.T) {
			for out := range outChan {
				if _, ok := out.Value.(loadWorker); !ok {
					t.Error("Invalid output")
				} else {
					counter++
				}
				wg.Done()
			}
		}(t)

		// Create work and the associated order
		for work := 0; work < max; work++ {
			wg.Add(1)
			inputChan <- loadWorker(work)
		}
		close(inputChan)
		wg.Wait()
		if counter != max {
			t.Error("Input count does not match output count")
		}
		t.Log("Test with Default Pool Size Completed")
	})
}

func Test3(t *testing.T) {
	t.Run("Test Zero Load", func(t *testing.T) {
		ctx := context.Background()

		max := 10
		inputChan := make(chan interface{})
		wg := &sync.WaitGroup{}

		outChan, err := Process(ctx, inputChan, processGeneratorGenerator(zeroLoadWorkerRun), &Options{OutChannelBuffer: 2})
		if err != nil {
			panic(err)
		}
		counter := 0
		go func(t *testing.T) {
			for out := range outChan {
				if _, ok := out.Value.(zeroLoadWorker); !ok {
					t.Error("Invalid output")
				} else {
					counter++
				}
				wg.Done()
			}
		}(t)

		// Create work and the associated order
		for work := 0; work < max; work++ {
			wg.Add(1)
			inputChan <- zeroLoadWorker(work)
		}
		close(inputChan)
		wg.Wait()
		if counter != max {
			t.Error("Input count does not match output count")
		}
		t.Log("Test with Default Pool Size and Zero Load Completed")
	})

}

func Test4(t *testing.T) {
	t.Run("Test without workgroup", func(t *testing.T) {
		ctx := context.Background()

		max := 10
		inputChan := make(chan interface{})
		output, err := Process(ctx, inputChan, processGeneratorGenerator(zeroLoadWorkerRun), &Options{PoolSize: 10, OutChannelBuffer: 10})
		if err != nil {
			panic(err)
		}
		go func() {
			for work := 0; work < max; work++ {
				inputChan <- zeroLoadWorker(work)
			}
			close(inputChan)
		}()
		counter := 0
		for out := range output {
			if _, ok := out.Value.(zeroLoadWorker); !ok {
				t.Error("Invalid output")
			} else {
				counter++
			}
		}
		if counter != max {
			t.Error("Input count does not match output count")
		}
		t.Log("Test without workgroup Completed")
	})
}

func TestSortedData(t *testing.T) {
	t.Run("Test if response is sorted", func(t *testing.T) {
		ctx := context.Background()

		max := 10
		inputChan := make(chan interface{})
		output, err := Process(ctx, inputChan, processGeneratorGenerator(loadWorkerRun), &Options{PoolSize: 10, OutChannelBuffer: 10})
		if err != nil {
			panic(err)
		}
		go func() {
			for work := 0; work < max; work++ {
				inputChan <- loadWorker(work)
			}
			close(inputChan)
		}()
		var res []loadWorker
		for out := range output {
			res = append(res, out.Value.(loadWorker))
		}
		isSorted := sort.SliceIsSorted(res, func(i, j int) bool {
			return res[i] < res[j]
		})
		if !isSorted {
			t.Error("output is not sorted")
		}
		t.Log("Test if response is sorted")
	})
}

func TestSortedDataMultiple(t *testing.T) {
	for i := 0; i < 50; i++ {
		t.Run("Test if response is sorted", func(t *testing.T) {
			ctx := context.Background()

			max := 10
			inputChan := make(chan interface{})
			output, err := Process(ctx, inputChan, processGeneratorGenerator(loadWorkerRun), &Options{PoolSize: 10, OutChannelBuffer: 10})
			if err != nil {
				panic(err)
			}
			go func() {
				for work := 0; work < max; work++ {
					inputChan <- loadWorker(work)
				}
				close(inputChan)
			}()
			var res []loadWorker
			for out := range output {
				res = append(res, out.Value.(loadWorker))
			}
			isSorted := sort.SliceIsSorted(res, func(i, j int) bool {
				return res[i] < res[j]
			})
			if !isSorted {
				t.Error("output is not sorted")
			}
			t.Log("Test if response is sorted")
		})
	}
}

func TestStreamingInput(t *testing.T) {
	t.Run("Test streaming input", func(t *testing.T) {
		ctx := context.Background()
		inputChan := make(chan interface{}, 10)
		output, err := Process(ctx, inputChan, processGeneratorGenerator(zeroLoadWorkerRun), &Options{PoolSize: 10, OutChannelBuffer: 10})
		if err != nil {
			panic(err)
		}

		ticker := time.NewTicker(100 * time.Millisecond)
		done := make(chan bool)
		wg := &sync.WaitGroup{}
		go func() {
			input := 0
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					inputChan <- zeroLoadWorker(input)
					wg.Add(1)
					input++
				default:
				}
			}
		}()

		var res []zeroLoadWorker

		go func() {
			for out := range output {
				res = append(res, out.Value.(zeroLoadWorker))
				wg.Done()
			}
		}()

		time.Sleep(1600 * time.Millisecond)
		ticker.Stop()
		done <- true
		close(inputChan)
		wg.Wait()
		isSorted := sort.SliceIsSorted(res, func(i, j int) bool {
			return res[i] < res[j]
		})
		if !isSorted {
			t.Error("output is not sorted")
		}
		t.Log("Test streaming input")
	})
}

func BenchmarkOC(b *testing.B) {
	max := 100000
	inputChan := make(chan interface{})
	output, err := Process(context.Background(), inputChan, processGeneratorGenerator(zeroLoadWorkerRun), &Options{PoolSize: 10, OutChannelBuffer: 10})
	if err != nil {
		panic(err)
	}
	go func() {
		for work := 0; work < max; work++ {
			inputChan <- zeroLoadWorker(work)
		}
		close(inputChan)
	}()
	for out := range output {
		_ = out
	}
}

func BenchmarkOCLoad(b *testing.B) {
	max := 10
	inputChan := make(chan interface{})
	output, err := Process(context.Background(), inputChan, processGeneratorGenerator(loadWorkerRun), &Options{PoolSize: 10, OutChannelBuffer: 10})
	if err != nil {
		panic(err)
	}
	go func() {
		for work := 0; work < max; work++ {
			inputChan <- loadWorker(work)
		}
		close(inputChan)
	}()
	for out := range output {
		_ = out
	}
}

func BenchmarkOC2(b *testing.B) {
	for i := 0; i < 100; i++ {
		max := 1000
		inputChan := make(chan interface{})
		output, err := Process(context.Background(), inputChan, processGeneratorGenerator(zeroLoadWorkerRun), &Options{PoolSize: 10, OutChannelBuffer: 10})
		if err != nil {
			panic(err)
		}
		go func() {
			for work := 0; work < max; work++ {
				inputChan <- zeroLoadWorker(work)
			}
			close(inputChan)
		}()
		for out := range output {
			_ = out
		}
	}
}
