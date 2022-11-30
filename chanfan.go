package chanfan

import (
	"errors"
	"runtime"
	"strings"
	"sync"
)

func zeroOrFirst(bufSize ...int) int {
	if len(bufSize) == 0 {
		return 0
	}
	return bufSize[0]
}

func Fan[T any](in <-chan T, fanSize int, bufSize ...int) []<-chan T {
	if fanSize == 0 {
		fanSize = runtime.GOMAXPROCS(0)
	}

	chans := make([]chan T, fanSize)
	outs := make([]<-chan T, fanSize)
	for i := range chans {
		chans[i] = make(chan T, zeroOrFirst(bufSize...))
		outs[i] = chans[i]
	}

	for i := 0; i < fanSize; i++ {
		go func(i int) {
			for v := range in {
				chans[i] <- v
			}
			close(chans[i])
		}(i)
	}

	return outs
}

func Queue[T any](in []T, bufSize ...int) <-chan T {
	out := make(chan T, zeroOrFirst(bufSize...))

	go func() {
		for _, v := range in {
			out <- v
		}
		close(out)
	}()

	return out
}

func Merge[T any](chans []<-chan T, bufSize ...int) <-chan T {
	out := make(chan T, zeroOrFirst(bufSize...))

	var wg sync.WaitGroup
	wg.Add(len(chans))

	go func() {
		wg.Wait()
		close(out)
	}()

	for _, ch := range chans {
		go func(ch <-chan T) {
			for v := range ch {
				out <- v
			}
			wg.Done()
		}(ch)
	}

	return out
}

type Result[T any] struct {
	Value T
	Error error
}

func Process[T any, K any](
	in <-chan T,
	process func(T) (K, error),
	bufSize ...int,
) <-chan *Result[K] {
	out := make(chan *Result[K], zeroOrFirst(bufSize...))

	go func() {
		for v := range in {
			r, err := process(v)
			out <- &Result[K]{
				Value: r,
				Error: err,
			}
		}
		close(out)
	}()

	return out
}

func ProcessMany[T any, K any](
	inputs []<-chan T,
	process func(T) (K, error),
	bufSize ...int,
) []<-chan *Result[K] {
	outputs := make([]<-chan *Result[K], len(inputs))

	for i, in := range inputs {
		outputs[i] = Process(in, process, bufSize...)
	}

	return outputs
}

func ProcessAndMerge[T any, K any](
	inputs []<-chan T,
	process func(T) (K, error),
	bufSize ...int,
) <-chan *Result[K] {
	return Merge(ProcessMany(inputs, process, bufSize...))
}

func CollectErrors[T any](in <-chan *Result[T]) error {
	errorMessages := []string{}

	for r := range in {
		if r.Error != nil {
			errorMessages = append(errorMessages, r.Error.Error())
		}
	}

	return errors.New(strings.Join(errorMessages, "\n"))
}

func Collect[T any](in <-chan *Result[T], appendOnErr bool) ([]T, error) {
	out := []T{}
	errorMessages := []string{}

	for r := range in {
		if r.Error != nil {
			errorMessages = append(errorMessages, r.Error.Error())
			if !appendOnErr {
				continue
			}
		}

		out = append(out, r.Value)
	}

	var err error
	if len(errorMessages) > 0 {
		err = errors.New(strings.Join(errorMessages, "\n"))
	}

	return out, err
}
