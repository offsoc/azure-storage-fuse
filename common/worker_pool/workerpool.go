/*
    _____           _____   _____   ____          ______  _____  ------
   |     |  |      |     | |     | |     |     | |       |            |
   |     |  |      |     | |     | |     |     | |       |            |
   | --- |  |      |     | |-----| |---- |     | |-----| |-----  ------
   |     |  |      |     | |     | |     |     |       | |       |
   | ____|  |_____ | ____| | ____| |     |_____|  _____| |_____  |_____


   Licensed under the MIT License <http://opensource.org/licenses/MIT>.

   Copyright © 2020-2024 Microsoft Corporation. All rights reserved.
   Author : <blobfusedev@microsoft.com>

   Permission is hereby granted, free of charge, to any person obtaining a copy
   of this software and associated documentation files (the "Software"), to deal
   in the Software without restriction, including without limitation the rights
   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
   copies of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:

   The above copyright notice and this permission notice shall be included in all
   copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE
*/

package worker_pool

import (
	"sync"
)

// Define a type for the callback function.
type CallbackFunc[T any] func(T) error

// WorkerPool is a group of workers that can be used to execute a task
type WorkerPool[T any] struct {
	// Number of workers running in this group
	workerCount uint32

	// Channel to hold pending work
	work chan T

	// Wait group to wait for all workers to finish
	wg sync.WaitGroup

	// Reader method that will actually read the data
	callback CallbackFunc[T]
}

// newWorkerPool creates a new worker pool
func Allocate[T any](count uint32, callback CallbackFunc[T]) *WorkerPool[T] {
	if count == 0 || callback == nil {
		return nil
	}

	return &WorkerPool[T]{
		workerCount: count,
		callback:    callback,
		work:        make(chan T, count*3),
	}
}

// Start all the workers and wait till they start receiving requests
func (t *WorkerPool[T]) Start() {
	for i := uint32(0); i < t.workerCount; i++ {
		t.wg.Add(1)
		go t.process()
	}
}

// Stop all the workers workers
func (t *WorkerPool[T]) Stop() {
	close(t.work)
	t.wg.Wait()
}

// Schedule the download of a block
func (t *WorkerPool[T]) Schedule(item T) {
	t.work <- item
}

// process is the core task to be executed by each worker worker
func (t *WorkerPool[T]) process() {
	defer t.wg.Done()

	for item := range t.work {
		t.callback(item)
	}
}
