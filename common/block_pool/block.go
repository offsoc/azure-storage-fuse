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

package block_pool

import (
	"fmt"
	"syscall"
)

// Block is a memory mapped buffer with its state to hold data
type Block[T any] struct {
	name    string // Name of object this belong to
	id      string // Unique id of this block in the object
	offset  uint64 // Start offset of the data this block holds
	length  uint64 // Length of data this block holds
	data    []byte // Data read from blob
	appInfo T      // Any additional info that application wants to hold
}

// Allocate creates a new memory mapped buffer for the given size
func allocate[T any](size uint64) (*Block[T], error) {
	if size == 0 {
		return nil, fmt.Errorf("invalid size")
	}

	prot, flags := syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE
	addr, err := syscall.Mmap(-1, 0, int(size), prot, flags)

	if err != nil {
		return nil, fmt.Errorf("mmap error: %v", err)
	}

	block := &Block[T]{
		name:   "",
		id:     "",
		offset: 0,
		length: 0,
		data:   addr,
	}

	return block, nil
}

// Release cleans up the memory mapped buffer
func (b *Block[T]) release() error {
	if b.data == nil {
		return fmt.Errorf("invalid buffer")
	}

	err := syscall.Munmap(b.data)
	b.data = nil
	if err != nil {
		// if we get here, there is likely memory corruption.
		return fmt.Errorf("munmap error: %v", err)
	}

	return nil
}

// ReUse re-inits the Block by just keeping the data buffer and resetting the rest
func (b *Block[T]) ReUse() {
	b.name = ""
	b.id = ""
	b.offset = 0
	b.length = 0
}

// Get the app info back
func (b *Block[T]) AppInfo() T {
	return b.appInfo
}
