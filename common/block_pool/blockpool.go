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

import "github.com/Azure/azure-storage-fuse/v2/common/log"

// BlockPool is a pool of Blocks
type BlockPool[T any] struct {
	// Channel holding free blocks
	blocksCh chan *Block[T]

	// Size of each block this pool holds
	blockSize uint64

	// Number of block that this pool can handle at max
	maxBlocks uint32
}

// Allocate allocates a new pool of blocks
func Allocate[T any](blockSize uint64, memSize uint64) *BlockPool[T] {
	// Ignore if config is invalid
	if blockSize == 0 || memSize < blockSize {
		log.Err("blockpool::NewBlockPool : blockSize : %v, memsize: %v", blockSize, memSize)
		return nil
	}

	// Calculate how many blocks can be allocated
	blockCount := uint32(memSize / blockSize)

	pool := &BlockPool[T]{
		blocksCh:  make(chan *Block[T], blockCount),
		maxBlocks: uint32(blockCount),
		blockSize: blockSize,
	}

	// Preallocate all blocks so that during runtime we do not spend CPU cycles on this
	for i := (uint32)(0); i < blockCount; i++ {
		b, err := allocate[T](blockSize)
		if err != nil {
			return nil
		}

		pool.blocksCh <- b
	}

	return pool
}

// Terminate ends the block pool life
func (pool *BlockPool[T]) Terminate() {
	close(pool.blocksCh)

	// Release back the memory allocated to each block
	for {
		b := <-pool.blocksCh
		if b == nil {
			break
		}
		_ = b.release()
	}
}

// Usage provides % usage of this block pool
func (pool *BlockPool[T]) Usage() uint32 {
	return ((pool.maxBlocks - (uint32)(len(pool.blocksCh))) * 100) / pool.maxBlocks
}

// GetBlock returns a block. Wait till a block is freed
func (pool *BlockPool[T]) GetBlock() *Block[T] {
	return <-pool.blocksCh
}

// GetBlockNoWait returns a block if available nil otherwise
func (pool *BlockPool[T]) GetBlockNoWait() *Block[T] {
	var b *Block[T] = nil

	select {
	case b = <-pool.blocksCh:
		b.ReUse()
	default:
		return nil
	}

	return b
}

// Release back the Block to the pool
func (pool *BlockPool[T]) ReleaseBlock(b *Block[T]) {
	pool.blocksCh <- b
}
