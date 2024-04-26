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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type blockTestSuite struct {
	suite.Suite
	assert *assert.Assertions
}

func (suite *blockTestSuite) SetupTest() {
}

func (suite *blockTestSuite) cleanupTest() {
}

func (suite *blockTestSuite) Testallocate() {
	suite.assert = assert.New(suite.T())

	b, err := allocate[interface{}](0)
	suite.assert.Nil(b)
	suite.assert.NotNil(err)
	suite.assert.Contains(err.Error(), "invalid size")

	b, err = allocate[interface{}](10)
	suite.assert.NotNil(b)
	suite.assert.Nil(err)
	suite.assert.NotNil(b.data)

	_ = b.release()
}

func (suite *blockTestSuite) TestallocateBig() {
	suite.assert = assert.New(suite.T())

	b, err := allocate[interface{}](100 * 1024 * 1024)
	suite.assert.NotNil(b)
	suite.assert.Nil(err)
	suite.assert.NotNil(b.data)
	suite.assert.Equal(cap(b.data), 100*1024*1024)

	b.release()
}

func (suite *blockTestSuite) TestallocateHuge() {
	suite.assert = assert.New(suite.T())

	b, err := allocate[interface{}](50 * 1024 * 1024 * 1024)
	suite.assert.Nil(b)
	suite.assert.NotNil(err)
	suite.assert.Contains(err.Error(), "mmap error")
}

func (suite *blockTestSuite) TestFreeNilData() {
	suite.assert = assert.New(suite.T())

	b, err := allocate[interface{}](1)
	suite.assert.NotNil(b)
	suite.assert.Nil(err)
	b.data = nil

	err = b.release()
	suite.assert.NotNil(err)
	suite.assert.Contains(err.Error(), "invalid buffer")
}

func (suite *blockTestSuite) TestFreeInvalidData() {
	suite.assert = assert.New(suite.T())

	b, err := allocate[interface{}](1)
	suite.assert.NotNil(b)
	suite.assert.Nil(err)
	b.data = make([]byte, 1)

	err = b.release()
	suite.assert.NotNil(err)
	suite.assert.Contains(err.Error(), "invalid argument")
}

func (suite *blockTestSuite) TestReuse() {
	suite.assert = assert.New(suite.T())

	type info struct {
		name  string
		value int32
		key   *os.FileInfo
	}

	b, err := allocate[*info](1)
	suite.assert.Nil(err)
	suite.assert.NotNil(b)
	suite.assert.NotNil(b.data)

	b.appInfo = &info{
		name:  "test",
		value: 10,
		key:   nil,
	}

	t := b.appInfo
	suite.assert.Equal(t.name, "test")

	b.ReUse()
	suite.assert.NotNil(b.data)
}

func TestBlockSuite(t *testing.T) {
	suite.Run(t, new(blockTestSuite))
}
