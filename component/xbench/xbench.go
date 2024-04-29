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

package xbench

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/config"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal"
)

/* NOTES:
   - Component shall have a structure which inherits "internal.BaseComponent" to participate in pipeline
   - Component shall register a name and its constructor to participate in pipeline  (add by default by generator)
   - Order of calls : Constructor -> Configure -> Start ..... -> Stop
   - To read any new setting from config file follow the Configure method default comments
*/

// Common structure for Component
type Xbench struct {
	internal.BaseComponent
	path      string
	buff      []byte
	blockSize uint64
	dataSize  uint64
	fileCount uint32
}

const (
	_1MB = (1024 * 1024)
)

// Structure defining your config parameters
type XbenchOptions struct {
	// e.g. var1 uint32 `config:"var1"`
	Path string `config:"bench-path" yaml:"bench-path,omitempty"`
}

const compName = "xbench"

// Verification to check satisfaction criteria with Component Interface
var _ internal.Component = &Xbench{}

func (c *Xbench) Name() string {
	return compName
}

func (c *Xbench) SetName(name string) {
	c.BaseComponent.SetName(name)
}

func (c *Xbench) SetNextComponent(nc internal.Component) {
	c.BaseComponent.SetNextComponent(nc)
}

// Start : Pipeline calls this method to start the component functionality
//
//	this shall not block the call otherwise pipeline will not start
func (c *Xbench) Start(ctx context.Context) error {
	log.Trace("Xbench::Start : Starting component %s", c.Name())

	// Xbench : start code goes here
	go c.StartTests()

	return nil
}

// Stop : Stop the component functionality and kill all threads started
func (c *Xbench) Stop() error {
	log.Trace("Xbench::Stop : Stopping component %s", c.Name())

	return nil
}

// Configure : Pipeline will call this method after constructor so that you can read config and initialize yourself
//
//	Return failure if any config is not valid to exit the process
func (c *Xbench) Configure(_ bool) error {
	log.Trace("Xbench::Configure : %s", c.Name())

	// >> If you do not need any config parameters remove below code and return nil
	conf := XbenchOptions{}
	err := config.UnmarshalKey(c.Name(), &conf)
	if err != nil {
		log.Err("Xbench::Configure : config error [invalid config attributes]")
		return fmt.Errorf("Xbench: config error [invalid config attributes]")
	}
	// Extract values from 'conf' and store them as you wish here

	c.blockSize = (8 * _1MB)
	c.dataSize = (40 * 1024 * _1MB)
	c.fileCount = 10

	c.path = common.ExpandPath(conf.Path)
	if c.path == "" {
		log.Err("Xbench::Configure : config error [bench-path not set]")
		return fmt.Errorf("config error in %s error [bench-path not set]", c.Name())
	}

	c.buff = make([]byte, c.blockSize)

	_, err = rand.Read(c.buff)
	if err != nil {
		log.Err("Xbench::Configure : Error in filling buffer with random data")
		return err
	}

	return nil
}

// OnConfigChange : If component has registered, on config file change this method is called
func (c *Xbench) OnConfigChange() {
}

func (c *Xbench) StartTests() {
	tests := []string{"localWrite", "localRead", "remoteWrite", "remoteRead", "multiRemoteWrite", "multiRemoteRead"}
	var err error

	log.Info("Xbench::StartTests : Starting tests")
	fileCount := 0
	for _, test := range tests {
		log.Info("Xbench::StartTests : Starting tests [%v]", test)
		startTime := time.Now()

		switch {
		case test == "localWrite":
			fileCount = 1
			err = c.LocalWriteTest(0)
		case test == "localRead":
			fileCount = 1
			err = c.LocalReadTest(0)
		case test == "remoteWrite":
			fileCount = 1
			err = c.RemoteWriteTest(0)
		case test == "remoteRead":
			fileCount = 1
			err = c.RemoteReadTest(0)
		case test == "multiRemoteWrite":
			fileCount = int(c.fileCount)
			err = c.MultiRemoteWriteTest()
		case test == "multiRemoteRead":
			fileCount = int(c.fileCount)
			err = c.MultiRemoteReadTest()
		default:
			log.Err("Xbench::StartTests : Invalid test name %s", test)
		}

		runTime := time.Since(startTime)
		if err != nil {
			log.Err("Xbench::StartTests : %s test failed %v", test, err)
			return
		} else {
			timeTaken := runTime.Seconds()
			log.Info("Xbench::StartTests : Test %s completed in %v seconds for %v bytes", test, timeTaken, (c.dataSize * uint64(fileCount)))
			speed := float64((c.dataSize/(_1MB))*uint64(fileCount)) / float64(timeTaken)
			log.Info("Xbench::StartTests : Test %s [%v MB in %v seconds, speed : %.2f MB/s]", test, (c.dataSize/(_1MB))*uint64(fileCount), timeTaken, speed)
		}
	}

	log.Info("Xbench::StartTests : Stopping tests")
}

func (c *Xbench) LocalWriteTest(fileNum int) error {
	// Write to local disk
	fileName := fmt.Sprintf("%s/testLocal_%d.data", c.path, fileNum)
	h, err := os.Create(fileName)
	if err != nil {
		return err
	}

	bytesWritten := uint64(0)
	for bytesWritten < c.dataSize {
		n, err := h.Write(c.buff)
		if err != nil {
			log.Err("Xbench::LocalWriteTest : Failed to write local file %s [%v]", fileName, err)
			return err
		}
		bytesWritten += uint64(n)
	}

	_ = h.Close()
	return nil
}

func (c *Xbench) LocalReadTest(fileNum int) error {
	// Read from local disk
	fileName := fmt.Sprintf("%s/testLocal_%d.data", c.path, fileNum)
	h, err := os.Open(fileName)
	if err != nil {
		return err
	}

	bytesRead := uint64(0)
	for bytesRead < c.dataSize {
		n, err := h.Read(c.buff)
		if err != nil {
			log.Err("Xbench::LocalReadTest : Failed to read local file %s [%v]", fileName, err)
			return err
		}
		bytesRead += uint64(n)
	}

	_ = h.Close()
	return nil
}

func (c *Xbench) RemoteWriteTest(fileNum int) error {
	// Write to remote location
	fileName := fmt.Sprintf("%s/testRemote_%d.data", c.path, fileNum)
	h, err := c.NextComponent().CreateFile(internal.CreateFileOptions{
		Name: fileName,
		Mode: 0666,
	})
	if err != nil {
		return err
	}

	bytesWritten := uint64(0)
	for bytesWritten < c.dataSize {
		n, err := c.NextComponent().WriteFile(internal.WriteFileOptions{
			Handle: h,
			Offset: int64(bytesWritten),
			Data:   c.buff,
		})
		if err != nil {
			log.Err("Xbench::RemoteWriteTest : Failed to write remote file %s [%v]", fileName, err)
			return err
		}
		bytesWritten += uint64(n)
	}

	_ = c.NextComponent().CloseFile(internal.CloseFileOptions{
		Handle: h,
	})

	return nil
}

func (c *Xbench) RemoteReadTest(fileNum int) error {
	// Read from remote location
	fileName := fmt.Sprintf("%s/testRemote_%d.data", c.path, fileNum)
	h, err := c.NextComponent().OpenFile(internal.OpenFileOptions{
		Name:  fileName,
		Flags: os.O_RDONLY,
		Mode:  0666,
	})
	if err != nil {
		return err
	}

	bytesRead := uint64(0)
	for bytesRead < c.dataSize {
		n, err := c.NextComponent().ReadInBuffer(internal.ReadInBufferOptions{
			Handle: h,
			Offset: int64(bytesRead),
			Data:   c.buff,
		})
		if err != nil {
			log.Err("Xbench::RemoteWriteTest : Failed to write remote file %s [%v]", fileName, err)
			return err
		}
		bytesRead += uint64(n)
	}

	_ = c.NextComponent().CloseFile(internal.CloseFileOptions{
		Handle: h,
	})
	return nil
}

func (c *Xbench) MultiRemoteWriteTest() error {
	var err error = nil
	wg := sync.WaitGroup{}

	writeFunc := func(fileNum int) {
		defer wg.Done()
		currentErr := c.RemoteWriteTest(fileNum)
		if err != nil {
			err = currentErr
		}
	}

	for i := 0; i < int(c.fileCount); i++ {
		wg.Add(1)
		go writeFunc(i)
	}

	wg.Wait()
	return err
}

func (c *Xbench) MultiRemoteReadTest() error {
	var err error = nil
	wg := sync.WaitGroup{}

	readFunc := func(fileNum int) {
		defer wg.Done()
		currentErr := c.RemoteReadTest(fileNum)
		if err != nil {
			err = currentErr
		}
	}

	for i := 0; i < int(c.fileCount); i++ {
		wg.Add(1)
		go readFunc(i)
	}

	wg.Wait()
	return err
}

// ------------------------- Factory -------------------------------------------

// Pipeline will call this method to create your object, initialize your variables here
// << DO NOT DELETE ANY AUTO GENERATED CODE HERE >>
func NewXbenchComponent() internal.Component {
	comp := &Xbench{}
	comp.SetName(compName)
	return comp
}

// On init register this component to pipeline and supply your constructor
func init() {
	internal.AddComponent(compName, NewXbenchComponent)

	pathFlag := config.AddStringFlag("bench-path", "", "configures the tmp location for the xbench. Configure the fastest disk (SSD or ramdisk) for best performance.")
	config.BindPFlag(compName+".bench-path", pathFlag)

}
