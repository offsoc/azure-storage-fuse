/*
    _____           _____   _____   ____          ______  _____  ------
   |     |  |      |     | |     | |     |     | |       |            |
   |     |  |      |     | |     | |     |     | |       |            |
   | --- |  |      |     | |-----| |---- |     | |-----| |-----  ------
   |     |  |      |     | |     | |     |     |       | |       |
   | ____|  |_____ | ____| | ____| |     |_____|  _____| |_____  |_____


   Licensed under the MIT License <http://opensource.org/licenses/MIT>.

   Copyright © 2020-2025 Microsoft Corporation. All rights reserved.
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

package distributed_cache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/config"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

var ctx = context.Background()

type distributedCacheTestSuite struct {
	suite.Suite
	assert           *assert.Assertions
	distributedCache *DistributedCache
	mockCtrl         *gomock.Controller
	mock             *internal.MockComponent
}

func (suite *distributedCacheTestSuite) SetupTest() {
	log.SetDefaultLogger("silent", common.LogConfig{Level: common.ELogLevel.LOG_DEBUG()})
	defaultConfig := "distributed_cache:\n  cache-id: mycache1\n  path: \\tmp"
	log.Debug(defaultConfig)

	suite.setupTestHelper(defaultConfig)
}

func (suite *distributedCacheTestSuite) setupTestHelper(cfg string) error {
	suite.assert = assert.New(suite.T())

	config.ReadConfigFromReader(strings.NewReader(cfg))

	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mock = internal.NewMockComponent(suite.mockCtrl)
	suite.distributedCache = NewDistributedCacheComponent().(*DistributedCache)
	suite.distributedCache.SetNextComponent(suite.mock)
	err := suite.distributedCache.Configure(true)
	if err != nil {
		return fmt.Errorf("Unable to configure distributed cache [%s]", err.Error())
	}
	return nil
}

func (suite *distributedCacheTestSuite) TearDownTest() error {
	config.ResetConfig()

	err := suite.distributedCache.Stop()
	if err != nil {
		log.Err("Unable to stop distributed cache [%s]", err.Error())
		return nil
	}

	return nil
}

func (suite *distributedCacheTestSuite) TestManadatoryConfigMissing() {
	suite.mock.EXPECT().GetAttr(gomock.Any()).Return(&internal.ObjAttr{}, errors.New("Failed"))
	suite.distributedCache.Start(ctx)
	suite.assert.Equal(suite.distributedCache.Name(), "distributed_cache")
	suite.assert.EqualValues("mycache1", suite.distributedCache.cacheID)
	suite.assert.EqualValues("\\tmp", suite.distributedCache.cachePath)
	suite.assert.EqualValues(uint8(3), suite.distributedCache.replicas)
	suite.assert.EqualValues(uint16(30), suite.distributedCache.hbDuration)

	emptyConfig := "read-only: true\n\ndistributed_cache:\n  cache-id: mycache1"
	err := suite.setupTestHelper(emptyConfig)

	suite.assert.Equal("Unable to configure distributed cache [config error in distributed_cache: [cache-path not set]]", err.Error())

	emptyConfig = ""
	err = suite.setupTestHelper(emptyConfig)
	suite.assert.Equal("Unable to configure distributed cache [config error in distributed_cache: [cache-id not set]]", err.Error())

	emptyConfig = "read-only: true\n\ndistributed_cache:\n  path: \\tmp"
	err = suite.setupTestHelper(emptyConfig)
	suite.assert.Equal("Unable to configure distributed cache [config error in distributed_cache: [cache-id not set]]", err.Error())
}

func (suite *distributedCacheTestSuite) TestSetupCacheStructureSuccess() {
	suite.mock.EXPECT().GetAttr(gomock.Any()).Return(&internal.ObjAttr{}, syscall.ENOENT)
	suite.mock.EXPECT().CreateDir(gomock.Any()).Return(nil).AnyTimes()
	suite.mock.EXPECT().WriteFromBuffer(gomock.Any()).Return(nil)
	err := suite.distributedCache.Start(ctx)
	suite.assert.Nil(err)
}

func (suite *distributedCacheTestSuite) TestSetupCacheStructureFailToReadStorage() {
	suite.mock.EXPECT().GetAttr(gomock.Any()).Return(&internal.ObjAttr{}, syscall.EACCES)
	err := suite.distributedCache.Start(ctx)
	suite.assert.NotNil(err)
	suite.assert.Equal("DistributedCache::Start error [failed to read creator file: permission denied]", err.Error())
}

func (suite *distributedCacheTestSuite) TestSetupCacheStructureFailToCreateDir() {
	suite.mock.EXPECT().GetAttr(gomock.Any()).Return(&internal.ObjAttr{}, syscall.ENOENT)
	suite.mock.EXPECT().CreateDir(gomock.Any()).Return(errors.New("Failed to create dir"))
	err := suite.distributedCache.Start(ctx)
	suite.assert.NotNil(err)
	suite.assert.Equal("DistributedCache::Start error [failed to create directory __CACHE__mycache1: Failed to create dir]", err.Error())
}

func (suite *distributedCacheTestSuite) TestSetupCacheStructureFailToCreateNodeDir() {
	suite.mock.EXPECT().GetAttr(gomock.Any()).Return(&internal.ObjAttr{}, syscall.ENOENT)
	opt1 := internal.CreateDirOptions{Name: "__CACHE__" + suite.distributedCache.cacheID, Etag: true}
	suite.mock.EXPECT().CreateDir(opt1).Return(nil)
	opt2 := internal.CreateDirOptions{Name: "__CACHE__" + suite.distributedCache.cacheID + "/Nodes", Etag: true}
	suite.mock.EXPECT().CreateDir(opt2).Return(errors.New("Failed to create dir"))
	err := suite.distributedCache.Start(ctx)
	suite.assert.NotNil(err)
	suite.assert.Equal("DistributedCache::Start error [failed to create directory __CACHE__mycache1/Nodes: Failed to create dir]", err.Error())
}

func (suite *distributedCacheTestSuite) TestSetupCacheStructureFailToCreateObjectDir() {
	suite.mock.EXPECT().GetAttr(gomock.Any()).Return(&internal.ObjAttr{}, syscall.ENOENT)
	opt1 := internal.CreateDirOptions{Name: "__CACHE__" + suite.distributedCache.cacheID, Etag: true}
	suite.mock.EXPECT().CreateDir(opt1).Return(nil)
	opt2 := internal.CreateDirOptions{Name: "__CACHE__" + suite.distributedCache.cacheID + "/Nodes", Etag: true}
	suite.mock.EXPECT().CreateDir(opt2).Return(nil)
	opt3 := internal.CreateDirOptions{Name: "__CACHE__" + suite.distributedCache.cacheID + "/Objects", Etag: true}
	suite.mock.EXPECT().CreateDir(opt3).Return(errors.New("Failed to create dir"))
	err := suite.distributedCache.Start(ctx)
	suite.assert.NotNil(err)
	suite.assert.Equal("DistributedCache::Start error [failed to create directory __CACHE__mycache1/Objects: Failed to create dir]", err.Error())
}

func (suite *distributedCacheTestSuite) TestSetupCacheStructureFailToWriteCreatoFile() {
	suite.mock.EXPECT().GetAttr(gomock.Any()).Return(&internal.ObjAttr{}, syscall.ENOENT)
	suite.mock.EXPECT().CreateDir(gomock.Any()).Return(nil).AnyTimes()
	suite.mock.EXPECT().WriteFromBuffer(gomock.Any()).Return(errors.New("Failed to create file"))
	err := suite.distributedCache.Start(ctx)
	suite.assert.NotNil(err)
	suite.assert.Equal("DistributedCache::Start error [failed to create creator file: Failed to create file]", err.Error())
}

func (suite *distributedCacheTestSuite) TestMetaDate() {
	err := suite.distributedCache.CreateMetadataFile("__CACHE_2311", "metadatafile1.json.md")
	suite.assert.Nil(err, "Failed to create metadata file")
	// check if the file is creatged
	// _, err = os.Stat("metadatafile1.json.md")
	// require.NoError(t, err, "Metadata file should be created")
}

func (suite *distributedCacheTestSuite) TestUpdateMetadata() {
	dummyReplica := map[string]interface{}{
		"offset":      "0938293",
		"size":        "1024",
		"num-stripes": "4",
		"stripe-size": "256",
		"nodes":       []string{"node1", "node2"},
	}
	suite.distributedCache.UpdateMetadataFile("__CACHE__2311", "metadatafile1.json.md", dummyReplica)
	// check if the file is updated
	metadataFile, err := os.ReadFile("metadatafile1.json.md")
	suite.assert.Nil(err, "Failed to read metadata file")
	suite.assert.Contains(string(metadataFile), "0938293", "Metadata file should be updated")
	suite.assert.Contains(string(metadataFile), "1024", "Metadata file should be updated")
	suite.assert.Contains(string(metadataFile), "4", "Metadata file should be updated")
	suite.assert.Contains(string(metadataFile), "256", "Metadata file should be updated")
	suite.assert.Contains(string(metadataFile), "node1", "Metadata file should be updated")
}

func (suite *distributedCacheTestSuite) TestWithGoRoutines() {
	cmd := exec.Command("bash", "-c", "../../blobfuse2", "/home/anubhuti/mntdir")
	_, err := cmd.Output()
	if err != nil {
		suite.assert.Fail("Failed to run command: %s", err)
	}
	// Start the timer
	startTime := time.Now()

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Number of goroutines to spawn
	numGoroutines := 50

	// Create 50 goroutines and run them in parallel to call CreateMetadataFile
	for i := range numGoroutines {
		wg.Add(1) // Increment the wait group counter
		go func(i int) {
			defer wg.Done() // Decrement the counter when the goroutine finishes

			// Generate a unique file name for each goroutine
			fileName := fmt.Sprintf("metadata_%d.md", i)

			// Call CreateMetadataFile
			err := suite.distributedCache.CreateMetadataFile("__CACHE__211", fileName)
			suite.assert.Nil(err, "Failed to create metadata file")
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Record the total time taken
	totalTime := time.Since(startTime)
	fmt.Printf("CreateMetadatFile :: Total time taken for %d goroutines: %v\n", numGoroutines, totalTime)

	cmd = exec.Command("bash", "-c", "../../blobfuse2", "unmount", "all")
	_, err = cmd.Output()
	if err != nil {
		suite.assert.Fail("Failed to run command: %s", err)
	}
}

// write a tes

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestDistributedCacheTestSuite(t *testing.T) {

	suite.Run(t, new(distributedCacheTestSuite))
}
