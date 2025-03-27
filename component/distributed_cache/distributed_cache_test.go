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
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common"
	"github.com/Azure/azure-storage-fuse/v2/common/config"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

var home_dir, _ = os.UserHomeDir()
var mountpoint = home_dir + "mountpoint"
var random *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
var ctx = context.Background()

type distributedCacheTestSuite struct {
	suite.Suite
	assert            *assert.Assertions
	fake_storage_path string
	disk_cache_path   string
	distributedCache  *DistributedCache
	mockCtrl          *gomock.Controller
	mockStorage       MockStorage
}

func randomString(length int) string {
	b := make([]byte, length)
	random.Read(b)
	return fmt.Sprintf("%x", b)[:length]
}

func getFakeStoragePath(base string) string {
	tmp_path := filepath.Join(home_dir, base+randomString(8))
	ret := os.Mkdir(tmp_path, 0777)
	log.Debug("Creating fake storage path %s", ret)
	return tmp_path
}

func (suite *distributedCacheTestSuite) SetupTest() {
	log.SetDefaultLogger("silent", common.LogConfig{Level: common.ELogLevel.LOG_DEBUG()})
	suite.fake_storage_path = getFakeStoragePath("fake_storage")
	suite.disk_cache_path = getFakeStoragePath("distributed_cache")

	defaultConfig := fmt.Sprintf("read-only: true\n\ndistributed_cache:\n  cache-id: mycache1\n  path: %s", suite.disk_cache_path)
	log.Debug(defaultConfig)
	suite.setupTestHelper(defaultConfig)
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockStorage = *NewMockStroage(suite.mockCtrl)
}

func (suite *distributedCacheTestSuite) setupTestHelper(cfg string) error {
	suite.assert = assert.New(suite.T())

	config.ReadConfigFromReader(strings.NewReader(cfg))
	config.Set("mount-path", mountpoint)

	suite.distributedCache = NewDistributedCacheComponent().(*DistributedCache)
	err := suite.distributedCache.Configure(true)
	if err != nil {
		return fmt.Errorf("Unable to configure distributed cache [%s]", err.Error())
	}
	suite.distributedCache.storage = &suite.mockStorage

	err = suite.distributedCache.Start(ctx)
	if err != nil {
		return fmt.Errorf("Unable to start distributed cache [%s]", err.Error())
	}

	return nil
}

func (suite *distributedCacheTestSuite) cleanupPipeline() error {
	config.ResetConfig()

	err := suite.distributedCache.Stop()
	if err != nil {
		log.Err("Unable to stop distributed cache [%s]", err.Error())
		return nil
	}

	os.RemoveAll(suite.fake_storage_path)
	os.RemoveAll(suite.disk_cache_path)
	return nil
}

func (suite *distributedCacheTestSuite) TestManadatoryConfigMissing() {
	suite.assert.Equal(suite.distributedCache.Name(), "distributed_cache")
	suite.assert.EqualValues("mycache1", suite.distributedCache.cacheID)
	suite.assert.EqualValues(suite.disk_cache_path, suite.distributedCache.cachePath)
	suite.assert.EqualValues(3, suite.distributedCache.replicas)
	suite.assert.EqualValues(30, suite.distributedCache.hbDuration)
	suite.assert.EqualValues(0, suite.distributedCache.hbTimeout)

	emptyConfig := fmt.Sprintf("read-only: true\n\nloopbackfs:\n  path: %s\n\ndistributed_cache:\n  cache-id: mycache1", suite.fake_storage_path)
	err := suite.setupTestHelper(emptyConfig)
	defer suite.cleanupPipeline()

	suite.assert.Equal(err.Error(), "Unable to configure distributed cache [config error in distributed_cache error [cache-path not set]]")

	emptyConfig = fmt.Sprintf("read-only: true\n\nloopbackfs:\n  path: %s", suite.fake_storage_path)
	err = suite.setupTestHelper(emptyConfig)
	suite.assert.Equal(err.Error(), "Unable to configure distributed cache [config error in distributed_cache error [cache-id not set]]")

	emptyConfig = fmt.Sprintf("read-only: true\n\nloopbackfs:\n  path: %s\n\ndistributed_cache:\n  cache-id: mycache1\n  path: %s", suite.fake_storage_path, suite.disk_cache_path)
	err = suite.setupTestHelper(emptyConfig)
	suite.assert.Equal(err.Error(), "Unable to start distributed cache [DistributedCache::Start : error [invalid or missing storage component]]")

	emptyConfig = fmt.Sprintf("read-only: true\n\nloopbackfs:\n  path: %s\n\ndistributed_cache:\n  path: %s", suite.fake_storage_path, suite.disk_cache_path)
	err = suite.setupTestHelper(emptyConfig)
	suite.assert.Equal(err.Error(), "Unable to configure distributed cache [config error in distributed_cache error [cache-id not set]]")

}

func (suite *distributedCacheTestSuite) TestDistributedCacheSetupCacheStructure() {

	cacheDir := "__CACHE__" + suite.distributedCache.cacheID
	suite.mockStorage.EXPECT().GetAttr(gomock.Any()).Return(nil, nil)
	// suite.mockStorage.EXPECT().
	// 	GetAttr(gomock.Any()).
	// 	Return(&internal.ObjAttr{}, nil).
	// 	Times(1)
	err := suite.distributedCache.setupCacheStructure(cacheDir)
	suite.assert.Nil(err)

	suite.mockStorage.EXPECT().GetAttr(cacheDir+"/creator.txt").Return(nil, errors.New("Failed"))
	err = suite.distributedCache.setupCacheStructure(cacheDir)
	suite.assert.Nil(err)
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
