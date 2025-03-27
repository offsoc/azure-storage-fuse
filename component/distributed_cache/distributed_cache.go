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
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-storage-fuse/v2/common/config"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/component/azstorage"
	"github.com/Azure/azure-storage-fuse/v2/internal"
)

/* NOTES:
   - Component shall have a structure which inherits "internal.BaseComponent" to participate in pipeline
   - Component shall register a name and its constructor to participate in pipeline  (add by default by generator)
   - Order of calls : Constructor -> Configure -> Start ..... -> Stop
   - To read any new setting from config file follow the Configure method default comments
*/

// Common structure for Component
type DistributedCache struct {
	internal.BaseComponent
	cacheID    string
	cachePath  string
	replicas   uint32
	hbTimeout  uint32
	hbDuration uint32

	storage          azstorage.AzConnection
	heartbeatManager *HeartbeatManager
}

// Structure defining your config parameters
type DistributedCacheOptions struct {
	CacheID           string `config:"cache-id" yaml:"cache-id,omitempty"`
	CachePath         string `config:"path" yaml:"path,omitempty"`
	ChunkSize         uint64 `config:"chunk-size" yaml:"chunk-size,omitempty"`
	CacheSize         uint64 `config:"cache-size" yaml:"cache-size,omitempty"`
	Replicas          uint32 `config:"replicas" yaml:"replicas,omitempty"`
	HeartbeatTimeout  uint32 `config:"heartbeat-timeout" yaml:"heartbeat-timeout,omitempty"`
	HeartbeatDuration uint32 `config:"heartbeat-duration" yaml:"heartbeat-duration,omitempty"`
	MissedHeartbeat   uint32 `config:"heartbeats-till-node-down" yaml:"heartbeats-till-node-down,omitempty"`
}

const (
	compName          = "distributed_cache"
	HeartBeatDuration = 30
	REPLICAS          = 3
)

// Verification to check satisfaction criteria with Component Interface
var _ internal.Component = &DistributedCache{}

func (distributedCache *DistributedCache) Name() string {
	return compName
}

func (distributedCache *DistributedCache) SetName(name string) {
	distributedCache.BaseComponent.SetName(name)
}

func (distributedCache *DistributedCache) SetNextComponent(nextComponent internal.Component) {
	distributedCache.BaseComponent.SetNextComponent(nextComponent)
}

// Start initializes the DistributedCache component without blocking the pipeline.
func (dc *DistributedCache) Start(ctx context.Context) error {
	log.Trace("DistributedCache::Start : Starting component %s", dc.Name())

	// Get and validate storage component
	storageComponent, ok := internal.GetStorageComponent().(*azstorage.AzStorage)
	if !ok || storageComponent.GetBlobStorage() == nil {
		return logAndReturnError("DistributedCache::Start : error [invalid or missing storage component]")
	}
	dc.storage = storageComponent.GetBlobStorage()
	cacheDir := "__CACHE__" + dc.cacheID

	// Check and create cache directory if needed
	if err := dc.setupCacheStructure(cacheDir); err != nil {
		return err
	}

	log.Info("DistributedCache::Start : Cache structure setup completed")
	dc.heartbeatManager = NewHeartbeatManager(dc.cachePath, dc.storage, dc.hbDuration, "__CACHE__"+dc.cacheID)
	dc.heartbeatManager.Start()
	// dc.CreateMetadataFile(cacheDir, "metadatafile1.json.md")
	// dc.UpdateMetaParallel(cacheDir)
	dc.MixedMetaParallel(cacheDir)
	return nil

}

func (dc *DistributedCache) CreateMetaParallel(cacheDir string) error {
	startTime := time.Now()
	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Number of goroutines to spawn
	numGoroutines := 100

	// Create 50 goroutines and run them in parallel to call CreateMetadataFile
	for i := range numGoroutines {
		wg.Add(1) // Increment the wait group counter
		go func(i int) {
			defer wg.Done() // Decrement the counter when the goroutine finishes

			// Generate a unique file name for each goroutine
			fileName := fmt.Sprintf("metadata_%d.md", i)

			// Call CreateMetadataFile
			err := dc.CreateMetadataFile(cacheDir, fileName)
			if err != nil {
				logAndReturnError("failed to create metadata file")
			}

		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Record the total time taken
	totalTime := time.Since(startTime)
	fmt.Printf("CreateMetadatFile :: Total time taken for %d goroutines: %v\n", numGoroutines, totalTime)

	return nil
}

func (dc *DistributedCache) MixedMetaParallel(cacheDir string) error {
	startTime := time.Now()
	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Number of goroutines to spawn
	numGoroutines := 7

	// Create 50 goroutines and run them in parallel to call CreateMetadataFile
	for i := range numGoroutines {
		wg.Add(1) // Increment the wait group counter
		go func(i int) {
			defer wg.Done() // Decrement the counter when the goroutine finishes

			// Generate a unique file name for each goroutine
			fileName := fmt.Sprintf("metadata_1.md")
			offsetname := fmt.Sprintf("ABCDEF%d", i)
			dummyReplica := map[string]interface{}{
				"offset":      offsetname,
				"size":        "1024",
				"num-stripes": "4",
				"stripe-size": "256",
				"nodes":       []string{"node1", "node2"},
			}
			err := dc.UpdateMetadataFile(cacheDir, fileName, dummyReplica)
			if err != nil {
				logAndReturnError("failed to create metadata file")
			}

		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Record the total time taken
	totalTime := time.Since(startTime)
	fmt.Printf("UpdateMetadatFile :: Total time taken for %d goroutines: %v\n", numGoroutines, totalTime)

	return nil
}

func (dc *DistributedCache) UpdateMetaParallel(cacheDir string) error {
	startTime := time.Now()
	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Number of goroutines to spawn
	numGoroutines := 100

	// Create 50 goroutines and run them in parallel to call CreateMetadataFile
	for i := range numGoroutines {
		wg.Add(1) // Increment the wait group counter
		go func(i int) {
			defer wg.Done() // Decrement the counter when the goroutine finishes

			// Generate a unique file name for each goroutine
			fileName := fmt.Sprintf("metadata_%d.md", i)
			offsetname := fmt.Sprintf("abcdef%d", i)
			dummyReplica := map[string]interface{}{
				"offset":      offsetname,
				"size":        "1024",
				"num-stripes": "4",
				"stripe-size": "256",
				"nodes":       []string{"node1", "node2"},
			}
			err := dc.UpdateMetadataFile(cacheDir, fileName, dummyReplica)
			if err != nil {
				logAndReturnError("failed to create metadata file")
			}

		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Record the total time taken
	totalTime := time.Since(startTime)
	fmt.Printf("UpdateMetadatFile :: Total time taken for %d goroutines: %v\n", numGoroutines, totalTime)

	return nil
}

// setupCacheStructure checks and creates necessary cache directories and metadata.
// It's doing 4 rest api calls, 3 for directory and 1 for creator file.+1 call to check the creator file
func (dc *DistributedCache) setupCacheStructure(cacheDir string) error {
	_, err := dc.storage.GetAttr(cacheDir + "/creator.txt")
	if err != nil {
		directories := []string{cacheDir, cacheDir + "/Nodes", cacheDir + "/Objects"}
		for _, dir := range directories {
			if err := dc.storage.CreateDirectory(dir, true); err != nil {

				// If I will check directly the creator file and if it is not created then I am kind of loop to check the file again and again Rather than that I have added the call to create remaining directory structure if the one is failed
				if bloberror.HasCode(err, bloberror.BlobAlreadyExists) {
					continue
				} else if err != nil {
					return logAndReturnError(fmt.Sprintf("DistributedCache::Start error [failed to create directory %s: %v]", dir, err))
				}
			}
		}

		// Add metadata file with VM IP
		ip, err := getVmIp()
		if err != nil {
			return logAndReturnError(fmt.Sprintf("DistributedCache::Start error [failed to get VM IP: %v]", err))
		}
		if err := dc.storage.WriteFromBuffer(internal.WriteFromBufferOptions{Name: cacheDir + "/creator.txt",
			Data: []byte(ip),
			Etag: true}); err != nil {
			if !bloberror.HasCode(err, bloberror.BlobAlreadyExists) {
				return logAndReturnError(fmt.Sprintf("DistributedCache::Start error [failed to create creator file: %v]", err))
			} else {
				return nil
			}
		}
		err = dc.CreateMetadataFile(cacheDir, "metadatafile1.json.md")
		if err != nil {
			return logAndReturnError(fmt.Sprintf("DistributedCache::Start error [failed to create metadata file: %v]", err))
		}
	}
	return nil
}

// logAndReturnError logs the error and returns it.
func logAndReturnError(msg string) error {
	log.Err(msg)
	return fmt.Errorf(msg)
}

// Create Metadata file for every new file created
func (dc *DistributedCache) CreateMetadataFile(cacheDir string, fileName string) error {
	// Open the example.json file
	sourceFile, err := os.ReadFile("../../setup/azure-storage-fuse/setup/example.json")
	if err != nil {
		log.Err("CreateMetadataFile: Failed to open source file: ", err)
		return err
	}
	if err := dc.storage.WriteFromBuffer(internal.WriteFromBufferOptions{Name: cacheDir + "/Objects/" + fileName,
		Data:    sourceFile,
		Etag:    true,
		EtagVal: ""}); err != nil {
		return logAndReturnError(fmt.Sprintf("DistributedCache:: Failed to create metadatafile: %v]", err))
	}
	return nil
}

// ReadMetadataFile reads the metadata file from the cache directory
func (dc *DistributedCache) ReadMetadataFile(cacheDir string, fileName string) ([]byte, error) {
	fileContent, err := dc.storage.ReadBuffer(cacheDir+"/"+fileName, 0, 0)
	if err != nil {
		logAndReturnError(fmt.Sprintf("DistributedCache:: Failed to read metadatafile %s: %v", fileName, err))
		return nil, err
	}
	return fileContent, nil
}

// UpdateMetadataFile updates the replicas section in the JSON file
func (dc *DistributedCache) UpdateMetadataFile(cacheDir string, fileName string, newReplica map[string]interface{}) error {
	retryCount := 5
	success := false
	var err error
	for i := range retryCount {
		attr, err := dc.storage.GetAttr(cacheDir + "/Objects/" + fileName)
		if err != nil {
			logAndReturnError(fmt.Sprintf("DistributedCache:: Failed to get file attributes: %v", err))
			return err
		}
		fileContent, err := dc.storage.ReadBuffer(cacheDir+"/"+fileName, 0, 0)
		if err != nil {
			logAndReturnError(fmt.Sprintf("DistributedCache:: Failed to read metadatafile %s: %v", fileName, err))
			return err
		}
		// Parse the JSON content into a map
		var jsonData map[string]any
		err = json.Unmarshal(fileContent, &jsonData)
		if err != nil {
			logAndReturnError(fmt.Sprintf("DistributedCache:: Failed to unmarshal JSON: %v", err))
			return err
		}

		// Access and update the replicas section
		layout, ok := jsonData["layout"].([]any)
		if !ok || len(layout) == 0 {
			logAndReturnError("DistributedCache:: Failed to find layout section in JSON")
			return err
		}

		// Assuming the first layout object contains the replicas section
		var firstLayout map[string]any
		for _, layoutItem := range layout {
			layoutMap, ok := layoutItem.(map[string]any)
			if !ok {
				continue
			}
			if layoutMap["chunk-id"] == newReplica["chunk-id"] { //TODO:: take chunk id as input?
				firstLayout = layoutMap
				break
			}
		}
		if firstLayout == nil {
			logAndReturnError("DistributedCache:: Failed to find layout section with the specified chunk-id in JSON")
			return err
		}

		firstLayout["replicas"] = newReplica

		// Update the layout back in the JSON data
		jsonData["layout"] = layout

		// Marshal the updated JSON back to a string
		updatedContent, err := json.MarshalIndent(jsonData, "", "    ")
		if err != nil {
			logAndReturnError(fmt.Sprintf("DistributedCache:: Failed to marshal updated JSON: %v", err))
			return err
		}

		// Replace the original file with the temporary file
		err = dc.storage.WriteFromBuffer(internal.WriteFromBufferOptions{Name: cacheDir + "/" + fileName, Data: []byte(updatedContent), Etag: true, EtagVal: attr.ETag})
		if err != nil {
			if bloberror.HasCode(err, bloberror.ConditionNotMet) {
				log.Warn("DistributedCache:: WriteFromBuffer failed due to ETag mismatch, retrying... Attempt %d/%d", i+1, retryCount)
				time.Sleep(5 * time.Second)
				continue
			}
			logAndReturnError(fmt.Sprintf("failed to write updated JSON to file: %v", err))
			return err
			// If successful, break out of the loop
		} else {
			success = true
			break
		}
	}

	// If retries exhausted and still failed, return an error
	if !success {
		logAndReturnError(fmt.Sprintf("failed to write updated JSON to file after %d attempts: %v", retryCount, err))
		return err
	}

	fmt.Printf("Metadata file updated successfully: %s\n", fileName)
	return err
}

// Stop : Stop the component functionality and kill all threads started
func (dc *DistributedCache) Stop() error {
	log.Trace("DistributedCache::Stop : Stopping component %s", dc.Name())
	if dc.heartbeatManager != nil {
		dc.heartbeatManager.Stop()
	}
	return nil
}

// Configure : Pipeline will call this method after constructor so that you can read config and initialize yourself
//
//	Return failure if any config is not valid to exit the process
func (distributedCache *DistributedCache) Configure(_ bool) error {
	log.Trace("DistributedCache::Configure : %s", distributedCache.Name())

	conf := DistributedCacheOptions{}
	err := config.UnmarshalKey(distributedCache.Name(), &conf)
	if err != nil {
		log.Err("DistributedCache::Configure : config error [invalid config attributes]")
		return fmt.Errorf("DistributedCache: config error [invalid config attributes]")
	}
	if config.IsSet(compName + ".cache-id") {
		distributedCache.cacheID = conf.CacheID
	} else {
		log.Err("DistributedCache: config error [cache-id not set]")
		return fmt.Errorf("config error in %s error [cache-id not set]", distributedCache.Name())
	}

	if config.IsSet(compName + ".path") {
		distributedCache.cachePath = conf.CachePath
	} else {
		log.Err("DistributedCache: config error [cache-path not set]")
		return fmt.Errorf("config error in %s error [cache-path not set]", distributedCache.Name())
	}

	distributedCache.replicas = REPLICAS
	if config.IsSet(compName + ".replicas") {
		distributedCache.replicas = conf.Replicas
	}

	distributedCache.hbDuration = HeartBeatDuration
	if config.IsSet(compName + ".heartbeat-duration") {
		distributedCache.hbDuration = conf.HeartbeatDuration
	}

	return nil
}

// OnConfigChange : If component has registered, on config file change this method is called
func (distributedCache *DistributedCache) OnConfigChange() {
}

// ------------------------- Factory -------------------------------------------

// Pipeline will call this method to create your object, initialize your variables here
// << DO NOT DELETE ANY AUTO GENERATED CODE HERE >>
func NewDistributedCacheComponent() internal.Component {
	comp := &DistributedCache{}
	comp.SetName(compName)
	return comp
}

// On init register this component to pipeline and supply your constructor
func init() {
	internal.AddComponent(compName, NewDistributedCacheComponent)

	cacheID := config.AddStringFlag("cache-id", "blobfuse", "Cache ID for the distributed cache")
	config.BindPFlag(compName+".cache-id", cacheID)

	cachePath := config.AddStringFlag("cache-dir", "/tmp", "Path to the cache")
	config.BindPFlag(compName+".path", cachePath)

	chunkSize := config.AddUint64Flag("chunk-size", 1024*1024, "Chunk size for the cache")
	config.BindPFlag(compName+".chunk-size", chunkSize)

	cacheSize := config.AddUint64Flag("cache-size", 1024*1024*1024, "Cache size for the cache")
	config.BindPFlag(compName+".cache-size", cacheSize)

	replicas := config.AddUint32Flag("replicas", 3, "Number of replicas for the cache")
	config.BindPFlag(compName+".replicas", replicas)

	heartbeatTimeout := config.AddUint32Flag("heartbeat-timeout", 30, "Heartbeat timeout for the cache")
	config.BindPFlag(compName+".heartbeat-timeout", heartbeatTimeout)

	heartbeatDuration := config.AddUint32Flag("heartbeat-duration", HeartBeatDuration, "Heartbeat duration for the cache")
	config.BindPFlag(compName+".heartbeat-duration", heartbeatDuration)

	missedHB := config.AddUint32Flag("heartbeats-till-node-down", 3, "Heartbeat absence for the cache")
	config.BindPFlag(compName+".heartbeats-till-node-down", missedHB)
}
