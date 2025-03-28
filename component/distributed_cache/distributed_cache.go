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
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
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
type DistributedCache struct {
	internal.BaseComponent
	cacheID      string
	cachePath    string
	maxCacheSize uint64
	replicas     uint8
	maxMissedHbs uint8
	hbDuration   uint16

	heartbeatManager *HeartbeatManager
}

// Structure defining your config parameters
type DistributedCacheOptions struct {
	CacheID             string `config:"cache-id" yaml:"cache-id,omitempty"`
	CachePath           string `config:"path" yaml:"path,omitempty"`
	ChunkSize           uint64 `config:"chunk-size" yaml:"chunk-size,omitempty"`
	MaxCacheSize        uint64 `config:"max-cache-size" yaml:"cache-size,omitempty"`
	Replicas            uint8  `config:"replicas" yaml:"replicas,omitempty"`
	HeartbeatDuration   uint16 `config:"heartbeat-duration" yaml:"heartbeat-duration,omitempty"`
	MaxMissedHeartbeats uint8  `config:"max-missed-heartbeats" yaml:"max-missed-heartbeats,omitempty"`
}

const (
	compName                 = "distributed_cache"
	defaultHeartBeatDuration = 30
	defaultReplicas          = 3
	defaultMaxMissedHBs      = 1
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

	cacheDir := "__CACHE__" + dc.cacheID

	// Check and create cache directory if needed
	if err := dc.setupCacheStructure(cacheDir); err != nil {
		return err
	}

	log.Info("DistributedCache::Start : Cache structure setup completed")
	dc.heartbeatManager = &HeartbeatManager{cachePath: dc.cachePath,
		comp:         dc.NextComponent(),
		hbDuration:   dc.hbDuration,
		hbPath:       "__CACHE__" + dc.cacheID,
		maxCacheSize: dc.maxCacheSize,
		maxMissedHbs: dc.maxMissedHbs,
	}
	dc.heartbeatManager.Start()
	// dc.CreateMetadataFile(cacheDir, "metadatafile1.json.md")
	// dc.UpdateMetaParallel(cacheDir)
	dc.OpenMetaParallel(cacheDir)
	// dc.OpenMetadataFile(cacheDir, "metadata_1.md")
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

func (dc *DistributedCache) OpenMetaParallel(cacheDir string) error {
	startTime := time.Now()
	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Number of goroutines to spawn
	numGoroutines := 20

	// Create 50 goroutines and run them in parallel to call CreateMetadataFile
	for i := range numGoroutines {
		wg.Add(1) // Increment the wait group counter
		go func(i int) {
			defer wg.Done() // Decrement the counter when the goroutine finishes

			// Generate a unique file name for each goroutine
			fileName := "metadata_1.md"

			// Call CreateMetadataFile
			fileContent, err := dc.OpenMetadataFile(cacheDir, fileName)
			if err != nil {
				logAndReturnError("failed to create metadata file")
			}
			// check if file is not empty
			if fileContent == nil {
				logAndReturnError("file is empty")
			}

		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Record the total time taken
	totalTime := time.Since(startTime)
	log.Info("CreateMetadatFile :: Total time taken for %d goroutines: %v\n", numGoroutines, totalTime)

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
	_, err := dc.NextComponent().GetAttr(internal.GetAttrOptions{Name: cacheDir + "/creator.txt"})
	if err != nil {
		if os.IsNotExist(err) || err == syscall.ENOENT {
			directories := []string{cacheDir, cacheDir + "/Nodes", cacheDir + "/Objects"}
			for _, dir := range directories {
				if err := dc.NextComponent().CreateDir(internal.CreateDirOptions{Name: dir, Etag: true}); err != nil {

					if !bloberror.HasCode(err, bloberror.BlobAlreadyExists) {
						return logAndReturnError(fmt.Sprintf("DistributedCache::Start error [failed to create directory %s: %v]", dir, err))
					}
				}
			}

			// Add metadata file with VM IP
			ip, err := getVmIp()
			if err != nil {
				return logAndReturnError(fmt.Sprintf("DistributedCache::Start error [failed to get VM IP: %v]", err))
			}
			if err := dc.NextComponent().WriteFromBuffer(internal.WriteFromBufferOptions{Name: cacheDir + "/creator.txt",
				Data: []byte(ip),
				Etag: true}); err != nil {
				if !bloberror.HasCode(err, bloberror.BlobAlreadyExists) {
					return logAndReturnError(fmt.Sprintf("DistributedCache::Start error [failed to create creator file: %v]", err))
				} else {
					return nil
				}
			}
		} else {
			return logAndReturnError(fmt.Sprintf("DistributedCache::Start error [failed to read creator file: %v]", err))
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
	return errors.New(msg)
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
func (dc *DistributedCache) OpenMetadataFile(cacheDir string, fileName string) ([]byte, error) {
	// retryCount := 10000 // TODO:: This is the limit for the numbr of nodes having simultaneous access
	success := false
	var err error
	var updatedContent []byte
	for !success {
		attr, err := dc.storage.GetAttr(cacheDir + "/" + fileName)
		if err != nil {
			logAndReturnError(fmt.Sprintf("DistributedCache:: Failed to get file attributes: %v", err))
			return nil, err
		}
		fileContent, err := dc.storage.ReadBuffer(cacheDir+"/"+fileName, 0, 0)
		if err != nil {
			logAndReturnError(fmt.Sprintf("DistributedCache:: Failed to read metadatafile %s: %v", fileName, err))
			return nil, err
		}
		// Parse the JSON content into a map
		var jsonData map[string]any
		err = json.Unmarshal(fileContent, &jsonData)
		if err != nil {
			logAndReturnError(fmt.Sprintf("DistributedCache:: Failed to unmarshal JSON: %v", err))
			return nil, err
		}

		if openCount, ok := jsonData["open-count"].(float64); ok {
			jsonData["open-count"] = openCount + 1 // Update the "open-fd" parameter
		} else {
			logAndReturnError("DistributedCache:: Failed to cast 'open-count' to int")
			return nil, fmt.Errorf("invalid type for 'open-count'")
		}

		// Marshal the updated JSON back to a string
		updatedContent, err = json.MarshalIndent(jsonData, "", "    ")
		if err != nil {
			logAndReturnError(fmt.Sprintf("DistributedCache:: Failed to marshal updated JSON: %v", err))
			return nil, err
		}

		// Replace the original file with the temporary file
		err = dc.storage.WriteFromBuffer(internal.WriteFromBufferOptions{Name: cacheDir + "/" + fileName, Data: []byte(updatedContent), Etag: true, EtagVal: attr.ETag})
		if err != nil {
			if bloberror.HasCode(err, bloberror.ConditionNotMet) {
				log.Warn("DistributedCache:: WriteFromBuffer failed due to ETag mismatch, retrying...")
				time.Sleep(5 * time.Millisecond)
				continue
			} else {
				logAndReturnError(fmt.Sprintf("failed to write updated JSON to file: %v", err))
				return nil, err
			}
			// If successful, break out of the loop
		} else {
			success = true
			break
		}
	}

	// If retries exhausted and still failed, return an error
	if !success {
		logAndReturnError(fmt.Sprintf("failed to write updated JSON to file after %d", err))
		return nil, err
	}
	return updatedContent, nil

}

// UpdateMetadataFile updates the replicas section in the JSON file
// TODO:: Update full json and add normal checks to see to maintains format
func (dc *DistributedCache) UpdateMetadataFile(cacheDir string, fileName string, newReplica map[string]interface{}) error {
	retryCount := 20
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
				time.Sleep(5 * time.Millisecond)
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
		return dc.heartbeatManager.Stop()

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
	if conf.CacheID == "" {
		return fmt.Errorf("config error in %s: [cache-id not set]", distributedCache.Name())
	}
	if conf.CachePath == "" {
		return fmt.Errorf("config error in %s: [cache-path not set]", distributedCache.Name())
	}

	distributedCache.cacheID = conf.CacheID
	distributedCache.cachePath = conf.CachePath
	distributedCache.maxCacheSize = conf.MaxCacheSize
	distributedCache.replicas = defaultReplicas
	if config.IsSet(compName + ".replicas") {
		distributedCache.replicas = conf.Replicas
	}
	distributedCache.hbDuration = defaultHeartBeatDuration
	if config.IsSet(compName + ".heartbeat-duration") {
		distributedCache.hbDuration = conf.HeartbeatDuration
	}
	distributedCache.maxMissedHbs = defaultMaxMissedHBs
	if config.IsSet(compName + ".max-missed-heartbeats") {
		distributedCache.maxMissedHbs = uint8(conf.MaxMissedHeartbeats)
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

	cacheID := config.AddStringFlag("cache-id", "", "Cache ID for the distributed cache")
	config.BindPFlag(compName+".cache-id", cacheID)

	cachePath := config.AddStringFlag("cache-dir", "", "Path to the cache")
	config.BindPFlag(compName+".path", cachePath)

	chunkSize := config.AddUint64Flag("chunk-size", 16*1024*1024, "Chunk size for the cache")
	config.BindPFlag(compName+".chunk-size", chunkSize)

	maxCacheSize := config.AddUint64Flag("max-cache-size", 0, "Cache size for the cache")
	config.BindPFlag(compName+".max-cache-size", maxCacheSize)

	replicas := config.AddUint8Flag("replicas", defaultReplicas, "Number of replicas for the cache")
	config.BindPFlag(compName+".replicas", replicas)

	heartbeatDuration := config.AddUint16Flag("heartbeat-duration", defaultHeartBeatDuration, "Heartbeat duration for the cache")
	config.BindPFlag(compName+".heartbeat-duration", heartbeatDuration)

	missedHB := config.AddUint32Flag("max-missed-heartbeats", 3, "Heartbeat absence for the cache")
	config.BindPFlag(compName+".max-missed-heartbeats", missedHB)
}
