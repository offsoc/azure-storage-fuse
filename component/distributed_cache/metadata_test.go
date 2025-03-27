package distributed_cache

import (
	"fmt"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type metadataTestSuite struct {
	suite.Suite
	assert           *assert.Assertions
	distributedCache *DistributedCache
}

func (suite *metadataTestSuite) TestWithGoRoutines() error {
	// cmd := exec.Command("bash", "-c", "../../blobfuse2", "/home/anubhuti/mntdir")
	// _, err := cmd.Output()
	// if err != nil {
	// 	suite.assert.Fail("Failed to run command: %s", err)
	// }
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

	cmd := exec.Command("bash", "-c", "../../blobfuse2", "unmount", "all")
	_, err := cmd.Output()
	if err != nil {
		suite.assert.Fail("Failed to run command: %s", err)
	}
	return nil
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestMetadataTestSuite(t *testing.T) {

	suite.Run(t, new(metadataTestSuite))
}
