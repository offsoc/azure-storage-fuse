// import azstorage "github.com/Azure/azure-storage-blob-go/azblob"
package testapp

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/Azure/azure-storage-fuse/v2/internal"
)

// Create Metadata file for every new file created
func CreateMetadataFile(fileName string) {
	// Open the example.json file
	sourceFile, err := os.Open("/home/anubhuti/Downloads/clone2/azure-storage-fuse/testapp/example.json")
	if err != nil {
		log.Fatalf("failed to open source file: %v", err)
	}
	defer sourceFile.Close()
	// Create a heartbeat file in storage with <nodeId>.hb
	if err := az.storage.WriteFromBuffer(internal.WriteFromBufferOptions{Name: hbPath, Data: data}); err != nil {
		log.Err("AddHeartBeat: Failed to write heartbeat file: ", err)
		return err
	}
	_, err = io.Copy(file, sourceFile)
	if err != nil {
		log.Fatalf("failed to copy content to file: %v", err)
	}
	fmt.Printf("Metadata file created: %s\n", fileName)
}

// UpdateMetadataFile updates the replicas section in the JSON file
func UpdateMetadataFile(fileName string, newReplica map[string]interface{}) {
	err := os.Chdir("/home/anubhuti/mntdir")
	if err != nil {
		log.Fatalf("failed to change directory: %v", err)
	}
	// Open the JSON file
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	// Read the file content
	fileContent, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("failed to read file: %v", err)
	}

	// Parse the JSON content into a map
	var jsonData map[string]interface{}
	err = json.Unmarshal(fileContent, &jsonData)
	if err != nil {
		log.Fatalf("failed to parse JSON: %v", err)
	}

	// Access and update the replicas section
	layout, ok := jsonData["layout"].([]interface{})
	if !ok || len(layout) == 0 {
		log.Fatalf("layout section is missing or invalid")
	}

	// Assuming the first layout object contains the replicas section
	firstLayout := layout[0].(map[string]interface{})

	firstLayout["replicas"] = newReplica

	// Update the layout back in the JSON data
	jsonData["layout"] = layout

	// Marshal the updated JSON back to a string
	updatedContent, err := json.MarshalIndent(jsonData, "", "    ")
	if err != nil {
		log.Fatalf("failed to marshal updated JSON: %v", err)
	}

	// Write the updated JSON back to the file atomically
	tempFileName := fileName + ".tmp"
	err = os.WriteFile(tempFileName, updatedContent, 0644)
	if err != nil {
		log.Fatalf("failed to write to temporary file: %v", err)
	}

	// Replace the original file with the temporary file
	err = os.Rename(tempFileName, fileName)
	if err != nil {
		log.Fatalf("failed to replace original file: %v", err)
	}

	fmt.Printf("Metadata file updated successfully: %s\n", fileName)
}
