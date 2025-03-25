package testapp

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetaDate(t *testing.T) {
	err := os.Chdir("/home/anubhuti/mntdir")
	require.NoError(t, err, "Failed to change directory")
	err = os.WriteFile("metadatafile1.json", []byte(`{"name": "test"}`), 0644)
	require.NoError(t, err, "Failed to create test file")
	CreateMetadataFile("metadatafile1.json")
	// check if the file is creatged
	_, err = os.Stat("metadatafile1.json.md")
	require.NoError(t, err, "Metadata file should be created")
}

func TestUpdateMetadata(t *testing.T) {
	err := os.Chdir("/home/anubhuti/mntdir")
	require.NoError(t, err, "Failed to change directory")
	dummyReplica := map[string]interface{}{
		"offset":      "12345",
		"size":        "1024",
		"num-stripes": "4",
		"stripe-size": "256",
		"nodes":       []string{"node1", "node2"},
	}
	UpdateMetadataFile("metadatafile1.json.md", dummyReplica)
	// check if the file is updated
	metadataFile, err := os.ReadFile("metadatafile1.json.md")
	require.NoError(t, err, "Failed to read metadata file")
	require.Contains(t, string(metadataFile), `"offset":"12345"`, "Metadata file should be updated")
	require.Contains(t, string(metadataFile), `"size":"1024"`, "Metadata file should be updated")
	require.Contains(t, string(metadataFile), `"num-stripes":"4"`, "Metadata file should be updated")
	require.Contains(t, string(metadataFile), `"stripe-size":"256"`, "Metadata file should be updated")
	require.Contains(t, string(metadataFile), `"nodes":["node1","node2"]`, "Metadata file should be updated")

}

// write a tes
