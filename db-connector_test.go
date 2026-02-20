package shuffle

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock Datastore Client
type mockDatastoreClient struct {
	mock.Mock
	data map[string][]byte // In-memory store: key.String() -> marshaled data
}

func (m *mockDatastoreClient) Put(ctx context.Context, key *datastore.Key, val interface{}) (*datastore.Key, error) {
	args := m.Called(ctx, key, val)
	if m.data == nil {
		m.data = make(map[string][]byte)
	}
	// Simulate storing the data
	byteVal, err := json.Marshal(val)
	if err != nil {
		return nil, fmt.Errorf("mockDatastoreClient: failed to marshal val: %v", err)
	}
	m.data[key.String()] = byteVal
	//log.Printf("[TEST_DEBUG] MockDatastore Put: key=%s, val=%s", key.String(), string(byteVal))
	return args.Get(0).(*datastore.Key), args.Error(1)
}

func (m *mockDatastoreClient) Get(ctx context.Context, key *datastore.Key, val interface{}) error {
	args := m.Called(ctx, key, val)
	//log.Printf("[TEST_DEBUG] MockDatastore Get attempt: key=%s", key.String())
	if data, ok := m.data[key.String()]; ok {
		//log.Printf("[TEST_DEBUG] MockDatastore Get: key=%s, found data=%s", key.String(), string(data))
		err := json.Unmarshal(data, val)
		if err != nil {
			return fmt.Errorf("mockDatastoreClient: failed to unmarshal data for key %s: %v", key.String(), err)
		}
		return args.Error(0)
	}
	//log.Printf("[TEST_DEBUG] MockDatastore Get: key=%s NOT FOUND", key.String())
	return datastore.ErrNoSuchEntity
}

// Mock Storage Client parts
type mockStorageClient struct {
	mock.Mock
	bucketData map[string]map[string][]byte // bucketName -> objectPath -> data
	uploadError error
	downloadError error
}

type mockBucketHandle struct {
	mock.Mock
	client *mockStorageClient
	bucketName string
}

type mockObjectHandle struct {
	mock.Mock
	client *mockStorageClient
	bucketName string
	objectPath string
}

type mockStorageWriter struct {
	mock.Mock
	client *mockStorageClient
	bucketName string
	objectPath string
	buffer strings.Builder
}

type mockStorageReader struct {
	io.ReadCloser
	client *mockStorageClient
	bucketName string
	objectPath string
	reader io.Reader
	closed bool
}

func (m *mockStorageClient) Bucket(name string) *mockBucketHandle {
	// For now, not using m.Called as it's a chain starter
	return &mockBucketHandle{client: m, bucketName: name}
}

func (b *mockBucketHandle) Object(path string) *mockObjectHandle {
	// For now, not using b.Called
	return &mockObjectHandle{client: b.client, bucketName: b.bucketName, objectPath: path}
}

func (o *mockObjectHandle) NewWriter(ctx context.Context) *mockStorageWriter {
	o.Called(ctx)
	return &mockStorageWriter{client: o.client, bucketName: o.bucketName, objectPath: o.objectPath}
}

func (w *mockStorageWriter) Write(p []byte) (n int, err error) {
	args := w.Called(p)
	if w.client.uploadError != nil {
		return 0, w.client.uploadError
	}
	n, err = w.buffer.Write(p)
	return n, args.Error(1) // Allow mocking specific write errors if needed
}

func (w *mockStorageWriter) Close() error {
	args := w.Called()
	if w.client.uploadError != nil { // Check general upload error on close too
		return w.client.uploadError
	}
	if w.client.bucketData == nil {
		w.client.bucketData = make(map[string]map[string][]byte)
	}
	if _, ok := w.client.bucketData[w.bucketName]; !ok {
		w.client.bucketData[w.bucketName] = make(map[string][]byte)
	}
	w.client.bucketData[w.bucketName][w.objectPath] = []byte(w.buffer.String())
	//log.Printf("[TEST_DEBUG] GCS Mock Write Close: bucket=%s, path=%s, data_len=%d", w.bucketName, w.objectPath, len(w.buffer.String()))
	return args.Error(0)
}

func (o *mockObjectHandle) NewReader(ctx context.Context) (*mockStorageReader, error) {
	args := o.Called(ctx)
	if o.client.downloadError != nil {
		return nil, o.client.downloadError
	}
	data, ok := o.client.bucketData[o.bucketName][o.objectPath]
	if !ok {
		//log.Printf("[TEST_DEBUG] GCS Mock NewReader: object NOT FOUND bucket=%s, path=%s", o.bucketName, o.objectPath)
		return nil, storage.ErrObjectNotExist
	}
	//log.Printf("[TEST_DEBUG] GCS Mock NewReader: object FOUND bucket=%s, path=%s, data_len=%d", o.bucketName, o.objectPath, len(data))
	return &mockStorageReader{reader: strings.NewReader(string(data)), client: o.client, bucketName: o.bucketName, objectPath: o.objectPath}, args.Error(1)
}

func (r *mockStorageReader) Read(p []byte) (n int, err error) {
	// args := r.client.Called(p) // Not directly mocking Read on client for simplicity
	if r.client.downloadError != nil { // check general download error
		return 0, r.client.downloadError
	}
	return r.reader.Read(p)
}

func (r *mockStorageReader) Close() error {
	// args := r.client.Called() // Not directly mocking Close on client
	r.closed = true
	return nil // Assuming close doesn't fail unless part of downloadError
}


// Helper to generate large string
func generateLargeString(size int) string {
	return strings.Repeat("a", size)
}

// Helper to create CacheKeyData
func newTestCacheKeyData(key, orgId, value, category string) CacheKeyData {
	return CacheKeyData{
		OrgId:    orgId,
		Key:      key,
		Value:    value,
		Category: category,
		Created:  time.Now().Unix(),
		Edited:   time.Now().Unix(),
	}
}

func TestSetAndGetDatastoreKey(t *testing.T) {
	ctx := context.Background()
	testOrgID := "test-org-1"
	testBucketName := "test-bucket"

	// Backup original project and defer restoration
	oldProject := project
	defer func() { project = oldProject }()

	// Initialize mocks
	mockDS := &mockDatastoreClient{data: make(map[string][]byte)}
	mockGCS := &mockStorageClient{bucketData: make(map[string]map[string][]byte)}

	project = ShuffleStorage{
		Dbclient:      mockDS,
		StorageClient: mockGCS,
		BucketName:    testBucketName,
		DbType:        "datastore", // Assuming datastore, can be parameterized if needed
		CacheDb:       false,       // Disable DB cache for these unit tests for simplicity
	}

	//maxCacheSize is global, ensure it's set (it is in db-connector.go)
	if maxCacheSize == 0 {
		maxCacheSize = 1020000 // Default from db-connector.go
	}

	smallValue := "This is a small value"
	largeValue := generateLargeString(maxCacheSize + 100)

	// --- Test Cases ---
	tests := []struct {
		name          string
		keyData       CacheKeyData
		gcsUploadErr  error
		gcsDownloadErr error
		expectGCSWrite bool
		expectGCSRetrieval bool
		expectSetError bool
		expectGetError bool
	}{
		{
			name:           "Small value",
			keyData:        newTestCacheKeyData("smallkey1", testOrgID, smallValue, "default"),
			expectGCSWrite: false,
			expectGCSRetrieval: false,
		},
		{
			name:           "Large value - good path",
			keyData:        newTestCacheKeyData("largekey1", testOrgID, largeValue, "default"),
			expectGCSWrite: true,
			expectGCSRetrieval: true,
		},
		{
			name: "Large value - GCS Upload Error",
			keyData: newTestCacheKeyData("largekey_upload_err", testOrgID, largeValue, "default"),
			gcsUploadErr: fmt.Errorf("simulated GCS upload error"),
			expectGCSWrite: true, // It will attempt to write
			expectSetError: true,
			expectGCSRetrieval: false, // Get won't happen if set fails
		},
		{
			name: "Large value - GCS Download Error",
			keyData: newTestCacheKeyData("largekey_download_err", testOrgID, largeValue, "default"), // Will be replaced by GCS path during set
			gcsDownloadErr: fmt.Errorf("simulated GCS download error"),
			expectGCSWrite: true, // Set should succeed
			expectGCSRetrieval: true, // Get will attempt retrieval
			// Current GetDatastoreKey logs error but returns placeholder, so no expectGetError
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Reset mock states for each test run
			mockDS.data = make(map[string][]byte) // Clear datastore mock
			mockGCS.bucketData = make(map[string]map[string][]byte) // Clear GCS mock
			mockGCS.uploadError = tc.gcsUploadErr
			mockGCS.downloadError = tc.gcsDownloadErr

			// Mock expectations for Datastore Put
			dsKeyName := fmt.Sprintf("%s_%s", tc.keyData.OrgId, tc.keyData.Key)
			if tc.keyData.Category != "" && tc.keyData.Category != "default" {
				dsKeyName = fmt.Sprintf("%s_%s", dsKeyName, tc.keyData.Category)
			}
			dsKeyName = url.QueryEscape(dsKeyName)
			if len(dsKeyName) > 127 {
				dsKeyName = dsKeyName[:127]
			}

			expectedDSKey := datastore.NameKey("org_cache", dsKeyName, nil)

			// --- Test SetDatastoreKey ---
			// Setup mock expectations for Put
			// We only expect Put if there's no SetError expected OR if GCS upload fails but it still tries to write original (not current code)
			if !tc.expectSetError {
				mockDS.On("Put", ctx, expectedDSKey, mock.AnythingOfType("*shuffle.CacheKeyData")).Return(expectedDSKey, nil).Once()
			}


			errSet := SetDatastoreKey(ctx, tc.keyData)

			if tc.expectSetError {
				assert.Error(t, errSet, "SetDatastoreKey should have returned an error")
				if tc.gcsUploadErr != nil && errSet != nil {
					assert.Contains(t, errSet.Error(), tc.gcsUploadErr.Error(), "SetDatastoreKey error should contain GCS upload error")
				}
				mockDS.AssertNotCalled(t, "Put", ctx, expectedDSKey, mock.AnythingOfType("*shuffle.CacheKeyData"))
				return // Skip Get part if Set failed as expected
			} else {
				assert.NoError(t, errSet, "SetDatastoreKey should not have returned an error")
			}

			// Verify Datastore Put
			if !tc.expectSetError {
				mockDS.AssertCalled(t, "Put", ctx, expectedDSKey, mock.AnythingOfType("*shuffle.CacheKeyData"))

				// Verify GCS write
				gcsPath := fmt.Sprintf("large_cache_values/%s/%s", tc.keyData.OrgId, tc.keyData.Key)
				if tc.expectGCSWrite {
					_, exists := mockGCS.bucketData[testBucketName][gcsPath]
					assert.True(t, exists, "Expected value to be written to GCS")
					if exists {
						assert.Equal(t, tc.keyData.Value, string(mockGCS.bucketData[testBucketName][gcsPath]), "GCS data mismatch")
					}

					// Check that datastore has placeholder
					var dsStoredData CacheKeyData
					dsRawData, dsOk := mockDS.data[expectedDSKey.String()]
					assert.True(t, dsOk, "Data not found in mock datastore after Set")
					if dsOk {
						err := json.Unmarshal(dsRawData, &dsStoredData)
						assert.NoError(t, err, "Failed to unmarshal data from mock datastore")
						assert.Equal(t, fmt.Sprintf("GCS_VALUE:%s", gcsPath), dsStoredData.Value, "Datastore should have GCS placeholder")
					}

				} else {
					_, exists := mockGCS.bucketData[testBucketName][gcsPath]
					assert.False(t, exists, "Value should NOT have been written to GCS")
					// Check that datastore has original value
					var dsStoredData CacheKeyData
					dsRawData, dsOk := mockDS.data[expectedDSKey.String()]
					assert.True(t, dsOk, "Data not found in mock datastore after Set")
					if dsOk {
						err := json.Unmarshal(dsRawData, &dsStoredData)
						assert.NoError(t, err, "Failed to unmarshal data from mock datastore")
						assert.Equal(t, tc.keyData.Value, dsStoredData.Value, "Datastore should have original value")
					}
				}
			}
			mockDS.Mock.AssertExpectations(t) // Reset expectations for Get
			mockDS.ExpectedCalls = nil


			// --- Test GetDatastoreKey ---
			// Prime datastore for Get (it's already primed by Set if successful)
			// Mock expectations for Get
			mockDS.On("Get", ctx, expectedDSKey, mock.AnythingOfType("*shuffle.CacheKeyData")).Return(nil).Once()


			retrievedData, errGet := GetDatastoreKey(ctx, dsKeyName, tc.keyData.Category)

			if tc.expectGetError {
				assert.Error(t, errGet, "GetDatastoreKey should have returned an error")
			} else {
				assert.NoError(t, errGet, "GetDatastoreKey should not have returned an error")
				if retrievedData != nil {
					if tc.expectGCSRetrieval && tc.gcsDownloadErr != nil {
						// If download error, current code returns placeholder
						assert.True(t, strings.HasPrefix(retrievedData.Value, "GCS_VALUE:"), "Expected GCS placeholder on download error")
					} else {
						assert.Equal(t, tc.keyData.Value, retrievedData.Value, "Retrieved value mismatch")
					}
					assert.Equal(t, tc.keyData.OrgId, retrievedData.OrgId)
					assert.Equal(t, tc.keyData.Key, retrievedData.Key)
				} else {
					assert.NotNil(t, retrievedData, "Retrieved data should not be nil")
				}
			}
			mockDS.AssertExpectations(t)
		})
	}
}

func init() {
	// Disable actual logging for tests unless debugging specific test issues
	//log.SetOutput(ioutil.Discard)

	// Setup project for testing if needed globally, or do it per test / suite
	// For now, doing it per test in TestSetAndGetDatastoreKey
}

[end of db-connector_test.go]
