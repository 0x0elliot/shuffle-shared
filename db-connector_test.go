package shuffle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- Mock Datastore Client ---
type mockDatastoreClient struct {
	mock.Mock
	data                map[string][]byte // In-memory store: key.String() -> marshaled CacheKeyData struct
	putError            error
	putErrorKey         *datastore.Key // Specific key for which Put should error
	getError            error
	getErrorKey         *datastore.Key // Specific key for which Get should error
	putCallCount        map[string]int
	getCallCount        map[string]int
	shouldFailSecondPut bool  // Controls if the second Put (with placeholder) should fail
	secondPutError      error // Error for the second Datastore Put attempt (with placeholder)
}

func newMockDatastoreClient() *mockDatastoreClient {
	return &mockDatastoreClient{
		data:         make(map[string][]byte),
		putCallCount: make(map[string]int),
		getCallCount: make(map[string]int),
	}
}

func (m *mockDatastoreClient) Put(ctx context.Context, key *datastore.Key, val interface{}) (*datastore.Key, error) {
	m.Called(ctx, key, val)
	m.putCallCount[key.String()]++

	if m.putCallCount[key.String()] == 1 && m.putError != nil && (m.putErrorKey == nil || m.putErrorKey.String() == key.String()) {
		return key, m.putError
	}

	if m.putCallCount[key.String()] == 2 && m.shouldFailSecondPut && m.secondPutError != nil {
		return key, m.secondPutError
	}

	// Generic error for any other Put call if putError is set and not specific to first/second call
	if m.putError != nil && (m.putErrorKey == nil || m.putErrorKey.String() == key.String()) && !(m.putCallCount[key.String()] <= 2 && (m.shouldFailSecondPut || m.putError != nil)) {
		return key, m.putError
	}

	byteVal, err := json.Marshal(val)
	if err != nil {
		return nil, fmt.Errorf("mockDatastoreClient: failed to marshal val for key %s: %v", key.String(), err)
	}
	m.data[key.String()] = byteVal
	return key, nil
}

func (m *mockDatastoreClient) Get(ctx context.Context, key *datastore.Key, val interface{}) error {
	m.Called(ctx, key, val)
	m.getCallCount[key.String()]++

	if m.getError != nil && (m.getErrorKey == nil || m.getErrorKey.String() == key.String()) {
		return m.getError
	}

	dataBytes, ok := m.data[key.String()]
	if !ok {
		return datastore.ErrNoSuchEntity
	}

	err := json.Unmarshal(dataBytes, val)
	if err != nil {
		return fmt.Errorf("mockDatastoreClient: failed to unmarshal data for key %s: %v. Data: %s", key.String(), err, string(dataBytes))
	}
	return nil
}

func (m *mockDatastoreClient) Reset() {
	m.data = make(map[string][]byte)
	m.putError = nil
	m.putErrorKey = nil
	m.getError = nil
	m.getErrorKey = nil
	m.putCallCount = make(map[string]int)
	m.getCallCount = make(map[string]int)
	m.shouldFailSecondPut = false
	m.secondPutError = nil
	m.Mock.ExpectedCalls = nil
	m.Mock.Calls = nil
}

// Implement other datastore.Client methods if needed by SUT, otherwise testify/mock handles them.
func (m *mockDatastoreClient) GetMulti(ctx context.Context, keys []*datastore.Key, dst interface{}) error { return errors.New("not implemented") }
func (m *mockDatastoreClient) Count(ctx context.Context, q *datastore.Query) (int, error) { return 0, errors.New("not implemented") }
func (m *mockDatastoreClient) Delete(ctx context.Context, key *datastore.Key) error { return errors.New("not implemented") }
func (m *mockDatastoreClient) DeleteMulti(ctx context.Context, keys []*datastore.Key) error { return errors.New("not implemented") }
func (m *mockDatastoreClient) Run(ctx context.Context, q *datastore.Query) *datastore.Iterator { return nil }
func (m *mockDatastoreClient) GetAll(ctx context.Context, q *datastore.Query, dst interface{}) ([]*datastore.Key, error) { return nil, errors.New("not implemented") }


// --- Mock GCS Components ---
type mockGCSWriter struct {
	io.WriteCloser
	objectPath string
	bucketName string
	buffer     strings.Builder
	closed     bool
	writeError error
	closeError error
	gcsStore   *map[string]map[string][]byte // Pointer to the test-case specific GCS store
}

func (w *mockGCSWriter) Write(p []byte) (n int, err error) {
	if w.writeError != nil {
		return 0, w.writeError
	}
	return w.buffer.Write(p)
}

func (w *mockGCSWriter) Close() error {
	if w.closed {
		return errors.New("mockGCSWriter: already closed")
	}
	w.closed = true
	if w.closeError != nil {
		return w.closeError
	}
	if (*w.gcsStore) == nil {
		(*w.gcsStore) = make(map[string]map[string][]byte)
	}
	if _, ok := (*w.gcsStore)[w.bucketName]; !ok {
		(*w.gcsStore)[w.bucketName] = make(map[string][]byte)
	}
	(*w.gcsStore)[w.bucketName][w.objectPath] = []byte(w.buffer.String())
	return nil
}

type mockGCSReader struct {
	io.ReadCloser // Embed io.Closer for the Close method
	reader    io.Reader
	closed    bool
	readError error
}

func newMockGCSReader(data []byte, readErr error) *mockGCSReader {
	return &mockGCSReader{
		reader:    strings.NewReader(string(data)),
		readError: readErr,
	}
}

func (r *mockGCSReader) Read(p []byte) (n int, err error) {
	if r.closed {
		return 0, errors.New("mockGCSReader: reading from closed reader")
	}
	if r.readError != nil {
		return 0, r.readError
	}
	return r.reader.Read(p)
}

func (r *mockGCSReader) Close() error {
	r.closed = true
	return nil
}

// Store original GCS factory functions
var (
	originalNewGCSWriterFunc func(ctx context.Context, o *storage.ObjectHandle) io.WriteCloser
	originalNewGCSReaderFunc func(ctx context.Context, o *storage.ObjectHandle) (io.ReadCloser, error)
)

func TestSetAndGetDatastoreKey_ReactiveGCS(t *testing.T) {
	ctx := context.Background()
	testOrgID := "test-org-gcs"
	testBucketName := "test-bucket-gcs"

	// Save original factory functions
	originalNewGCSWriterFunc = newGCSWriterFunc
	originalNewGCSReaderFunc = newGCSReaderFunc

	// Defer restoration of original factory functions and log output
	defer func() {
		newGCSWriterFunc = originalNewGCSWriterFunc
		newGCSReaderFunc = originalNewGCSReaderFunc
		log.SetOutput(os.Stderr) // Restore default logger output
	}()

	var logBuffer strings.Builder
	log.SetOutput(&logBuffer) // Capture logs

	mockDS := newMockDatastoreClient()
	// gcsBucketData is now test-case local, managed by the factories based on tc.gcsWrite/ReadError

	// Initialize project for testing
	// project.StorageClient is a real client; mocking is done via factory overrides.
	project = ShuffleStorage{
		Dbclient:      mockDS,
		StorageClient: &storage.Client{},
		BucketName:    testBucketName,
		DbType:        "datastore", // Default to datastore, can be changed per test case
		CacheDb:       false,
	}

	if maxCacheSize == 0 {
		maxCacheSize = 1020000 // Ensure it's > 0, consistent with db-connector.go
	}

	smallValue := "small value for reactive test"
	largeValue := generateLargeString(maxCacheSize + 200)
	// Use a consistent error string for "entity too big"
	datastoreTooBigError := errors.New("rpc error: code = ResourceExhausted desc = entity is too big")
	genericDatastoreError := errors.New("some other datastore error")
	gcsGenericError := errors.New("simulated GCS error")

	tests := []struct {
		name              string
		keyData           CacheKeyData
		dbType            string // "datastore" or "opensearch"
		initialPutError   error  // For first Datastore Put
		gcsWriteError     error  // For GCS writer.Write
		gcsCloseError     error  // For GCS writer.Close
		secondPutError    error  // For second Datastore Put (placeholder)
		getDatastoreError error  // For Datastore Get
		gcsReadError      error  // For GCS reader.Read
		expectedSetValue  string // Expected value in datastore after Set
		expectedGetValue  string // Expected value after Get
		expectSetError    bool
		expectGetError    bool
		skipGet           bool   // If Set is expected to fail hard
	}{
		{
			name:             "Datastore - Small Value - Success",
			keyData:          newTestCacheKeyData("dsSmall", testOrgID, smallValue, "cat1"),
			dbType:           "datastore",
			expectedSetValue: smallValue,
			expectedGetValue: smallValue,
		},
		{
			name:             "Datastore - Large Value - Reactive GCS Success",
			keyData:          newTestCacheKeyData("dsLargeGCSGood", testOrgID, largeValue, "cat2"),
			dbType:           "datastore",
			initialPutError:  datastoreTooBigError,
			expectedSetValue: fmt.Sprintf("GCS_VALUE:large_cache_values/%s/dsLargeGCSGood", testOrgID),
			expectedGetValue: largeValue,
		},
		{
			name:            "Datastore - Set - Initial Put Fails (Non-TooBig Error)",
			keyData:         newTestCacheKeyData("dsSetFailInitialPut", testOrgID, largeValue, "cat3"),
			dbType:          "datastore",
			initialPutError: genericDatastoreError,
			expectSetError:  true,
			skipGet:         true,
		},
		{
			name:            "Datastore - Set - GCS Write Fails",
			keyData:         newTestCacheKeyData("dsSetFailGCSWrite", testOrgID, largeValue, "cat4"),
			dbType:          "datastore",
			initialPutError: datastoreTooBigError,
			gcsWriteError:   gcsGenericError,
			expectSetError:  true,
			skipGet:         true,
		},
		{
			name:            "Datastore - Set - GCS Close Fails",
			keyData:         newTestCacheKeyData("dsSetFailGCSClose", testOrgID, largeValue, "cat5"),
			dbType:          "datastore",
			initialPutError: datastoreTooBigError,
			gcsCloseError:   gcsGenericError,
			expectSetError:  true,
			skipGet:         true,
		},
		{
			name:             "Datastore - Set - Second Put (Placeholder) Fails",
			keyData:          newTestCacheKeyData("dsSetFailSecondPut", testOrgID, largeValue, "cat6"),
			dbType:           "datastore",
			initialPutError:  datastoreTooBigError,
			secondPutError:   genericDatastoreError,
			expectedSetValue: fmt.Sprintf("GCS_VALUE:large_cache_values/%s/dsSetFailSecondPut", testOrgID), // GCS write succeeded
			expectSetError:   true,
			skipGet:          true,
		},
		{
			name:             "Datastore - Get - GCS Read Fails",
			keyData:          newTestCacheKeyData("dsGetFailGCSRead", testOrgID, largeValue, "cat7"), // Used to prime DS+GCS
			dbType:           "datastore",
			initialPutError:  datastoreTooBigError,                                                       // To trigger GCS path for Set
			expectedSetValue: fmt.Sprintf("GCS_VALUE:large_cache_values/%s/dsGetFailGCSRead", testOrgID), // This is what's in DS
			gcsReadError:     gcsGenericError,                                                            // Get will encounter this
			expectedGetValue: fmt.Sprintf("GCS_VALUE:large_cache_values/%s/dsGetFailGCSRead", testOrgID), // Expect placeholder back
			// expectGetError is false, as GetDatastoreKey returns placeholder and logs error
		},
		{
			name:             "OpenSearch - Small Value - No GCS",
			keyData:          newTestCacheKeyData("osSmallNoGCS", testOrgID, smallValue, "cat8"),
			dbType:           "opensearch",
			expectedSetValue: smallValue, // Value written to OpenSearch (mocked by indexEs behavior)
			expectedGetValue: smallValue, // Value retrieved from OpenSearch
		},
		{
			name:             "OpenSearch - Large Value - No GCS",
			keyData:          newTestCacheKeyData("osLargeNoGCS", testOrgID, largeValue, "cat9"),
			dbType:           "opensearch",
			expectedSetValue: largeValue, // OpenSearch should handle large values or error differently
			expectedGetValue: largeValue,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			project.DbType = tc.dbType
			mockDS.Reset()
			currentGCSStore := make(map[string]map[string][]byte) // Test-case specific GCS data
			logBuffer.Reset()

			dsKeyName := fmt.Sprintf("%s_%s", tc.keyData.OrgId, tc.keyData.Key)
			if tc.keyData.Category != "" && tc.keyData.Category != "default" {
				dsKeyName = fmt.Sprintf("%s_%s", dsKeyName, tc.keyData.Category)
			}
			dsKeyName = url.QueryEscape(dsKeyName)
			if len(dsKeyName) > 127 { dsKeyName = dsKeyName[:127] }

			expectedDSKey := datastore.NameKey("org_cache", dsKeyName, nil)
			gcsPath := fmt.Sprintf("large_cache_values/%s/%s", tc.keyData.OrgId, tc.keyData.Key)

			// Configure GCS factory functions for this specific test case
			newGCSWriterFunc = func(ctx context.Context, o *storage.ObjectHandle) io.WriteCloser {
				return &mockGCSWriter{objectPath: o.ObjectName(), bucketName: o.BucketName(), gcsStore: &currentGCSStore, writeError: tc.gcsWriteError, closeError: tc.gcsCloseError}
			}
			newGCSReaderFunc = func(ctx context.Context, o *storage.ObjectHandle) (io.ReadCloser, error) {
				if tc.gcsReadError != nil {
					return nil, tc.gcsReadError
				}
				bucketData, bucketExists := currentGCSStore[o.BucketName()]
				if !bucketExists {
					return nil, storage.ErrObjectNotExist
				}
				objData, objExists := bucketData[o.ObjectName()]
				if !objExists {
					return nil, storage.ErrObjectNotExist
				}
				return newMockGCSReader(objData, nil), nil
			}

			// Configure Datastore mock behavior
			if tc.dbType == "datastore" {
				mockDS.putError = tc.initialPutError // Error for the first attempt
				mockDS.shouldFailSecondPut = (tc.secondPutError != nil)
				mockDS.secondPutError = tc.secondPutError
			}

			// --- Test SetDatastoreKey ---
			errSet := SetDatastoreKey(ctx, tc.keyData)

			if tc.expectSetError {
				assert.Error(t, errSet, "SetDatastoreKey expected to return an error")
			} else {
				assert.NoError(t, errSet, "SetDatastoreKey expected to succeed")
			}

			// Verify Datastore state (if datastore was used and Set didn't error out before Put)
			if tc.dbType == "datastore" {
				if !tc.expectSetError || (tc.expectSetError && tc.secondPutError != nil && tc.gcsWriteError == nil && tc.gcsCloseError == nil)) { // Check DS if set was successful OR if it failed on second put (meaning first GCS part was ok)
					dsStoredDataBytes, ok := mockDS.data[expectedDSKey.String()]
					if strings.HasPrefix(tc.expectedSetValue, "GCS_VALUE:") || !tc.expectSetError { // If we expect a placeholder or full success
						assert.True(t, ok, "Data should be in mock datastore")
						if ok {
							var dsStoredData CacheKeyData
							err := json.Unmarshal(dsStoredDataBytes, &dsStoredData)
							assert.NoError(t, err)
							assert.Equal(t, tc.expectedSetValue, dsStoredData.Value, "Value in datastore mismatch after Set")
						}
					} else if tc.expectSetError && tc.initialPutError != nil && !strings.Contains(tc.initialPutError.Error(), "entity is too big") {
						assert.False(t, ok, "Data should not be in mock datastore if initial put failed with non-too-big error")
					}
				}
			}

			// Verify GCS state
			wasTooBig := tc.initialPutError != nil && strings.Contains(tc.initialPutError.Error(), "entity is too big")
			gcsWriteShouldHaveSucceeded := wasTooBig && tc.gcsWriteError == nil && tc.gcsCloseError == nil

			if gcsWriteShouldHaveSucceeded {
				bucketData, bucketExists := currentGCSStore[testBucketName]
				assert.True(t, bucketExists, "GCS bucket should exist if GCS write was successful")
				if bucketExists {
					objData, objExists := bucketData[gcsPath]
					assert.True(t, objExists, "GCS object should exist if GCS write was successful")
					assert.Equal(t, tc.keyData.Value, string(objData), "GCS data mismatch")
				}
			} else if wasTooBig { // Attempted GCS write but it failed
				bucketData, bucketExists := currentGCSStore[testBucketName]
				if bucketExists {
					_, objExists := bucketData[gcsPath]
					assert.False(t, objExists, "GCS object should not exist if GCS write/close failed")
				}
			}


			if tc.skipGet {
				return
			}

			// --- Test GetDatastoreKey ---
			mockDS.getError = tc.getDatastoreError // Set error for the Get call

			// Prime Datastore for Get if necessary (e.g., if Set failed or state needs specific setup for Get)
			if tc.dbType == "datastore" {
				// If Set was supposed to store a placeholder and did so (even if second Datastore put failed)
				if strings.HasPrefix(tc.expectedSetValue, "GCS_VALUE:") {
					if !tc.expectSetError || (tc.expectSetError && tc.secondPutError != nil) {
						primeDSData := newTestCacheKeyData(tc.keyData.Key, tc.keyData.OrgId, tc.expectedSetValue, tc.keyData.Category)
						primeDSBytes, _ := json.Marshal(primeDSData)
						mockDS.data[expectedDSKey.String()] = primeDSBytes
					}
				} else if !tc.expectSetError { // If Set stored the original value directly
					primeDSData := newTestCacheKeyData(tc.keyData.Key, tc.keyData.OrgId, tc.expectedSetValue, tc.keyData.Category)
					primeDSBytes, _ := json.Marshal(primeDSData)
					mockDS.data[expectedDSKey.String()] = primeDSBytes
				}
				// If GCS read error is expected for Get, ensure GCS has the "real" data for the reader to attempt and fail
                if tc.gcsReadError != nil && strings.HasPrefix(tc.expectedSetValue, "GCS_VALUE:") {
                    if currentGCSStore[testBucketName] == nil {
                        currentGCSStore[testBucketName] = make(map[string][]byte)
                    }
                    currentGCSStore[testBucketName][gcsPath] = []byte(tc.keyData.Value) // original large value
                }
			}


			retrievedData, errGet := GetDatastoreKey(ctx, dsKeyName, tc.keyData.Category)

			if tc.expectGetError {
				assert.Error(t, errGet, "GetDatastoreKey expected to return an error")
			} else {
				assert.NoError(t, errGet, "GetDatastoreKey expected to succeed")
				if assert.NotNil(t, retrievedData) {
					assert.Equal(t, tc.expectedGetValue, retrievedData.Value, "Retrieved value mismatch after Get")
				}
			}

			// Check for log message if GCS read error occurred and placeholder was returned
			if tc.gcsReadError != nil && tc.expectedGetValue == tc.expectedSetValue && strings.HasPrefix(tc.expectedGetValue, "GCS_VALUE:") {
				assert.Contains(t, logBuffer.String(), fmt.Sprintf("[ERROR] Failed to read GCS object for key %s (path %s): %s", tc.keyData.Key, gcsPath, tc.gcsReadError.Error()), "Expected log message for GCS read error")
			}
		})
	}
}

// Helper to generate large string
func generateLargeString(size int) string {
	return strings.Repeat("a", size)
}

// Helper to create CacheKeyData
func newTestCacheKeyData(key, orgId, value, category string) CacheKeyData {
	fixedTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	return CacheKeyData{
		OrgId:    orgId,
		Key:      key,
		Value:    value,
		Category: category,
		Created:  fixedTime,
		Edited:   fixedTime,
	}
}
[end of db-connector_test.go]
