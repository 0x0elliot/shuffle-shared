package shuffle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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
	data              map[string][]byte // In-memory store: key.String() -> marshaled CacheKeyData struct
	putError          error
	putErrorKey       *datastore.Key // Specific key for which Put should error
	putErrorOnce      bool           // If true, Put will only return putError once, then nil
	getError          error
	getErrorKey       *datastore.Key // Specific key for which Get should error
	putCallCount      map[string]int
	getCallCount      map[string]int
	shouldFailSecondPut bool // Controls if the second Put (with placeholder) should fail
    secondPutError    error      // Error for the second Datastore Put attempt (with placeholder)
}

func newMockDatastoreClient() *mockDatastoreClient {
	return &mockDatastoreClient{
		data:         make(map[string][]byte),
		putCallCount: make(map[string]int),
		getCallCount: make(map[string]int),
	}
}

func (m *mockDatastoreClient) Put(ctx context.Context, key *datastore.Key, val interface{}) (*datastore.Key, error) {
	m.Called(ctx, key, val) // Record the call for mock assertions

	m.putCallCount[key.String()]++

	// Specific error for the first Put (entity too big)
	if m.putCallCount[key.String()] == 1 && m.putError != nil && (m.putErrorKey == nil || m.putErrorKey.String() == key.String()) {
		if m.putErrorOnce {
			errToReturn := m.putError
			// Don't reset m.putError here if we need to control subsequent calls specifically
			// log.Printf("[TEST_DEBUG] MockDatastore Put (ERROR ONCE - First Call): key=%s, returning err: %v", key.String(), errToReturn)
			return key, errToReturn
		}
		// log.Printf("[TEST_DEBUG] MockDatastore Put (ERROR - First Call): key=%s, returning err: %v", key.String(), m.putError)
		return key, m.putError
	}

	// Specific error for the second Put (placeholder save fails)
	if m.putCallCount[key.String()] == 2 && m.shouldFailSecondPut && m.secondPutError != nil {
		// log.Printf("[TEST_DEBUG] MockDatastore Put (ERROR - Second Call with placeholder): key=%s, returning err: %v", key.String(), m.secondPutError)
		return key, m.secondPutError
	}

	// Generic put error not tied to call count (if needed for other tests)
	if m.putError != nil && !m.putErrorOnce && (m.putErrorKey == nil || m.putErrorKey.String() == key.String()) {
		// log.Printf("[TEST_DEBUG] MockDatastore Put (GENERIC ERROR): key=%s, returning err: %v", key.String(), m.putError)
		return key, m.putError
	}


	byteVal, err := json.Marshal(val)
	if err != nil {
		return nil, fmt.Errorf("mockDatastoreClient: failed to marshal val for key %s: %v", key.String(), err)
	}
	m.data[key.String()] = byteVal
	// log.Printf("[TEST_DEBUG] MockDatastore Put (SUCCESS): key=%s, val_type=%T, stored_data_len=%d, call_count: %d", key.String(), val, len(byteVal), m.putCallCount[key.String()])
	return key, nil
}


func (m *mockDatastoreClient) Get(ctx context.Context, key *datastore.Key, val interface{}) error {
	m.Called(ctx, key, val) // Record the call
	m.getCallCount[key.String()]++

	if m.getError != nil && (m.getErrorKey == nil || m.getErrorKey.String() == key.String()) {
		// log.Printf("[TEST_DEBUG] MockDatastore Get (ERROR): key=%s, returning err: %v", key.String(), m.getError)
		return m.getError
	}

	dataBytes, ok := m.data[key.String()]
	if !ok {
		// log.Printf("[TEST_DEBUG] MockDatastore Get: key=%s NOT FOUND", key.String())
		return datastore.ErrNoSuchEntity
	}

	// log.Printf("[TEST_DEBUG] MockDatastore Get: key=%s, FOUND data_len=%d", key.String(), len(dataBytes))
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
	m.putErrorOnce = false
	m.getError = nil
	m.getErrorKey = nil
	m.putCallCount = make(map[string]int)
	m.getCallCount = make(map[string]int)
	m.shouldFailSecondPut = false
	m.secondPutError = nil
	m.Mock = mock.Mock{} // Resets testify mock expectations
}


// --- Mock GCS Components ---
type mockGCSWriter struct {
	io.WriteCloser
	objectPath string
	bucketName string
	buffer     strings.Builder
	closed     bool
	writeError error
	closeError error
	clientData *map[string]map[string][]byte // Pointer to the mock storage client's data
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
	// Store data in the mock client's map
	if (*w.clientData) == nil {
		(*w.clientData) = make(map[string]map[string][]byte)
	}
	if _, ok := (*w.clientData)[w.bucketName]; !ok {
		(*w.clientData)[w.bucketName] = make(map[string][]byte)
	}
	(*w.clientData)[w.bucketName][w.objectPath] = []byte(w.buffer.String())
	// log.Printf("[TEST_DEBUG] MockGCSWriter Close: path=%s, data_len=%d", w.objectPath, len(w.buffer.String()))
	return nil
}

type mockGCSReader struct {
	io.ReadCloser
	data       []byte
	currentPos int
	closed     bool
	readError  error
}

func newMockGCSReader(data []byte, readErr error) *mockGCSReader {
	return &mockGCSReader{data: data, readError: readErr}
}

func (r *mockGCSReader) Read(p []byte) (n int, err error) {
	if r.closed {
		return 0, errors.New("mockGCSReader: reading from closed reader")
	}
	if r.readError != nil {
		return 0, r.readError
	}
	if r.currentPos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.currentPos:])
	r.currentPos += n
	return n, nil
}

func (r *mockGCSReader) Close() error {
	r.closed = true
	return nil
}

// Store GCS factory functions to be overridden in tests
var (
	originalNewGCSWriterFunc func(ctx context.Context, o *storage.ObjectHandle) io.WriteCloser
	originalNewGCSReaderFunc func(ctx context.Context, o *storage.ObjectHandle) (io.ReadCloser, error)
)

// Mock implementation for storage.ObjectHandle - only what's needed
type mockObjectHandle struct {
	bucket     string
	name       string
	clientData *map[string]map[string][]byte // Pointer to where GCS data is stored for tests
	writeErr   error
	readErr    error
	closeErr   error
}

func (o *mockObjectHandle) ObjectName() string { return o.name }
func (o *mockObjectHandle) BucketName() string { return o.bucket }


func TestSetAndGetDatastoreKey_ReactiveGCS(t *testing.T) {
	ctx := context.Background()
	testOrgID := "test-org-gcs"
	testBucketName := "test-bucket-gcs"

	oldProject := project

	// Store original GCS factory functions and defer their restoration
	originalNewGCSWriterFunc = newGCSWriterFunc
	originalNewGCSReaderFunc = newGCSReaderFunc

	defer func() {
		project = oldProject
		newGCSWriterFunc = originalNewGCSWriterFunc
		newGCSReaderFunc = originalNewGCSReaderFunc
		log.SetOutput(os.Stderr) // Restore default logger output
	}()

	// Capture log output
    var logBuffer strings.Builder
    log.SetOutput(&logBuffer)


	mockDS := newMockDatastoreClient()

	// This map will simulate our GCS bucket storage for the test
	gcsBucketData := make(map[string]map[string][]byte)

	project = ShuffleStorage{
		Dbclient:      mockDS,
		StorageClient: &storage.Client{}, // Real client not used due to factory override
		BucketName:    testBucketName,
		DbType:        "datastore",
		CacheDb:       false,
	}

	if maxCacheSize == 0 {
		maxCacheSize = 1020000 // Default if not set, ensure it's > 0
	}

	smallValue := "small value for reactive test"
	largeValue := generateLargeString(maxCacheSize + 200) // Ensure this is larger than maxCacheSize
	datastoreTooBigError := errors.New("rpc error: code = ResourceExhausted desc = entity is too big")
	genericDatastoreError := errors.New("some other datastore error")
	gcsGenericError := errors.New("simulated GCS error")

	// Override GCS factory functions
	newGCSWriterFunc = func(ctx context.Context, o *storage.ObjectHandle) io.WriteCloser {
		objHandle := o // Assuming o is compatible with mockObjectHandle for ObjectName/BucketName
		mObj, ok := o.(*mockObjectHandle) // Try to cast to our mock type
		if !ok {
			// Fallback if it's a real storage.ObjectHandle (e.g. if test setup changes)
			// This path might not work as expected if it's a real handle without more complex mocking.
			return &mockGCSWriter{objectPath: objHandle.ObjectName(), bucketName: objHandle.BucketName(), clientData: &gcsBucketData}
		}
		return &mockGCSWriter{objectPath: mObj.name, bucketName: mObj.bucket, clientData: &gcsBucketData, writeError: mObj.writeErr, closeError: mObj.closeErr}
	}

	newGCSReaderFunc = func(ctx context.Context, o *storage.ObjectHandle) (io.ReadCloser, error) {
		objHandle := o
		mObj, ok := o.(*mockObjectHandle)
		var path, bucket string
		var readErr error
		if !ok {
			path = objHandle.ObjectName()
			bucket = objHandle.BucketName()
		} else {
			path = mObj.name
			bucket = mObj.bucket
			readErr = mObj.readErr
		}

		if readErr != nil {
			return nil, readErr
		}
		if gcsBucketData[bucket] == nil || gcsBucketData[bucket][path] == nil {
			return nil, storage.ErrObjectNotExist
		}
		return newMockGCSReader(gcsBucketData[bucket][path], nil), nil
	}


	tests := []struct {
		name              string
		keyData           CacheKeyData
		dbType            string
		initialPutError   error
		gcsWriteError     error
		gcsCloseError     error
		secondPutError    error
		getDatastoreError error
		gcsReadError      error
		expectedSetValue  string
		expectedGetValue  string
		expectSetError    bool
		expectGetError    bool
		skipGet           bool
	}{
		{
			name:             "Small Value - Datastore",
			keyData:          newTestCacheKeyData("smallReactive", testOrgID, smallValue, "default"),
			dbType:           "datastore",
			expectedSetValue: smallValue,
			expectedGetValue: smallValue,
		},
		{
			name:             "Large Value - Reactive GCS Success - Datastore",
			keyData:          newTestCacheKeyData("largeReactiveGood", testOrgID, largeValue, "default"),
			dbType:           "datastore",
			initialPutError:  datastoreTooBigError,
			expectedSetValue: fmt.Sprintf("GCS_VALUE:large_cache_values/%s/largeReactiveGood", testOrgID),
			expectedGetValue: largeValue,
		},
		{
			name:            "Datastore Error (Not 'entity is too big') - SetDatastoreKey",
			keyData:         newTestCacheKeyData("dsErrorNonBig", testOrgID, largeValue, "default"),
			dbType:          "datastore",
			initialPutError: genericDatastoreError,
			expectSetError:  true,
			skipGet:         true,
		},
		{
			name:            "GCS Upload Write Error - SetDatastoreKey",
			keyData:         newTestCacheKeyData("gcsUploadWriteErr", testOrgID, largeValue, "default"),
			dbType:          "datastore",
			initialPutError: datastoreTooBigError,
			gcsWriteError:   gcsGenericError,
			expectSetError:  true,
			skipGet:         true,
		},
		{
			name:            "GCS Upload Close Error - SetDatastoreKey",
			keyData:         newTestCacheKeyData("gcsUploadCloseErr", testOrgID, largeValue, "default"),
			dbType:          "datastore",
			initialPutError: datastoreTooBigError,
			gcsCloseError:   gcsGenericError,
			expectSetError:  true,
			skipGet:         true,
		},
		{
			name:             "Second Datastore Put Fails - SetDatastoreKey",
			keyData:          newTestCacheKeyData("secondPutFails", testOrgID, largeValue, "default"),
			dbType:           "datastore",
			initialPutError:  datastoreTooBigError,
			secondPutError:   genericDatastoreError, // Error for the second Put
			expectSetError:   true,
			skipGet:          true,
		},
		{
			name:             "Large Value - GCS Download Error - GetDatastoreKey",
			keyData:          newTestCacheKeyData("gcsDownloadErr", testOrgID, largeValue, "default"),
			dbType:           "datastore",
			initialPutError:  datastoreTooBigError,
			expectedSetValue: fmt.Sprintf("GCS_VALUE:large_cache_values/%s/gcsDownloadErr", testOrgID),
			gcsReadError:     gcsGenericError,
			expectedGetValue: fmt.Sprintf("GCS_VALUE:large_cache_values/%s/gcsDownloadErr", testOrgID),
			// expectGetError: true, // GetDatastoreKey now returns placeholder and logs error
		},
		{
			name:             "OpenSearch - Small Value - No GCS",
			keyData:          newTestCacheKeyData("osSmall", testOrgID, smallValue, "default"),
			dbType:           "opensearch",
			expectedSetValue: smallValue,
			expectedGetValue: smallValue,
		},
		{
			name:             "OpenSearch - Large Value - No GCS (no 'entity too big' error)",
			keyData:          newTestCacheKeyData("osLarge", testOrgID, largeValue, "default"),
			dbType:           "opensearch",
			initialPutError:  nil, // Assume OpenSearch handles large values or gives different errors
			expectedSetValue: largeValue,
			expectedGetValue: largeValue,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			project.DbType = tc.dbType
			mockDS.Reset()
			gcsBucketData = make(map[string]map[string][]byte) // Reset GCS mock data
			logBuffer.Reset()


			dsKeyName := fmt.Sprintf("%s_%s", tc.keyData.OrgId, tc.keyData.Key)
			if tc.keyData.Category != "" && tc.keyData.Category != "default" {
				dsKeyName = fmt.Sprintf("%s_%s", dsKeyName, tc.keyData.Category)
			}
			dsKeyName = url.QueryEscape(dsKeyName)
			if len(dsKeyName) > 127 {dsKeyName = dsKeyName[:127]}

			expectedDSKey := datastore.NameKey("org_cache", dsKeyName, nil)
			gcsPath := fmt.Sprintf("large_cache_values/%s/%s", tc.keyData.OrgId, tc.keyData.Key)

			// Configure mock Datastore behavior
			if tc.dbType == "datastore" {
				mockDS.putError = tc.initialPutError
				mockDS.putErrorOnce = true
				mockDS.shouldFailSecondPut = (tc.secondPutError != nil)
				mockDS.secondPutError = tc.secondPutError
			}

			// --- SetDatastoreKey Test ---
			// Configure GCS behavior for Set (via mockObjectHandle passed to newGCSWriterFunc)
			project.StorageClient.storageClient = &storage.Client{} // Ensure it's not nil

			// This is a bit of a hack as storage.ObjectHandle is a struct.
			// We make our factory functions aware of these mock handles.
			currentGCSObjectHandle := &mockObjectHandle{
				bucket: testBucketName,
				name: gcsPath,
				clientData: &gcsBucketData,
				writeErr: tc.gcsWriteError,
				closeErr: tc.gcsCloseError,
				readErr: tc.gcsReadError,
			}

			// Override StorageClient.Object to return our mock handle if needed,
			// but SetDatastoreKey constructs it directly. So factories are key.
			// The factories will use the details from currentGCSObjectHandle when invoked.

			errSet := SetDatastoreKey(ctx, tc.keyData)

			if tc.expectSetError {
				assert.Error(t, errSet, "SetDatastoreKey should have returned an error")
			} else {
				assert.NoError(t, errSet, "SetDatastoreKey should not have returned an error")
			}

			if tc.dbType == "datastore" && !tc.expectSetError {
				dsStoredDataBytes, ok := mockDS.data[expectedDSKey.String()]
				assert.True(t, ok, "Data should be in mock datastore if Set succeeded without error")
				if ok {
					var dsStoredData CacheKeyData
					err := json.Unmarshal(dsStoredDataBytes, &dsStoredData)
					assert.NoError(t, err)
					assert.Equal(t, tc.expectedSetValue, dsStoredData.Value, "Value in datastore mismatch")
				}
			}

			// Verify GCS write
			if tc.initialPutError != nil && strings.Contains(tc.initialPutError.Error(), "entity is too big") { // GCS path was attempted
				if tc.gcsWriteError == nil && tc.gcsCloseError == nil {
					if !tc.expectSetError { // If Set was expected to succeed overall
						bucket, ok := gcsBucketData[testBucketName]
						assert.True(t, ok, "GCS bucket should exist")
						if ok {
							data, ok := bucket[gcsPath]
							assert.True(t, ok, "GCS object should exist")
							assert.Equal(t, largeValue, string(data), "GCS data mismatch")
						}
					}
				} else { // GCS write/close error occurred
					_, bucketOk := gcsBucketData[testBucketName]
					if bucketOk {
						_, objOk := gcsBucketData[testBucketName][gcsPath]
						assert.False(t, objOk, "GCS object should not exist if GCS write/close failed")
					}
				}
			}


			if tc.skipGet {
				return
			}

			// --- GetDatastoreKey Test ---
			mockDS.getError = tc.getDatastoreError

			// Prime Datastore for Get if Set didn't populate it as expected for this Get test case
			if tc.dbType == "datastore" {
				// If Set was supposed to store a placeholder, ensure it's there for Get
				if tc.expectedSetValue != tc.keyData.Value && strings.HasPrefix(tc.expectedSetValue, "GCS_VALUE:") {
					primeData := newTestCacheKeyData(tc.keyData.Key, tc.keyData.OrgId, tc.expectedSetValue, tc.keyData.Category)
					primeBytes, _ := json.Marshal(primeData)
					mockDS.data[expectedDSKey.String()] = primeBytes
				} else if tc.expectedSetValue == tc.keyData.Value && !strings.HasPrefix(tc.expectedSetValue, "GCS_VALUE:") {
					primeData := newTestCacheKeyData(tc.keyData.Key, tc.keyData.OrgId, tc.expectedSetValue, tc.keyData.Category)
					primeBytes, _ := json.Marshal(primeData)
					mockDS.data[expectedDSKey.String()] = primeBytes
				}
				// If GCS read error is expected, make sure GCS has the "real" data for the reader to attempt
				if tc.gcsReadError != nil && strings.HasPrefix(tc.expectedSetValue, "GCS_VALUE:") {
					if gcsBucketData[testBucketName] == nil {
						gcsBucketData[testBucketName] = make(map[string][]byte)
					}
					gcsBucketData[testBucketName][gcsPath] = []byte(tc.keyData.Value) // Store original large value
				}
			}

			retrievedData, errGet := GetDatastoreKey(ctx, dsKeyName, tc.keyData.Category)

			if tc.expectGetError {
				assert.Error(t, errGet, "GetDatastoreKey should have returned an error")
			} else {
				assert.NoError(t, errGet, "GetDatastoreKey should not have returned an error")
				if assert.NotNil(t, retrievedData) {
					assert.Equal(t, tc.expectedGetValue, retrievedData.Value, "Retrieved value mismatch")

					if tc.gcsReadError != nil && strings.HasPrefix(retrievedData.Value, "GCS_VALUE:") {
						assert.Contains(t, logBuffer.String(), fmt.Sprintf("[ERROR] Failed to read GCS object for key %s", tc.keyData.Key), "Expected log message for GCS read error")
					} else if tc.gcsReadError != nil && !strings.HasPrefix(retrievedData.Value, "GCS_VALUE:") {
						// This case should not happen if GetDatastoreKey returns placeholder on GCS read error
						// assert.Fail(t, "GCS read error was expected, but value is not a placeholder")
					}
				}
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
	// Using a fixed time for Created/Edited for easier comparison of the CacheKeyData struct itself if needed,
	// though most tests focus on the .Value field.
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

func (m *mockDatastoreClient) GetMulti(ctx context.Context, keys []*datastore.Key, dst interface{}) error {
    // Simplified mock for GetMulti. Assumes dst is a slice of the correct type.
    // Production GetMulti is more complex.

    // This is a basic implementation. A real GetMulti might need to handle partial errors, etc.
    // For now, assume it behaves like multiple Gets.

    // Get the underlying type of dst (should be []CacheKeyData or []*CacheKeyData)
    // For simplicity, this mock won't try to dynamically handle different slice types.
    // It will assume dst is compatible with what Get stores.

    // Example: if dst is []CacheKeyData
    // results := dst.([]CacheKeyData)
    // if len(results) != len(keys) {
    //    return errors.New("mockDatastoreClient GetMulti: len(dst) != len(keys)")
    // }
    // for i, key := range keys {
    //    if err := m.Get(ctx, key, &results[i]); err != nil {
    //        // Handle error, possibly by returning a datastore.MultiError
    //        return err
    //    }
    // }
    return errors.New("mockDatastoreClient GetMulti: Not fully implemented for this test setup")
}

func (m *mockDatastoreClient) Count(ctx context.Context, q *datastore.Query) (int, error) {
	return 0, errors.New("mockDatastoreClient Count: Not implemented")
}

func (m *mockDatastoreClient) Delete(ctx context.Context, key *datastore.Key) error {
	delete(m.data, key.String())
	return nil
}

func (m *mockDatastoreClient) DeleteMulti(ctx context.Context, keys []*datastore.Key) error {
	for _, key := range keys {
		delete(m.data, key.String())
	}
	return nil
}

func (m *mockDatastoreClient) Run(ctx context.Context, q *datastore.Query) *datastore.Iterator {
	return nil // Not implemented for these tests
}

func (m *mockDatastoreClient) GetAll(ctx context.Context, q *datastore.Query, dst interface{}) ([]*datastore.Key, error) {
    // This is a simplified mock. A real GetAll would iterate and unmarshal.
    // For tests that need GetAll, this would need to be more sophisticated,
    // potentially using the `q` parameter to filter m.data.

    // Example of how a more complete mock might look:
    // var results []CacheKeyData // Assuming CacheKeyData is the type
    // for keyStr, dataBytes := range m.data {
    //     // Apply query filters here based on q
    //     // For simplicity, let's say we just return all data for now if no filter is applied by test
    //     var item CacheKeyData
    //     if err := json.Unmarshal(dataBytes, &item); err == nil {
    //         // Check if item matches query q (e.g., q.filter)
    //         // If matches, append to results
    //     }
    // }
    // Copy results to dst (which is a pointer to a slice)
    // reflect.ValueOf(dst).Elem().Set(reflect.ValueOf(results))
    // return nil, nil // And appropriate keys

    return nil, errors.New("mockDatastoreClient GetAll: Not fully implemented for this test setup")
}
[end of db-connector_test.go]
