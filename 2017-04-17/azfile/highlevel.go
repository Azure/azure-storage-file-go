package azfile

import (
	"context"
	"fmt"
	"io"

	"bytes"
	"os"
	"sync"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

const (
	// defaultParallelCount specifies default parallel count will be used by parallel upload/download methods
	defaultParallelCount = 5

	// fileSegmentSize specifies file segment size that file would be splitted into during parallel upload/download
	fileSegmentSize = 500 * 1024 * 1024
)

// UploadToAzureFileOptions identifies options used by the UploadBufferToAzureFile and UploadFileToAzureFile functions.
type UploadToAzureFileOptions struct {
	// RangeSize specifies the range size to use in each parallel upload; the default (and maximum size) is FileMaxUploadRangeBytes.
	RangeSize int64

	// Progress is a function that is invoked periodically as bytes are send in a UploadRange call to the FileURL.
	Progress pipeline.ProgressReceiver

	// Parallelism indicates the maximum number of ranges to upload in parallel. If 0(default) is provided, 5 parallelism will be used by default.
	Parallelism uint16

	// FileHTTPHeaders contains read/writeable file properties.
	FileHTTPHeaders FileHTTPHeaders

	// Metadata contains metadata key/value pairs.
	Metadata Metadata
}

// UploadBufferToAzureFile uploads a buffer to an Azure file.
func UploadBufferToAzureFile(ctx context.Context, b []byte,
	fileURL FileURL, o UploadToAzureFileOptions) error {

	// 1. Validate parameters, and set defaults.
	if o.RangeSize < 0 || o.RangeSize > FileMaxUploadRangeBytes {
		panic(fmt.Sprintf("o.RangeSize must be >= 0 and <= %d, in bytes", FileMaxUploadRangeBytes))
	}
	if o.RangeSize == 0 {
		o.RangeSize = FileMaxUploadRangeBytes
	}

	size := int64(len(b))

	if size > FileMaxSizeInBytes {
		panic(fmt.Sprintf("b's length must be <= %d, in bytes", FileMaxSizeInBytes))
	}

	parallelism := o.Parallelism
	if parallelism == 0 {
		parallelism = defaultParallelCount // default parallelism
	}

	// 2. Try to create the Azure file.
	_, err := fileURL.Create(ctx, size, o.FileHTTPHeaders, o.Metadata)
	if err != nil {
		return err
	}
	// If size equals to 0, upload nothing and directly return.
	if size == 0 {
		return nil
	}

	// 3. Prepare and do parallel upload.
	fileProgress := int64(0)
	progressLock := &sync.Mutex{}

	return doBatchTransfer(ctx, batchTransferOptions{
		transferSize: size,
		chunkSize:    o.RangeSize,
		parallelism:  parallelism,
		operation: func(offset int64, curRangeSize int64) error {
			// Prepare to read the proper section of the buffer.
			var body io.ReadSeeker = bytes.NewReader(b[offset : offset+curRangeSize])
			if o.Progress != nil {
				rangeProgress := int64(0)
				body = pipeline.NewRequestBodyProgress(body,
					func(bytesTransferred int64) {
						diff := bytesTransferred - rangeProgress
						rangeProgress = bytesTransferred
						progressLock.Lock()
						fileProgress += diff
						o.Progress(fileProgress)
						defer progressLock.Unlock()
					})
			}

			_, err := fileURL.UploadRange(ctx, int64(offset), body)
			return err
		},
		operationName: "UploadBufferToAzureFile",
	})
}

// UploadFileToAzureFile uploads a local file to an Azure file.
func UploadFileToAzureFile(ctx context.Context, file *os.File,
	fileURL FileURL, o UploadToAzureFileOptions) error {

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	m := mmf{} // Default to an empty slice; used for 0-size file
	if stat.Size() != 0 {
		m, err = newMMF(file, false, 0, int(stat.Size()))
		if err != nil {
			return err
		}
		defer m.unmap()
	}
	return UploadBufferToAzureFile(ctx, m, fileURL, o)
}

// DownloadFromAzureFileOptions identifies options used by the DownloadAzureFileToBuffer and DownloadAzureFileToFile functions.
type DownloadFromAzureFileOptions struct {
	// RangeSize specifies the range size to use in each parallel download; the default is FileMaxUploadRangeBytes.
	RangeSize int64

	// Progress is a function that is invoked periodically as bytes are recieved.
	Progress pipeline.ProgressReceiver

	// Parallelism indicates the maximum number of ranges to download in parallel. If 0(default) is provided, 5 parallelism will be used by default.
	Parallelism uint16

	// Max retry requests used during reading data for each range.
	MaxRetryRequestsPerRange int
}

// downloadAzureFileToBuffer downloads an Azure file to a buffer with parallel.
func downloadAzureFileToBuffer(ctx context.Context, fileURL FileURL, azfileProperties *FileGetPropertiesResponse,
	b []byte, o DownloadFromAzureFileOptions) (*FileGetPropertiesResponse, error) {

	// 1. Validate parameters, and set defaults.
	if o.RangeSize < 0 {
		panic("RangeSize option must be >= 0")
	}
	if o.RangeSize == 0 {
		o.RangeSize = FileMaxUploadRangeBytes
	}

	if azfileProperties == nil {
		p, err := fileURL.GetProperties(ctx)
		if err != nil {
			return nil, err
		}
		azfileProperties = p
	}
	azfileSize := azfileProperties.ContentLength()

	// If azure file size equals to 0, directly return as nothing need be downloaded.
	if azfileSize == 0 {
		return azfileProperties, nil
	}

	if int64(len(b)) < azfileSize {
		panic(fmt.Sprintf("The buffer's size should be equal to or larger than Azure file's size: %d.", azfileSize))
	}

	parallelism := o.Parallelism
	if parallelism == 0 {
		parallelism = defaultParallelCount // default parallelism
	}

	// 2. Prepare and do parallel download.
	fileProgress := int64(0)
	progressLock := &sync.Mutex{}

	err := doBatchTransfer(ctx, batchTransferOptions{
		transferSize: azfileSize,
		chunkSize:    o.RangeSize,
		parallelism:  parallelism,
		operation: func(offset int64, curRangeSize int64) error {
			dr, err := fileURL.Download(ctx, offset, curRangeSize, false)
			body := dr.Body(RetryReaderOptions{MaxRetryRequests: o.MaxRetryRequestsPerRange})

			if o.Progress != nil {
				rangeProgress := int64(0)
				body = pipeline.NewResponseBodyProgress(
					body,
					func(bytesTransferred int64) {
						diff := bytesTransferred - rangeProgress
						rangeProgress = bytesTransferred
						progressLock.Lock()
						fileProgress += diff
						o.Progress(fileProgress)
						defer progressLock.Unlock()
					})
			}

			_, err = io.ReadFull(body, b[offset:offset+curRangeSize])
			body.Close()

			return err
		},
		operationName: "downloadAzureFileToBuffer",
	})
	if err != nil {
		return nil, err
	}

	return azfileProperties, nil
}

// DownloadAzureFileToBuffer downloads an Azure file to a buffer with parallel.
func DownloadAzureFileToBuffer(ctx context.Context, fileURL FileURL,
	b []byte, o DownloadFromAzureFileOptions) (*FileGetPropertiesResponse, error) {
	return downloadAzureFileToBuffer(ctx, fileURL, nil, b, o)
}

// DownloadAzureFileToFile downloads an Azure file to a local file.
// The file would be created if it doesn't exist, and would be truncated if the size doesn't match.
func DownloadAzureFileToFile(ctx context.Context, fileURL FileURL, file *os.File, o DownloadFromAzureFileOptions) (*FileGetPropertiesResponse, error) {

	// 1. Validate parameters.
	if file == nil {
		panic("file should not be nil")
	}

	// 2. Try to get Azure file's size.
	azfileProperties, err := fileURL.GetProperties(ctx)
	if err != nil {
		return nil, err
	}
	azfileSize := azfileProperties.ContentLength()

	// 3. Compare and try to resize local file's size if it doesn't match Azure file's size.
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() != azfileSize {
		if err = file.Truncate(azfileSize); err != nil {
			return nil, err
		}
	}

	// 4. Set mmap and call DownloadAzureFileToBuffer, in this case file size should be > 0.
	m := mmf{} // Default to an empty slice; used for 0-size file
	if azfileSize > 0 {
		m, err = newMMF(file, true, 0, int(azfileSize))
		if err != nil {
			return nil, err
		}
		defer m.unmap()
	}

	return downloadAzureFileToBuffer(ctx, fileURL, azfileProperties, m, o)
}

// BatchTransferOptions identifies options used by doBatchTransfer.
type batchTransferOptions struct {
	transferSize  int64
	chunkSize     int64
	parallelism   uint16
	operation     func(offset int64, chunkSize int64) error
	operationName string
}

// doBatchTransfer helps to execute operations in a batch manner.
func doBatchTransfer(ctx context.Context, o batchTransferOptions) error {
	// Prepare and do parallel operations.
	numChunks := ((o.transferSize - 1) / o.chunkSize) + 1
	operationChannel := make(chan func() error, o.parallelism) // Create the channel that release 'parallelism' goroutines concurrently
	operationResponseChannel := make(chan error, numChunks)    // Holds each response
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create the goroutines that process each operation (in parallel).
	for g := uint16(0); g < o.parallelism; g++ {
		//grIndex := g
		go func() {
			for f := range operationChannel {
				//fmt.Printf("[%s] gr-%d start action\n", o.operationName, grIndex)
				err := f()
				operationResponseChannel <- err
				//fmt.Printf("[%s] gr-%d end action\n", o.operationName, grIndex)
			}
		}()
	}

	curChunkSize := o.chunkSize
	// Add each chunk's operation to the channel.
	for chunkIndex := int64(0); chunkIndex < numChunks; chunkIndex++ {
		if chunkIndex == numChunks-1 { // Last chunk
			curChunkSize = o.transferSize - (int64(chunkIndex) * o.chunkSize) // Remove size of all transferred chunks from total
		}
		offset := int64(chunkIndex) * o.chunkSize

		closureChunkSize := curChunkSize
		operationChannel <- func() error {
			return o.operation(offset, closureChunkSize)
		}
	}
	close(operationChannel)

	// Wait for the operations to complete.
	for chunkIndex := int64(0); chunkIndex < numChunks; chunkIndex++ {
		responseError := <-operationResponseChannel
		if responseError != nil {
			cancel()             // As soon as any operation fails, cancel all remaining operation calls
			return responseError // No need to process anymore responses
		}
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////////////
// Segment transfer POC
///////////////////////////////////////////////////////////////////////////////////////

// segmentTransferStatus contains single segment's transfer status.
type segmentTransferStatus struct {
	mMap               *mmf
	finishedChunkCount int64
	totalChunkCount    int64
}

// chunkTransferResult contains single chunk's transfer result.
type chunkTransferResult struct {
	segmentID segmentID
	err       error
}

type segmentID int64

// getFileSegmentSize is used for mocking fileSegmentSize
var getFileSegmentSize = func() int64 {
	return int64(fileSegmentSize)
}

// uploadBufPoolToAzureFile uploads buffer pool which consists of MMFs into Azure file by segments.
// Each segment is mapped to a MMF, and after each segment is transfered, it's corresponding MMF will be unmapped.
func uploadBufPoolToAzureFile(ctx context.Context, bufPool []mmf, size int64,
	fileURL FileURL, o UploadToAzureFileOptions) error {

	// 1. Validate parameters, and set defaults.
	if o.RangeSize < 0 || o.RangeSize > FileMaxUploadRangeBytes {
		panic(fmt.Sprintf("o.RangeSize must be >= 0 and <= %d, in bytes", FileMaxUploadRangeBytes))
	}
	if o.RangeSize == 0 {
		o.RangeSize = FileMaxUploadRangeBytes
	}

	if size > FileMaxSizeInBytes {
		panic(fmt.Sprintf("b's length must be <= %d, in bytes", FileMaxSizeInBytes))
	}

	parallelism := o.Parallelism
	if parallelism == 0 {
		parallelism = defaultParallelCount // default parallelism
	}

	// 2. Try to create the Azure file.
	_, err := fileURL.Create(ctx, size, o.FileHTTPHeaders, o.Metadata)
	if err != nil {
		return err
	}
	// If size equals to 0, upload nothing and directly return.
	if size == 0 {
		return nil
	}

	// 3. Prepare and do parallel upload.
	// Initialize transferSegmentMap and calculate total chunks.
	transferSegmentMap := make(map[segmentID]*segmentTransferStatus)
	defer func() {
		for _, sts := range transferSegmentMap {
			if sts.mMap != nil {
				sts.mMap.unmap()
			}
		}
	}()

	var numTotalChunks int64

	//fmt.Println(time.Now(), " uploadBufPoolToAzureFile starts to initialize transferSegmentMap.")

	// bufIndex is used as segmentID
	for bufIndex := int64(0); bufIndex < int64(len(bufPool)); bufIndex++ {
		curBuf := &bufPool[bufIndex]

		numSegmentChunks := (int64(len(*curBuf)-1) / o.RangeSize) + 1
		numTotalChunks += numSegmentChunks

		segID := segmentID(bufIndex)
		transferSegmentMap[segID] = &segmentTransferStatus{mMap: curBuf, totalChunkCount: numSegmentChunks}
	}

	//fmt.Println(time.Now(), " uploadBufPoolToAzureFile finishes to initialize transferSegmentMap.")

	// Initialize context and channels.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	operationChannel := make(chan func() (segmentID, error), parallelism)      // Create the channel that release 'parallelism' goroutines concurrently
	operationResponseChannel := make(chan chunkTransferResult, numTotalChunks) // Holds each response

	// Create the goroutines that process each operation (in parallel)
	for g := uint16(0); g < parallelism; g++ {
		go func() {
			for f := range operationChannel {
				segmentID, err := f()
				operationResponseChannel <- chunkTransferResult{segmentID: segmentID, err: err}
			}
		}()
	}

	// Create the goroutine that dispatches operations
	go func() {
		fileProgress := int64(0)
		progressLock := &sync.Mutex{}

		// bufIndex is used as segmentID
		for bufIndex := int64(0); bufIndex < int64(len(bufPool)); bufIndex++ {
			curBuf := &bufPool[bufIndex]
			curBufLen := int64(len(*curBuf))

			numChunks := ((curBufLen - 1) / o.RangeSize) + 1

			curChunkSize := o.RangeSize
			segID := segmentID(bufIndex)

			for chunkIndex := int64(0); chunkIndex < numChunks; chunkIndex++ {
				if chunkIndex == numChunks-1 { // Last chunk
					curChunkSize = curBufLen - (int64(chunkIndex) * o.RangeSize) // Remove size of dispatched chunks from total
				}
				bufOffset := int64(chunkIndex) * o.RangeSize
				fileOffset := bufIndex*getFileSegmentSize() + bufOffset

				// Prepare to read the proper section of the buffer
				var body io.ReadSeeker = bytes.NewReader((*curBuf)[bufOffset : bufOffset+curChunkSize])
				if o.Progress != nil {
					chunkProgress := int64(0)
					body = pipeline.NewRequestBodyProgress(body,
						func(bytesTransferred int64) {
							diff := bytesTransferred - chunkProgress
							chunkProgress = bytesTransferred
							progressLock.Lock()
							fileProgress += diff
							o.Progress(fileProgress)
							progressLock.Unlock()
						})
				}

				operationChannel <- func() (segmentID, error) {
					_, err := fileURL.UploadRange(ctx, int64(fileOffset), body)
					return segID, err
				}
			}
		}
		close(operationChannel)
	}()

	// Wait for the operations to complete.
	for chunkIndex := int64(0); chunkIndex < numTotalChunks; chunkIndex++ {
		chunkTransferResult := <-operationResponseChannel
		if chunkTransferResult.err != nil {
			cancel()                       // As soon as any operation fails, cancel all remaining operation calls
			return chunkTransferResult.err // No need to process anymore responses
		} else {
			sts, ok := transferSegmentMap[chunkTransferResult.segmentID]
			if !ok {
				panic(fmt.Sprintf("invalid status, cannot found segmentID: %d", chunkTransferResult.segmentID))
			}
			sts.finishedChunkCount++
			if sts.finishedChunkCount == sts.totalChunkCount {
				sts.mMap.unmap()
				delete(transferSegmentMap, chunkTransferResult.segmentID)
			}
		}
	}

	return nil
}

// UploadFileToAzureFile2 uploads a local file to an Azure file by segments.
func UploadFileToAzureFile2(ctx context.Context, file *os.File,
	fileURL FileURL, o UploadToAzureFileOptions) error {

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	var bufPool []mmf
	size := int64(stat.Size())

	segmentSize := getFileSegmentSize()
	//fmt.Println("UploadFileToAzureFile2 uses file segment size: ", segmentSize)

	if size != 0 {
		segmentCount := ((size - 1) / segmentSize) + 1
		curSegmentSize := int64(segmentSize)

		for i := int64(0); i < segmentCount; i++ {
			if i == segmentCount-1 {
				curSegmentSize = size - i*segmentSize
			}

			offset := i * segmentSize
			m, err := newMMF(file, false, offset, int(curSegmentSize))
			if err != nil { // If fail to create any of the Mmap file, return error immediately
				return err
			}
			bufPool = append(bufPool, m)
		}
	} else {
		m := mmf{} // Default to an empty slice; used for 0-size file
		bufPool = append(bufPool, m)
	}
	return uploadBufPoolToAzureFile(ctx, bufPool, size, fileURL, o)
}

///////////////////////////////////////////////////////////////////////////////////////
// End of Segment transfer POC
///////////////////////////////////////////////////////////////////////////////////////
