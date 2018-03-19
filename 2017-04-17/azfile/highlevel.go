package azfile

import (
	"context"
	"fmt"
	"io"
	"math"

	"bytes"
	"os"
	"sync"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

// UploadToAzureFileOptions identifies options used by the UploadBufferToAzureFile and UploadFileToAzureFile functions.
type UploadToAzureFileOptions struct {
	// RangeSize specifies the range size to use in each parallel upload; the default (and maximum size) is FileMaxUploadRangeBytes.
	RangeSize int64

	// Progress is a function that is invoked periodically as bytes are send in a UploadRange call to the FileURL.
	Progress pipeline.ProgressReceiver

	// Parallelism indicates the maximum number of ranges to upload in parallel. If 0(default) is provided, 5 parallelism will be used by default.
	Parallelism int16

	// Overwrite indicates whether to overwrite the destination if it already exists.
	Overwrite bool

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
		panic(fmt.Sprintf("RangeSize option must be > 0 and <= %d", FileMaxUploadRangeBytes))
	}
	if o.RangeSize == 0 {
		o.RangeSize = FileMaxUploadRangeBytes
	}

	size := int64(len(b))

	if size > FileMaxSize {
		panic(fmt.Sprintf("The buffer is too big, the size must be <= %d.", FileMaxSize))
	}

	parallelism := o.Parallelism
	if parallelism == 0 {
		parallelism = 5 // default parallelism
	}

	// 2. Try to create the Azure file.
	_, err := fileURL.Create(ctx, size, o.FileHTTPHeaders, o.Metadata)
	if err != nil && err.(StorageError) != nil && (err.(StorageError)).ServiceCode() == ServiceCodeResourceAlreadyExists {
		if !o.Overwrite { // return error if not want to overwrite existing Azure file
			return err
		}

		// Otherwise, resize the Azure file
		_, err = fileURL.Resize(ctx, size)
		if err != nil {
			return err
		}
	}

	// 3. Prepare and do parallel upload.
	numRanges := int16(math.Ceil(float64(size) / float64(o.RangeSize)))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	uploadRangeChannel := make(chan func() (*FileUploadRangeResponse, error), parallelism) // Create the channel that release 'parallelism' goroutines concurrently
	uploadRangeResponseChannel := make(chan error, numRanges)                              // Holds each UploadRange's response

	// Create the goroutines that process each UploadRange (in parallel).
	for g := int16(0); g < parallelism; g++ {
		//grIndex := g
		go func() {
			for f := range uploadRangeChannel {
				//fmt.Printf("gr-%d start upload\n", grIndex)
				_, err := f()
				uploadRangeResponseChannel <- err
				//fmt.Printf("gr-%d end upload\n", grIndex)
			}
		}()
	}

	fileProgress := int64(0)
	progressLock := &sync.Mutex{}

	curRangeSize := o.RangeSize
	// Add each upload range to the channel.
	for rangeNum := int16(0); rangeNum < numRanges; rangeNum++ {
		if rangeNum == numRanges-1 { // Last range
			curRangeSize = size - (int64(rangeNum) * o.RangeSize) // Remove size of all uploaded ranges from total
		}
		offset := int64(rangeNum) * o.RangeSize

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
					progressLock.Unlock()
				})
		}

		uploadRangeChannel <- func() (*FileUploadRangeResponse, error) {
			return fileURL.UploadRange(ctx, int64(offset), body)
		}
	}
	close(uploadRangeChannel)

	// Wait for the upload ranges to complete.
	for rangeNum := int16(0); rangeNum < numRanges; rangeNum++ {
		responseError := <-uploadRangeResponseChannel
		if responseError != nil {
			cancel()             // As soon as any UploadRange fails, cancel all remaining UploadRange calls
			return responseError // No need to process anymore responses
		}
	}

	return nil
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
	Parallelism int16

	// Max retry requests used during reading data for each range.
	MaxRetryRequestsPerRange int
}

// downloadAzureFileToBuffer downloads an Azure file to a buffer with parallel.
func downloadAzureFileToBuffer(ctx context.Context, fileURL FileURL, azfileProperties *FileGetPropertiesResponse,
	b []byte, o DownloadFromAzureFileOptions) (*FileGetPropertiesResponse, error) {

	// 1. Validate parameters, and set defaults.
	if o.RangeSize < 0 {
		panic("RangeSize option must be > 0")
	}
	if o.RangeSize == 0 {
		o.RangeSize = FileMaxUploadRangeBytes
	}

	if azfileProperties == nil {
		p, err := fileURL.GetProperties(ctx)
		azfileProperties = p
		if err != nil {
			return nil, err
		}
	}
	azfileSize := azfileProperties.ContentLength()

	if int64(len(b)) < azfileSize {
		panic(fmt.Sprintf("The buffer's size should be equal to or larger than Azure file's size: %d.", azfileSize))
	}

	parallelism := o.Parallelism
	if parallelism == 0 {
		parallelism = 5 // default parallelism
	}

	// 2. Prepare and do parallel download.
	numRanges := int16(math.Ceil(float64(azfileSize) / float64(o.RangeSize)))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	downloadRangeChannel := make(chan func() (*DownloadResponse, error), parallelism) // Create the channel that release 'parallelism' goroutines concurrently
	downloadRangeResponseChannel := make(chan error, numRanges)                       // Holds each response

	// Create the goroutines that process each DownloadRange (in parallel)
	for g := int16(0); g < parallelism; g++ {
		//grIndex := g
		go func() {
			for f := range downloadRangeChannel {
				//fmt.Printf("gr-%d start download\n", grIndex)
				_, err := f()
				downloadRangeResponseChannel <- err
				//fmt.Printf("gr-%d end download\n", grIndex)
			}
		}()
	}

	fileProgress := int64(0)
	progressLock := &sync.Mutex{}

	curRangeSize := o.RangeSize
	// Add each download range to the channel
	for rangeNum := int16(0); rangeNum < numRanges; rangeNum++ {
		if rangeNum == numRanges-1 { // Last range
			curRangeSize = azfileSize - (int64(rangeNum) * o.RangeSize) // Remove size of all downloaded ranges from total
		}
		offset := int64(rangeNum) * o.RangeSize

		// Prepare to write the proper section to the buffer
		downloadRangeChannel <- func() (*DownloadResponse, error) {
			dr, err := fileURL.Download(ctx, offset, curRangeSize, false)
			body := dr.Body(ResilientReaderOptions{MaxRetryRequests: o.MaxRetryRequestsPerRange})

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
						progressLock.Unlock()
					})
			}

			_, err = io.ReadFull(body, b[offset:offset+curRangeSize])
			body.Close()

			return dr, err
		}
	}
	close(downloadRangeChannel)

	// Wait for the download ranges to complete
	for rangeNum := int16(0); rangeNum < numRanges; rangeNum++ {
		responseError := <-downloadRangeResponseChannel
		if responseError != nil {
			cancel()                  // As soon as any DownloadRange fails, cancel all remaining DownloadRange calls
			return nil, responseError // No need to process anymore responses
		}
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
		panic("file should not be nils")
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

	// 4. Set mmap and call DownloadAzureFileToBuffer.
	m, err := newMMF(file, true, 0, int(azfileSize))
	if err != nil {
		return nil, err
	}
	defer m.unmap()

	return downloadAzureFileToBuffer(ctx, fileURL, azfileProperties, m, o)
}
