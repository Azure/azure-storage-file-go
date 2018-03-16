package azfile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

const (
	fileType = "file"

	// FileMaxUploadRangeBytes indicates the maximum number of bytes that can be sent in a call to UploadRange.
	FileMaxUploadRangeBytes = 4 * 1024 * 1024 // 4MB

	// FileMaxSize indicates the maxiumum file size.
	FileMaxSize = 1 * 1024 * 1024 * 1024 * 1024 // 1TB
)

// A FileURL represents a URL to an Azure Storage file.
type FileURL struct {
	fileClient fileClient
}

// NewFileURL creates a FileURL object using the specified URL and request policy pipeline.
func NewFileURL(url url.URL, p pipeline.Pipeline) FileURL {
	if p == nil {
		panic("p can't be nil")
	}
	fileClient := newFileClient(url, p)
	return FileURL{fileClient: fileClient}
}

// URL returns the URL endpoint used by the FileURL object.
func (f FileURL) URL() url.URL {
	return f.fileClient.URL()
}

// String returns the URL as a string.
func (f FileURL) String() string {
	u := f.URL()
	return u.String()
}

// WithPipeline creates a new FileURL object identical to the source but with the specified request policy pipeline.
func (f FileURL) WithPipeline(p pipeline.Pipeline) FileURL {
	if p == nil {
		panic("p can't be nil")
	}
	return NewFileURL(f.fileClient.URL(), p)
}

// WithSnapshot creates a new FileURL object identical to the source but with the specified share snapshot timestamp.
// Pass time.Time{} to remove the share snapshot returning a URL to the base file.
func (f FileURL) WithSnapshot(shareSnapshot string) FileURL {
	p := NewFileURLParts(f.URL())
	p.ShareSnapshot = shareSnapshot
	return NewFileURL(p.URL(), f.fileClient.Pipeline())
}

// Create creates a new file or replaces a file. Note that this method only initializes the file.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/create-file.
func (f FileURL) Create(ctx context.Context, size int64, h FileHTTPHeaders, metadata Metadata) (*FileCreateResponse, error) {
	return f.fileClient.Create(ctx, size, fileType, nil,
		&h.ContentType, &h.ContentEncoding, &h.ContentLanguage, &h.CacheControl,
		h.contentMD5Pointer(), &h.ContentDisposition, metadata)
}

// StartCopy copies the data at the source URL to a file.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/copy-file.
func (f FileURL) StartCopy(ctx context.Context, source url.URL, metadata Metadata) (*FileStartCopyResponse, error) {
	return f.fileClient.StartCopy(ctx, source.String(), nil, metadata)
}

// AbortCopy stops a pending copy that was previously started and leaves a destination file with 0 length and metadata.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/abort-copy-file.
func (f FileURL) AbortCopy(ctx context.Context, copyID string) (*FileAbortCopyResponse, error) {
	return f.fileClient.AbortCopy(ctx, copyID, "abort", nil)
}

// toRange makes range string adhere to REST API.
// A count of zero means count of bytes from offset to the end of file.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/specifying-the-range-header-for-file-service-operations.
func toRange(offset int64, count int64) *string {
	endRange := ""
	if count != 0 {
		endRange = strconv.FormatInt(offset+count-1, 10)
	}
	r := fmt.Sprintf("bytes=%d-%s", offset, endRange)
	return &r
}

// FileRange defines a range of bytes within a file, starting at Offset and ending
// at Offset+Count-1 inclusively. Use a zero-value FileRange to indicate the entire file..
type FileRange struct {
	Offset int64
	Count  int64
}

func (dr *FileRange) pointers() *string {
	if dr.Offset < 0 {
		panic("The file's range Offset must be >= 0")
	}
	if dr.Count < 0 {
		panic("The file's range Count must be >= 0")
	}
	if dr.Offset == 0 && dr.Count == 0 {
		return nil
	}

	return toRange(dr.Offset, dr.Count)
}

// Download downloads data start from offset with count bytes.
// A count of zero means count of bytes from offset to the end of file.
// If both offset and count is zero, entire file will be downloaded.
// The response also includes the file's properties.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/get-file.
func (f FileURL) Download(ctx context.Context, offset int64, count int64, rangeGetContentMD5 bool) (*DownloadResponse, error) {
	var xRangeGetContentMD5 *bool
	if rangeGetContentMD5 {
		xRangeGetContentMD5 = &rangeGetContentMD5
	}
	return f.fileClient.Download(ctx, nil, (&FileRange{Offset: offset, Count: count}).pointers(), xRangeGetContentMD5)
}

//=====================================================
//=====================================================
type DownloadOption struct {
	StreamReadRetryCount int64
	doInjectError        bool
	doInjectErrorRound   int64
}

func (f FileURL) DownloadToStream(ctx context.Context, offset int64, count int64, rangeGetContentMD5 bool, option DownloadOption) (*DownloadResponse, error) {
	var xRangeGetContentMD5 *bool
	if rangeGetContentMD5 {
		xRangeGetContentMD5 = &rangeGetContentMD5
	}

	downloadResponse, error := f.fileClient.Download(ctx, nil, (&FileRange{Offset: offset, Count: count}).pointers(), xRangeGetContentMD5)

	copy := *downloadResponse.Response()
	if downloadResponse != nil && downloadResponse.Body() != nil {
		retryStream := retryStream2{
			ctx:            ctx,
			fileURL:        f,
			offset:         offset,
			count:          count,
			downloadOption: option,
			response:       &copy,
			eTag:           downloadResponse.ETag()}
		downloadResponse.setBody(&retryStream)
	}

	return downloadResponse, error
}

func (gr DownloadResponse) setBody(readCloser io.ReadCloser) {
	gr.rawResponse.Body = readCloser
}

// TODO: Name it as retryStream2, until decision finalized, and the retryStream in highlevel.go is removed.
type retryStream2 struct {
	ctx            context.Context
	fileURL        FileURL
	offset, count  int64
	downloadOption DownloadOption
	response       *http.Response
	eTag           ETag
}

func (s *retryStream2) Read(p []byte) (n int, err error) {
	try := int64(0)
	for ; try <= s.downloadOption.StreamReadRetryCount; try++ {
		//fmt.Println(try) // TODO: Just for fun.

		// TODO: Ensure whether response could be nil, when there is no error returned.
		n, err := s.response.Body.Read(p) // Read from the stream

		if s.downloadOption.doInjectError && try == s.downloadOption.doInjectErrorRound {
			err = &net.DNSError{IsTemporary: true}
		}

		if err == nil || err == io.EOF { // We successfully read data or end EOF
			s.offset += int64(n) // Increments the start offset in case we need to make a new HTTP request in the future
			if s.count != 0 {
				s.count -= int64(n) // Decrement the count in case we need to make a new HTTP request in the future
			}
			return n, err // Return the Read result to the caller
		}
		// Something went wrong; our stream is no longer good, close it.
		s.Close()
		s.response = nil

		// Check the retry count and error code, and decide whether to retry.
		if try == s.downloadOption.StreamReadRetryCount {
			return n, err // No retry, or retry exhausted
		} else if netErr, ok := err.(net.Error); ok {
			if !netErr.Timeout() && !netErr.Temporary() {
				return n, err // Not retryable
			}
		} else {
			return n, err // Not retryable, just return
		}

		// Do retry and try to get a response stream to read from.
		response, err := s.fileURL.Download(s.ctx, s.offset, s.count, false)
		if err != nil {
			return 0, err
		}
		if response.ETag() != ETag("") && response.ETag() != s.eTag {
			return 0, fmt.Errorf("invalid status, source file has been changed, please restart the download")
		}

		// Successful GET; this is the network stream we'll read from
		s.response = response.Response()

		// Loop around and try to read from this stream
	}

	if s.downloadOption.doInjectError &&
		s.downloadOption.doInjectErrorRound <= s.downloadOption.StreamReadRetryCount &&
		s.downloadOption.doInjectErrorRound > 0 &&
		try < s.downloadOption.doInjectErrorRound {
		panic("invalid status, internal error, stream read retry is not working properly.")
	}

	return 0, nil // The compiler is wrong; we never actually get here
}

func (s *retryStream2) Close() error {
	if s.response != nil && s.response.Body != nil {
		return s.response.Body.Close()
	}
	return nil
}

//==========================================================================================
//==========================================================================================

// Delete immediately removes the file from the storage account.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/delete-file2.
func (f FileURL) Delete(ctx context.Context) (*FileDeleteResponse, error) {
	return f.fileClient.Delete(ctx, nil)
}

// GetProperties returns the file's metadata and properties.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/get-file-properties.
func (f FileURL) GetProperties(ctx context.Context) (*FileGetPropertiesResponse, error) {
	return f.fileClient.GetProperties(ctx, nil, nil)
}

// SetHTTPHeaders sets file's system properties.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/set-file-properties.
func (f FileURL) SetHTTPHeaders(ctx context.Context, h FileHTTPHeaders) (*FileSetHTTPHeadersResponse, error) {
	return f.fileClient.SetHTTPHeaders(ctx, nil,
		nil, &h.ContentType, &h.ContentEncoding, &h.ContentLanguage, &h.CacheControl, h.contentMD5Pointer(), &h.ContentDisposition)
}

// SetMetadata sets a file's metadata.
// https://docs.microsoft.com/rest/api/storageservices/set-file-metadata.
func (f FileURL) SetMetadata(ctx context.Context, metadata Metadata) (*FileSetMetadataResponse, error) {
	return f.fileClient.SetMetadata(ctx, nil, metadata)
}

// Resize resizes the file to the specified size.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/set-file-properties.
func (f FileURL) Resize(ctx context.Context, length int64) (*FileSetHTTPHeadersResponse, error) {
	return f.fileClient.SetHTTPHeaders(ctx, nil,
		&length, nil, nil, nil, nil, nil, nil)
}

// getStreamSize gets the size of current stream in bytes.
// When there is error, -1 would be returned for size. Please check error for error details.
func getStreamSize(s io.Seeker) (int64, error) {
	size, err := s.Seek(0, io.SeekEnd)
	if err != nil {
		return -1, err
	}

	_, err = s.Seek(0, io.SeekStart)
	if err != nil {
		return -1, err
	}

	return size, nil
}

// UploadRange writes bytes to a file.
// offset indiciates the offset at which to begin writing, in bytes.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/put-range.
func (f FileURL) UploadRange(ctx context.Context, offset int64, body io.ReadSeeker) (*FileUploadRangeResponse, error) {
	if offset < 0 {
		panic("offset must be >= 0")
	}
	if body == nil {
		panic("body must not be nil")
	}

	validateSeekableStreamAt0(body)

	size, err := getStreamSize(body)
	if err != nil {
		panic(err)
	}

	// TransactionalContentMD5 isn't supported in convenience layer.
	return f.fileClient.UploadRange(ctx, *toRange(offset, size), FileRangeWriteUpdate, size, body, nil, nil)
}

// ClearRange clears the specified range and releases the space used in storage for that range.
// The range composed is from offset to offset+count-1.
// If the range specified by offset and count is not 512-byte aligned, the operation will write zeros to
// the start or end of the range that is not 512-byte aligned and free the rest of the range inside that is 512-byte aligned.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/put-range.
func (f FileURL) ClearRange(ctx context.Context, offset int64, count int64) (*FileUploadRangeResponse, error) {
	if offset < 0 {
		panic("offset must be >= 0")
	}
	if count <= 0 {
		panic("count must be > 0")
	}

	return f.fileClient.UploadRange(ctx, *toRange(offset, count), FileRangeWriteClear, 0, nil, nil, nil)
}

// GetRangeList returns the list of valid ranges for a file.
// Use a zero-value count to indicate the left part of file start from offset.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/list-ranges.
func (f FileURL) GetRangeList(ctx context.Context, offset int64, count int64) (*Ranges, error) {
	return f.fileClient.GetRangeList(ctx, nil, nil, (&FileRange{Offset: offset, Count: count}).pointers())
}

func validateSeekableStreamAt0(body io.ReadSeeker) {
	if body == nil { // nil body's are "logically" seekable to 0
		return
	}
	if pos, err := body.Seek(0, io.SeekCurrent); pos != 0 || err != nil {
		if err != nil {
			panic(err)
		}
		panic(errors.New("stream must be set to position 0"))
	}
}
