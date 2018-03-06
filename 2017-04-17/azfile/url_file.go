package azfile

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

const (
	fileType = "file"
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
func (f FileURL) WithSnapshot(shareSnapshot time.Time) FileURL {
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
func (f FileURL) StartCopy(ctx context.Context, source url.URL, metadata Metadata) (*FileCopyResponse, error) {
	return f.fileClient.Copy(ctx, source.String(), nil, metadata)
}

// AbortCopy stops a pending copy that was previously started and leaves a destination file with 0 length and metadata.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/abort-copy-file.
func (f FileURL) AbortCopy(ctx context.Context, copyID string) (*FileAbortCopyResponse, error) {
	return f.fileClient.AbortCopy(ctx, copyID, "abort", nil)
}

// FileRange defines a range of bytes within a file, starting at Offset and ending
// at Offset+Count-1 inclusively. Use a zero-value FileRange to indicate the entire file.
// TODO: FileRange uses Offset + Count, this is different from Range which contains StartOffset and EndOffset(inclusive).
// 		 There is possiblity to use these two concepts incorrectly for user.
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
	endRange := ""
	if dr.Count > 0 {
		endRange = strconv.FormatInt((dr.Offset+dr.Count)-1, 10)
	}
	dataRange := fmt.Sprintf("bytes=%v-%s", dr.Offset, endRange)
	return &dataRange
}

func (r *Range) String() string {
	if r.Start < 0 {
		panic("Range's Start value must be greater than or equal to 0")
	}
	if r.End <= 0 {
		panic("Range's End value must be greater than 0")
	}
	if r.End <= r.Start {
		panic("Range's End value must be after the start")
	}
	asString := fmt.Sprintf("bytes=%v-%v", r.Start, r.End)
	return asString
}

// GetFile reads a range of bytes from a file. The response also includes the file's properties and metadata.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/get-file.
func (f FileURL) GetFile(ctx context.Context, fileRange FileRange, rangeGetContentMD5 bool) (*GetResponse, error) {
	var xRangeGetContentMD5 *bool
	if rangeGetContentMD5 {
		xRangeGetContentMD5 = &rangeGetContentMD5
	}
	return f.fileClient.Get(ctx, nil, fileRange.pointers(), xRangeGetContentMD5)
}

// Delete immediately removes the file from the storage account.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/delete-file2.
func (f FileURL) Delete(ctx context.Context) (*FileDeleteResponse, error) {
	return f.fileClient.Delete(ctx, nil)
}

// GetPropertiesAndMetadata returns the file's metadata and properties.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/get-file-properties.
func (f FileURL) GetPropertiesAndMetadata(ctx context.Context) (*FileGetPropertiesResponse, error) {
	return f.fileClient.GetProperties(ctx, nil, nil)
}

// SetProperties sets file's system properties.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/set-file-properties.
func (f FileURL) SetProperties(ctx context.Context, h FileHTTPHeaders) (*FileSetPropertiesResponse, error) {
	return f.fileClient.SetProperties(ctx, nil,
		nil, &h.ContentType, &h.ContentEncoding, &h.ContentLanguage, &h.CacheControl, h.contentMD5Pointer(), &h.ContentDisposition)
}

// SetMetadata sets a file's metadata.
// https://docs.microsoft.com/rest/api/storageservices/set-file-metadata.
func (f FileURL) SetMetadata(ctx context.Context, metadata Metadata) (*FileSetMetadataResponse, error) {
	return f.fileClient.SetMetadata(ctx, nil, metadata)
}

// Resize resizes the file to the specified size.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/set-file-properties.
func (f FileURL) Resize(ctx context.Context, length int64) (*FileSetPropertiesResponse, error) {
	return f.fileClient.SetProperties(ctx, nil,
		&length, nil, nil, nil, nil, nil, nil)
}

// PutRange writes a range of bytes to a file.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/put-range.
func (f FileURL) PutRange(ctx context.Context, r Range, body io.ReadSeeker) (*FilePutRangeResponse, error) {
	size := r.End - r.Start + 1
	// TransactionalContentMD5 isn't supported in convenience layer.
	return f.fileClient.PutRange(ctx, r.String(), FileRangeWriteUpdate, size, body, nil, nil)
}

// ClearRange clears the specified range and releases the space used in storage for that range.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/put-range.
func (f FileURL) ClearRange(ctx context.Context, r Range) (*FilePutRangeResponse, error) {
	return f.fileClient.PutRange(ctx, r.String(), FileRangeWriteClear, 0, nil, nil, nil)
}

// ListRanges returns the list of valid ranges for a file. Use a zero-value FileRange to indicate the entire file.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/list-ranges.
func (f FileURL) ListRanges(ctx context.Context, fr FileRange) (*Ranges, error) {
	return f.fileClient.ListRanges(ctx, nil, nil, fr.pointers())
}
