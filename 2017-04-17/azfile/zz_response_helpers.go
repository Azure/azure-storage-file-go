package azfile

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"net/http"
	"time"
)

// FileHTTPHeaders contains read/writeable file properties.
type FileHTTPHeaders struct {
	ContentType        string
	ContentMD5         [md5.Size]byte
	ContentEncoding    string
	ContentLanguage    string
	ContentDisposition string
	CacheControl       string
}

func (h FileHTTPHeaders) contentMD5Pointer() *string {
	if h.ContentMD5 == [md5.Size]byte{} {
		return nil
	}
	str := base64.StdEncoding.EncodeToString(h.ContentMD5[:])
	return &str
}

// NewHTTPHeaders returns the user-modifiable properties for this file.
func (dr DownloadResponse) NewHTTPHeaders() FileHTTPHeaders {
	return FileHTTPHeaders{
		ContentType:        dr.ContentType(),
		ContentEncoding:    dr.ContentEncoding(),
		ContentLanguage:    dr.ContentLanguage(),
		ContentDisposition: dr.ContentDisposition(),
		CacheControl:       dr.CacheControl(),
		ContentMD5:         dr.ContentMD5(),
	}
}

// NewHTTPHeaders returns the user-modifiable properties for this file.
func (fgpr FileGetPropertiesResponse) NewHTTPHeaders() FileHTTPHeaders {
	return FileHTTPHeaders{
		ContentType:        fgpr.ContentType(),
		ContentEncoding:    fgpr.ContentEncoding(),
		ContentLanguage:    fgpr.ContentLanguage(),
		ContentDisposition: fgpr.ContentDisposition(),
		CacheControl:       fgpr.CacheControl(),
		ContentMD5:         fgpr.ContentMD5(),
	}
}

func md5StringToMD5(md5String string) (hash [md5.Size]byte) {
	if md5String == "" {
		return
	}
	md5Slice, err := base64.StdEncoding.DecodeString(md5String)
	if err != nil {
		panic(err)
	}
	copy(hash[:], md5Slice)
	return
}

// ContentMD5 returns the value for header Content-MD5.
func (fgpr FileGetPropertiesResponse) ContentMD5() [md5.Size]byte {
	return md5StringToMD5(fgpr.rawResponse.Header.Get("Content-MD5"))
}

// ContentMD5 returns the value for header Content-MD5.
func (bpr FileUploadRangeResponse) ContentMD5() [md5.Size]byte {
	return md5StringToMD5(bpr.rawResponse.Header.Get("Content-MD5"))
}

// DownloadResponse wraps AutoRest generated downloadResponse and helps to provide info for retry.
type DownloadResponse struct {
	dr *downloadResponse

	// Fields need for retry.
	ctx  context.Context
	f    FileURL
	info HTTPGetterInfo
}

// Response returns the raw HTTP response object.
func (dr DownloadResponse) Response() *http.Response {
	return dr.dr.Response()
}

// StatusCode returns the HTTP status code of the response, e.g. 200.
func (dr DownloadResponse) StatusCode() int {
	return dr.dr.StatusCode()
}

// Status returns the HTTP status message of the response, e.g. "200 OK".
func (dr DownloadResponse) Status() string {
	return dr.dr.Status()
}

// AcceptRanges returns the value for header Accept-Ranges.
func (dr DownloadResponse) AcceptRanges() string {
	return dr.dr.AcceptRanges()
}

// CacheControl returns the value for header Cache-Control.
func (dr DownloadResponse) CacheControl() string {
	return dr.dr.CacheControl()
}

// ContentDisposition returns the value for header Content-Disposition.
func (dr DownloadResponse) ContentDisposition() string {
	return dr.dr.ContentDisposition()
}

// ContentEncoding returns the value for header Content-Encoding.
func (dr DownloadResponse) ContentEncoding() string {
	return dr.dr.ContentEncoding()
}

// ContentLanguage returns the value for header Content-Language.
func (dr DownloadResponse) ContentLanguage() string {
	return dr.dr.ContentLanguage()
}

// ContentLength returns the value for header Content-Length.
func (dr DownloadResponse) ContentLength() int64 {
	return dr.dr.ContentLength()
}

// ContentRange returns the value for header Content-Range.
func (dr DownloadResponse) ContentRange() string {
	return dr.dr.ContentRange()
}

// ContentType returns the value for header Content-Type.
func (dr DownloadResponse) ContentType() string {
	return dr.dr.ContentType()
}

// CopyCompletionTime returns the value for header x-ms-copy-completion-time.
func (dr DownloadResponse) CopyCompletionTime() time.Time {
	return dr.dr.CopyCompletionTime()
}

// CopyID returns the value for header x-ms-copy-id.
func (dr DownloadResponse) CopyID() string {
	return dr.dr.CopyID()
}

// CopyProgress returns the value for header x-ms-copy-progress.
func (dr DownloadResponse) CopyProgress() string {
	return dr.dr.CopyProgress()
}

// CopySource returns the value for header x-ms-copy-source.
func (dr DownloadResponse) CopySource() string {
	return dr.dr.CopySource()
}

// CopyStatus returns the value for header x-ms-copy-status.
func (dr DownloadResponse) CopyStatus() CopyStatusType {
	return dr.dr.CopyStatus()
}

// CopyStatusDescription returns the value for header x-ms-copy-status-description.
func (dr DownloadResponse) CopyStatusDescription() string {
	return dr.dr.CopyStatusDescription()
}

// Date returns the value for header Date.
func (dr DownloadResponse) Date() time.Time {
	return dr.dr.Date()
}

// ETag returns the value for header ETag.
func (dr DownloadResponse) ETag() ETag {
	return dr.dr.ETag()
}

// IsServerEncrypted returns the value for header x-ms-server-encrypted.
func (dr DownloadResponse) IsServerEncrypted() string {
	return dr.dr.IsServerEncrypted()
}

// LastModified returns the value for header Last-Modified.
func (dr DownloadResponse) LastModified() time.Time {
	return dr.dr.LastModified()
}

// RequestID returns the value for header x-ms-request-id.
func (dr DownloadResponse) RequestID() string {
	return dr.dr.RequestID()
}

// Version returns the value for header x-ms-version.
func (dr DownloadResponse) Version() string {
	return dr.dr.Version()
}

// NewMetadata returns user-defined key/value pairs.
func (dr DownloadResponse) NewMetadata() Metadata {
	return dr.dr.NewMetadata()
}

// FileContentMD5 returns the value for header x-ms-content-md5.
func (dr DownloadResponse) FileContentMD5() [md5.Size]byte {
	return md5StringToMD5(dr.dr.rawResponse.Header.Get("x-ms-content-md5"))
}

// ContentMD5 returns the value for header Content-MD5.
func (dr DownloadResponse) ContentMD5() [md5.Size]byte {
	return md5StringToMD5(dr.dr.rawResponse.Header.Get("Content-MD5"))
}
