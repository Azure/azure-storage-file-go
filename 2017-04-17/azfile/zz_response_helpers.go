package azfile

import (
	"crypto/md5"
	"encoding/base64"
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
func (fgr DownloadResponse) NewHTTPHeaders() FileHTTPHeaders {
	return FileHTTPHeaders{
		ContentType:        fgr.ContentType(),
		ContentEncoding:    fgr.ContentEncoding(),
		ContentLanguage:    fgr.ContentLanguage(),
		ContentDisposition: fgr.ContentDisposition(),
		CacheControl:       fgr.CacheControl(),
		ContentMD5:         fgr.ContentMD5(),
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

// FileContentMD5 returns the value for header x-ms-content-md5.
func (fgr DownloadResponse) FileContentMD5() [md5.Size]byte {
	return md5StringToMD5(fgr.rawResponse.Header.Get("x-ms-content-md5"))
}

// ContentMD5 returns the value for header Content-MD5.
func (fgr DownloadResponse) ContentMD5() [md5.Size]byte {
	return md5StringToMD5(fgr.rawResponse.Header.Get("Content-MD5"))
}
