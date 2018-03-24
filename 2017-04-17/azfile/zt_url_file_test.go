package azfile_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-file-go/2017-04-17/azfile"
	chk "gopkg.in/check.v1" // go get gopkg.in/check.v1
)

type FileURLSuite struct{}

var _ = chk.Suite(&FileURLSuite{})

func delFile(c *chk.C, file azfile.FileURL) {
	resp, err := file.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Response().StatusCode, chk.Equals, 202)
}

func getReaderToRandomBytes(n int) *bytes.Reader {
	r, _ := getRandomDataAndReader(n)
	return r
}

func getRandomDataAndReader(n int) (*bytes.Reader, []byte) {
	data := make([]byte, n, n)
	for i := 0; i < n; i++ {
		data[i] = byte(i)
	}
	return bytes.NewReader(data), data
}

func (b *FileURLSuite) TestCreateDelete(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	// Create and delete file in root directory.
	file := share.NewRootDirectoryURL().NewFileURL(generateFileName())

	cResp, err := file.Create(context.Background(), 0, azfile.FileHTTPHeaders{}, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.Response().StatusCode, chk.Equals, 201)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(cResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(cResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date().IsZero(), chk.Equals, false)
	c.Assert(cResp.IsServerEncrypted(), chk.NotNil)

	delResp, err := file.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(delResp.Response().StatusCode, chk.Equals, 202)
	c.Assert(delResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(delResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(delResp.Date().IsZero(), chk.Equals, false)

	dir, _ := createNewDirectoryFromShare(c, share)
	defer delDirectory(c, dir)

	// Create and delete file in named directory.
	file = dir.NewFileURL(generateFileName())

	cResp, err = file.Create(context.Background(), 0, azfile.FileHTTPHeaders{}, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.Response().StatusCode, chk.Equals, 201)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(cResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(cResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date().IsZero(), chk.Equals, false)
	c.Assert(cResp.IsServerEncrypted(), chk.NotNil)

	delResp, err = file.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(delResp.Response().StatusCode, chk.Equals, 202)
	c.Assert(delResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(delResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(delResp.Date().IsZero(), chk.Equals, false)
}

func (b *FileURLSuite) TestGetSetProperties(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	file, _ := createNewFileFromShare(c, share, 0)
	defer delFile(c, file)

	md5Str := "MDAwMDAwMDA="
	var testMd5 [md5.Size]byte
	copy(testMd5[:], md5Str)

	properties := azfile.FileHTTPHeaders{
		ContentType:        "text/html",
		ContentEncoding:    "gzip",
		ContentLanguage:    "tr,en",
		ContentMD5:         testMd5,
		CacheControl:       "no-transform",
		ContentDisposition: "attachment",
	}
	setResp, err := file.SetHTTPHeaders(context.Background(), properties)
	c.Assert(err, chk.IsNil)
	c.Assert(setResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(setResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(setResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(setResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(setResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(setResp.Date().IsZero(), chk.Equals, false)
	c.Assert(setResp.IsServerEncrypted(), chk.NotNil)

	getResp, err := file.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(getResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(setResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(getResp.FileType(), chk.Equals, "File")

	c.Assert(getResp.ContentType(), chk.Equals, properties.ContentType)
	c.Assert(getResp.ContentEncoding(), chk.Equals, properties.ContentEncoding)
	c.Assert(getResp.ContentLanguage(), chk.Equals, properties.ContentLanguage)
	c.Assert(getResp.ContentMD5(), chk.DeepEquals, properties.ContentMD5)
	c.Assert(getResp.CacheControl(), chk.Equals, properties.CacheControl)
	c.Assert(getResp.ContentDisposition(), chk.Equals, properties.ContentDisposition)
	c.Assert(getResp.ContentLength(), chk.Equals, int64(0))

	c.Assert(getResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(getResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(getResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(getResp.Date().IsZero(), chk.Equals, false)
	c.Assert(getResp.IsServerEncrypted(), chk.NotNil)
}

func (b *FileURLSuite) TestGetSetMetadata(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	file, _ := createNewFileFromShare(c, share, 0)
	defer delFile(c, file)

	metadata := azfile.Metadata{
		"foo": "foovalue",
		"bar": "barvalue",
	}
	setResp, err := file.SetMetadata(context.Background(), metadata)
	c.Assert(err, chk.IsNil)
	c.Assert(setResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(setResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(setResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(setResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(setResp.Date().IsZero(), chk.Equals, false)
	c.Assert(setResp.IsServerEncrypted(), chk.NotNil)

	getResp, err := file.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(getResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(getResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(getResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(getResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(getResp.Date().IsZero(), chk.Equals, false)
	md := getResp.NewMetadata()
	c.Assert(md, chk.DeepEquals, metadata)
}

func (b *FileURLSuite) TestCopy(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	srcFile, _ := createNewFileFromShare(c, share, 2048)
	defer delFile(c, srcFile)

	destFile, _ := getFileURLFromShare(c, share)
	defer delFile(c, destFile)

	_, err := srcFile.UploadRange(context.Background(), 0, getReaderToRandomBytes(2048))
	c.Assert(err, chk.IsNil)

	copyResp, err := destFile.StartCopy(context.Background(), srcFile.URL(), nil)
	c.Assert(err, chk.IsNil)
	c.Assert(copyResp.Response().StatusCode, chk.Equals, 202)
	c.Assert(copyResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(copyResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(copyResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(copyResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(copyResp.Date().IsZero(), chk.Equals, false)
	c.Assert(copyResp.CopyID(), chk.Not(chk.Equals), "")
	c.Assert(copyResp.CopyStatus(), chk.Not(chk.Equals), "")

	var copyStatus azfile.CopyStatusType
	timeout := time.Duration(2) * time.Minute
	start := time.Now()

	var getResp *azfile.FileGetPropertiesResponse

	for copyStatus != azfile.CopyStatusSuccess && time.Now().Sub(start) < timeout {
		getResp, err = destFile.GetProperties(context.Background())
		c.Assert(err, chk.IsNil)
		c.Assert(getResp.CopyID(), chk.Equals, copyResp.CopyID())
		c.Assert(getResp.CopyStatus(), chk.Not(chk.Equals), azfile.CopyStatusNone)
		c.Assert(getResp.CopySource(), chk.Equals, srcFile.String())
		copyStatus = getResp.CopyStatus()

		time.Sleep(time.Duration(5) * time.Second)
	}

	if getResp != nil && getResp.CopyStatus() == azfile.CopyStatusSuccess {
		// Abort will fail after copy finished
		abortResp, err := destFile.AbortCopy(context.Background(), copyResp.CopyID())
		c.Assert(err, chk.NotNil)
		c.Assert(abortResp, chk.IsNil)
		se, ok := err.(azfile.StorageError)
		c.Assert(ok, chk.Equals, true)
		c.Assert(se.Response().StatusCode, chk.Equals, http.StatusConflict)
	}
}

func (b *FileURLSuite) TestPutGetFileRange(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	file, _ := createNewFileFromShare(c, share, 2048)
	defer delFile(c, file)

	contentR, contentD := getRandomDataAndReader(2048)

	pResp, err := file.UploadRange(context.Background(), 0, contentR)
	c.Assert(err, chk.IsNil)
	c.Assert(pResp.ContentMD5(), chk.Not(chk.Equals), [md5.Size]byte{})
	c.Assert(pResp.StatusCode(), chk.Equals, http.StatusCreated)
	c.Assert(pResp.IsServerEncrypted(), chk.NotNil)
	c.Assert(pResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(pResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(pResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(pResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(pResp.Date().IsZero(), chk.Equals, false)

	// Get with rangeGetContentMD5 enabled.
	// Partial data, check status code 206.
	resp, err := file.Download(context.Background(), 0, 1024, true)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusPartialContent)
	c.Assert(resp.ContentLength(), chk.Equals, int64(1024))
	c.Assert(resp.ContentMD5(), chk.Not(chk.Equals), [md5.Size]byte{})
	c.Assert(resp.ContentType(), chk.Equals, "application/octet-stream")

	download, err := ioutil.ReadAll(resp.Response().Body)
	c.Assert(err, chk.IsNil)
	c.Assert(download, chk.DeepEquals, contentD[:1024])

	// Set ContentMD5 for the entire file.
	_, err = file.SetHTTPHeaders(context.Background(), azfile.FileHTTPHeaders{ContentMD5: pResp.ContentMD5()})
	c.Assert(err, chk.IsNil)

	// Test get with another type of range index, and validate if FileContentMD5 can be get correclty.
	resp, err = file.Download(context.Background(), 1024, 0, false)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusPartialContent)
	c.Assert(resp.ContentLength(), chk.Equals, int64(1024))
	c.Assert(resp.ContentMD5(), chk.Equals, [md5.Size]byte{})
	c.Assert(resp.FileContentMD5(), chk.DeepEquals, pResp.ContentMD5())

	download, err = ioutil.ReadAll(resp.Response().Body)
	c.Assert(err, chk.IsNil)
	c.Assert(download, chk.DeepEquals, contentD[1024:])

	c.Assert(resp.AcceptRanges(), chk.Equals, "bytes")
	c.Assert(resp.CacheControl(), chk.Equals, "")
	c.Assert(resp.ContentDisposition(), chk.Equals, "")
	c.Assert(resp.ContentEncoding(), chk.Equals, "")
	c.Assert(resp.ContentRange(), chk.Equals, "bytes 1024-2047/2048")
	c.Assert(resp.ContentType(), chk.Equals, "") // Note ContentType is set during SetHTTPHeaders, TODO: discuss this behavior with azfile.FileHTTPHeaders.
	c.Assert(resp.CopyCompletionTime().IsZero(), chk.Equals, true)
	c.Assert(resp.CopyID(), chk.Equals, "")
	c.Assert(resp.CopyProgress(), chk.Equals, "")
	c.Assert(resp.CopySource(), chk.Equals, "")
	c.Assert(resp.CopyStatus(), chk.Equals, azfile.CopyStatusNone)
	c.Assert(resp.CopyStatusDescription(), chk.Equals, "")
	c.Assert(resp.Date().IsZero(), chk.Equals, false)
	c.Assert(resp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(resp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(resp.NewMetadata(), chk.DeepEquals, azfile.Metadata{})
	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(resp.Version(), chk.Not(chk.Equals), "")
	c.Assert(resp.IsServerEncrypted(), chk.NotNil)

	// Get entire file, check status code 200.
	resp, err = file.Download(context.Background(), 0, 0, false)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusOK)
	c.Assert(resp.ContentLength(), chk.Equals, int64(2048))
	c.Assert(resp.ContentMD5(), chk.Equals, pResp.ContentMD5())   // Note: This case is inted to get entire file, entire file's MD5 will be returned.
	c.Assert(resp.FileContentMD5(), chk.Equals, [md5.Size]byte{}) // Note: FileContentMD5 is returned, only when range is specified explicitly.

	download, err = ioutil.ReadAll(resp.Response().Body)
	c.Assert(err, chk.IsNil)
	c.Assert(download, chk.DeepEquals, contentD[:])

	c.Assert(resp.AcceptRanges(), chk.Equals, "bytes")
	c.Assert(resp.CacheControl(), chk.Equals, "")
	c.Assert(resp.ContentDisposition(), chk.Equals, "")
	c.Assert(resp.ContentEncoding(), chk.Equals, "")
	c.Assert(resp.ContentRange(), chk.Equals, "") // Note: ContentRange is returned, only when range is specified explicitly.
	c.Assert(resp.ContentType(), chk.Equals, "")
	c.Assert(resp.CopyCompletionTime().IsZero(), chk.Equals, true)
	c.Assert(resp.CopyID(), chk.Equals, "")
	c.Assert(resp.CopyProgress(), chk.Equals, "")
	c.Assert(resp.CopySource(), chk.Equals, "")
	c.Assert(resp.CopyStatus(), chk.Equals, azfile.CopyStatusNone)
	c.Assert(resp.CopyStatusDescription(), chk.Equals, "")
	c.Assert(resp.Date().IsZero(), chk.Equals, false)
	c.Assert(resp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(resp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(resp.NewMetadata(), chk.DeepEquals, azfile.Metadata{})
	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(resp.Version(), chk.Not(chk.Equals), "")
	c.Assert(resp.IsServerEncrypted(), chk.NotNil)
}

func (b *FileURLSuite) TestListRanges(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	file, _ := getFileURLFromShare(c, share)

	fileSize := int64(512 * 10)

	file.Create(context.Background(), fileSize, azfile.FileHTTPHeaders{}, nil)

	defer delFile(c, file)

	putResp, err := file.UploadRange(context.Background(), 0, getReaderToRandomBytes(1024))
	c.Assert(err, chk.IsNil)
	c.Assert(putResp.Response().StatusCode, chk.Equals, 201)
	c.Assert(putResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(putResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(putResp.ContentMD5(), chk.Not(chk.Equals), "")
	c.Assert(putResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(putResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(putResp.Date().IsZero(), chk.Equals, false)

	rangeList, err := file.GetRangeList(context.Background(), 0, 1023)
	c.Assert(err, chk.IsNil)
	c.Assert(rangeList.Response().StatusCode, chk.Equals, 200)
	c.Assert(rangeList.LastModified().IsZero(), chk.Equals, false)
	c.Assert(rangeList.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(rangeList.FileContentLength(), chk.Equals, fileSize)
	c.Assert(rangeList.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(rangeList.Version(), chk.Not(chk.Equals), "")
	c.Assert(rangeList.Date().IsZero(), chk.Equals, false)
	c.Assert(rangeList.Value, chk.HasLen, 1)
	c.Assert(rangeList.Value[0], chk.DeepEquals, azfile.Range{Start: 0, End: 1022})
}

func (b *FileURLSuite) TestClearRange(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	file, _ := createNewFileFromShare(c, share, 4096)
	defer delFile(c, file)

	_, err := file.UploadRange(context.Background(), 2048, getReaderToRandomBytes(2048))
	c.Assert(err, chk.IsNil)

	clearResp, err := file.ClearRange(context.Background(), 2048, 2048)
	c.Assert(err, chk.IsNil)
	c.Assert(clearResp.Response().StatusCode, chk.Equals, 201)

	rangeList, err := file.GetRangeList(context.Background(), 0, 0)
	c.Assert(err, chk.IsNil)
	c.Assert(rangeList.Value, chk.HasLen, 0)
}

func (b *FileURLSuite) TestResizeFile(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	file, _ := createNewFileFromShare(c, share, 1234)

	gResp, err := file.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.ContentLength(), chk.Equals, int64(1234))

	rResp, err := file.Resize(context.Background(), 4096)
	c.Assert(err, chk.IsNil)
	c.Assert(rResp.Response().StatusCode, chk.Equals, 200)

	gResp, err = file.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.ContentLength(), chk.Equals, int64(4096))
}

func (f *FileURLSuite) TestServiceSASShareSAS(c *chk.C) {
	fsu := getFSU()
	share, shareName := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	credential, accountName := getCredential()

	sasQueryParams := azfile.FileSASSignatureValues{
		Protocol:    azfile.SASProtocolHTTPS,
		ExpiryTime:  time.Now().UTC().Add(48 * time.Hour),
		ShareName:   shareName,
		Permissions: azfile.ShareSASPermissions{Create: true, Read: true, Write: true, Delete: true, List: true}.String(),
	}.NewSASQueryParameters(credential)

	qp := sasQueryParams.Encode()

	fileName := "testFile"
	dirName := "testDir"
	fileUrlStr := fmt.Sprintf("https://%s.file.core.windows.net/%s/%s?%s",
		accountName, shareName, fileName, qp)
	fu, _ := url.Parse(fileUrlStr)

	dirUrlStr := fmt.Sprintf("https://%s.file.core.windows.net/%s/%s?%s",
		accountName, shareName, dirName, qp)
	du, _ := url.Parse(dirUrlStr)

	fileURL := azfile.NewFileURL(*fu, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))
	dirURL := azfile.NewDirectoryURL(*du, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))

	s := "Hello"
	_, err := fileURL.Create(ctx, int64(len(s)), azfile.FileHTTPHeaders{}, azfile.Metadata{})
	c.Assert(err, chk.IsNil)
	_, err = fileURL.UploadRange(ctx, 0, bytes.NewReader([]byte(s)))
	c.Assert(err, chk.IsNil)
	_, err = fileURL.Download(ctx, 0, 0, false)
	c.Assert(err, chk.IsNil)
	_, err = fileURL.Delete(ctx)
	c.Assert(err, chk.IsNil)

	_, err = dirURL.Create(ctx, azfile.Metadata{})
	c.Assert(err, chk.IsNil)

	_, err = dirURL.ListFilesAndDirectoriesSegment(ctx, azfile.Marker{}, azfile.ListFilesAndDirectoriesOptions{})
	c.Assert(err, chk.IsNil)
}

func (f *FileURLSuite) TestServiceSASFileSAS(c *chk.C) {
	fsu := getFSU()
	share, shareName := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	credential, accountName := getCredential()

	sasQueryParams := azfile.FileSASSignatureValues{
		Protocol:    azfile.SASProtocolHTTPS,
		ExpiryTime:  time.Now().UTC().Add(48 * time.Hour),
		ShareName:   shareName,
		Permissions: azfile.FileSASPermissions{Create: true, Read: true, Write: true, Delete: true}.String(),
	}.NewSASQueryParameters(credential)

	qp := sasQueryParams.Encode()

	fileName := "testFile"
	urlWithSAS := fmt.Sprintf("https://%s.file.core.windows.net/%s/%s?%s",
		accountName, shareName, fileName, qp)
	u, _ := url.Parse(urlWithSAS)

	fileURL := azfile.NewFileURL(*u, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))

	s := "Hello"
	_, err := fileURL.Create(ctx, int64(len(s)), azfile.FileHTTPHeaders{}, azfile.Metadata{})
	c.Assert(err, chk.IsNil)
	_, err = fileURL.UploadRange(ctx, 0, bytes.NewReader([]byte(s)))
	c.Assert(err, chk.IsNil)
	_, err = fileURL.Download(ctx, 0, 0, false)
	c.Assert(err, chk.IsNil)
	_, err = fileURL.Delete(ctx)
	c.Assert(err, chk.IsNil)
}

// TODO: Share snapshot tests for get Properties, Metadata and GetRangeList.
