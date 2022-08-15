package azfile_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-storage-file-go/azfile"
	chk "gopkg.in/check.v1" // go get gopkg.in/check.v1
)

type FileURLSuite struct{}

var _ = chk.Suite(&FileURLSuite{})

const (
	testFileRangeSize         = 512           // Use this number considering clear range's function
	fileShareMaxQuota         = 5120          // Size is in GB (Service Version 2020-02-10)
	fileMaxAllowedSizeInBytes = 4398046511104 // 4 TiB (Service Version 2020-02-10)
)

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

func (s *FileURLSuite) TestFileWithNewPipeline(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := getShareURL(c, fsu)
	fileURL := shareURL.NewRootDirectoryURL().NewFileURL(filePrefix)

	newfileURL := fileURL.WithPipeline(testPipeline{})
	_, err := newfileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, azfile.Metadata{})
	c.Assert(err, chk.NotNil)
	c.Assert(err.Error(), chk.Equals, testPipelineMessage)
}

// func (s *FileURLSuite) TestFileNewFileURLNegative(c *chk.C) {
// 	c.Assert(func() { azfile.NewFileURL(url.URL{}, nil) }, chk.Panics, "p can't be nil")
// }

func (s *FileURLSuite) TestFileCreateDeleteDefault(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	// Create and delete file in root directory.
	file := shareURL.NewRootDirectoryURL().NewFileURL(generateFileName())

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

	dir, _ := createNewDirectoryFromShare(c, shareURL)
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

func (s *FileURLSuite) TestFileCreateNonDefaultMetadataNonEmpty(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := getFileURLFromShare(c, shareURL)

	_, err := fileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, basicMetadata)

	resp, err := fileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.NewMetadata(), chk.DeepEquals, basicMetadata)
}

func (s *FileURLSuite) TestFileCreateNonDefaultHTTPHeaders(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := getFileURLFromShare(c, shareURL)

	_, err := fileURL.Create(ctx, 0, basicHeaders, nil)
	c.Assert(err, chk.IsNil)

	resp, err := fileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	h := resp.NewHTTPHeaders()
	c.Assert(h, chk.DeepEquals, basicHeaders)
}

func (s *FileURLSuite) TestFileCreateNegativeMetadataInvalid(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := getFileURLFromShare(c, shareURL)

	_, err := fileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, azfile.Metadata{"!@#$%^&*()": "!@#$%^&*()"})
	c.Assert(err, chk.NotNil)
}

func (s *FileURLSuite) TestFileGetSetPropertiesNonDefault(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
	defer delFile(c, fileURL)

	md5Str := "MDAwMDAwMDA="
	var testMd5 []byte
	copy(testMd5[:], md5Str)

	attribs := azfile.FileAttributeTemporary.Add(azfile.FileAttributeHidden)
	creationTime := time.Now().Add(-time.Hour)
	lastWriteTime := time.Now().Add(-time.Minute * 15)

	// Format and re-parse the times so we have the same precision
	creationTime, err := time.Parse(azfile.ISO8601, creationTime.Format(azfile.ISO8601))
	c.Assert(err, chk.IsNil)
	lastWriteTime, err = time.Parse(azfile.ISO8601, lastWriteTime.Format(azfile.ISO8601))
	c.Assert(err, chk.IsNil)

	properties := azfile.FileHTTPHeaders{
		ContentType:        "text/html",
		ContentEncoding:    "gzip",
		ContentLanguage:    "tr,en",
		ContentMD5:         testMd5,
		CacheControl:       "no-transform",
		ContentDisposition: "attachment",
		SMBProperties: azfile.SMBProperties{
			PermissionString:  &sampleSDDL, // Because our permission string is less than 9KB, it can be used here.
			FileAttributes:    &attribs,
			FileCreationTime:  &creationTime,
			FileLastWriteTime: &lastWriteTime,
		},
	}
	setResp, err := fileURL.SetHTTPHeaders(context.Background(), properties)
	c.Assert(err, chk.IsNil)
	c.Assert(setResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(setResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(setResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(setResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(setResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(setResp.Date().IsZero(), chk.Equals, false)
	c.Assert(setResp.IsServerEncrypted(), chk.NotNil)

	getResp, err := fileURL.GetProperties(context.Background())
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
	// We'll just ensure a permission exists, no need to test overlapping functionality.
	c.Assert(getResp.FilePermissionKey(), chk.Not(chk.Equals), "")
	// Ensure our attributes and other properties (after parsing) are equivalent to our original
	// There's an overlapping test for this in ntfs_property_bitflags_test.go, but it doesn't hurt to test it alongside other things.
	c.Assert(azfile.ParseFileAttributeFlagsString(getResp.FileAttributes()), chk.Equals, attribs)
	// Adapt to time.Time
	adapter := azfile.SMBPropertyAdapter{PropertySource: getResp}
	c.Log("Original last write time: ", lastWriteTime, " new time: ", adapter.FileLastWriteTime())
	c.Assert(adapter.FileLastWriteTime().Equal(lastWriteTime), chk.Equals, true)
	c.Log("Original creation time: ", creationTime, " new time: ", adapter.FileCreationTime())
	c.Assert(adapter.FileCreationTime().Equal(creationTime), chk.Equals, true)

	c.Assert(getResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(getResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(getResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(getResp.Date().IsZero(), chk.Equals, false)
	c.Assert(getResp.IsServerEncrypted(), chk.NotNil)
}

func (s *FileURLSuite) TestFilePreservePermissions(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShareWithPermissions(c, shareURL, 0)
	defer delFile(c, fileURL)

	// Grab the original perm key before we set file headers.
	getResp, err := fileURL.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)

	oKey := getResp.FilePermissionKey()
	timeAdapter := azfile.SMBPropertyAdapter{PropertySource: getResp}
	cTime := timeAdapter.FileCreationTime()
	lwTime := timeAdapter.FileLastWriteTime()
	attribs := getResp.FileAttributes()

	md5Str := "MDAwMDAwMDA="
	var testMd5 []byte
	copy(testMd5[:], md5Str)

	properties := azfile.FileHTTPHeaders{
		ContentType:        "text/html",
		ContentEncoding:    "gzip",
		ContentLanguage:    "tr,en",
		ContentMD5:         testMd5,
		CacheControl:       "no-transform",
		ContentDisposition: "attachment",
		SMBProperties:      azfile.SMBProperties{
			// SMBProperties, when options are left nil, leads to preserving.
		},
	}

	setResp, err := fileURL.SetHTTPHeaders(context.Background(), properties)
	c.Assert(err, chk.IsNil)
	c.Assert(setResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(setResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(setResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(setResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(setResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(setResp.Date().IsZero(), chk.Equals, false)
	c.Assert(setResp.IsServerEncrypted(), chk.NotNil)

	getResp, err = fileURL.GetProperties(context.Background())
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
	// Ensure that the permission key gets preserved
	c.Assert(getResp.FilePermissionKey(), chk.Equals, oKey)
	timeAdapter = azfile.SMBPropertyAdapter{PropertySource: getResp}
	c.Log("Original last write time: ", lwTime, " new time: ", timeAdapter.FileLastWriteTime())
	c.Assert(timeAdapter.FileLastWriteTime().Equal(lwTime), chk.Equals, true)
	c.Log("Original creation time: ", cTime, " new time: ", timeAdapter.FileCreationTime())
	c.Assert(timeAdapter.FileCreationTime().Equal(cTime), chk.Equals, true)
	c.Assert(getResp.FileAttributes(), chk.Equals, attribs)

	c.Assert(getResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(getResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(getResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(getResp.Date().IsZero(), chk.Equals, false)
	c.Assert(getResp.IsServerEncrypted(), chk.NotNil)
}

func (s *FileURLSuite) TestFileGetSetPropertiesSnapshot(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionInclude)

	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	md5Str := "MDAwMDAwMDA="
	var testMd5 []byte
	copy(testMd5[:], md5Str)

	properties := azfile.FileHTTPHeaders{
		ContentType:        "text/html",
		ContentEncoding:    "gzip",
		ContentLanguage:    "tr,en",
		ContentMD5:         testMd5,
		CacheControl:       "no-transform",
		ContentDisposition: "attachment",
	}
	setResp, err := fileURL.SetHTTPHeaders(context.Background(), properties)
	c.Assert(err, chk.IsNil)
	c.Assert(setResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(setResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(setResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(setResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(setResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(setResp.Date().IsZero(), chk.Equals, false)
	c.Assert(setResp.IsServerEncrypted(), chk.NotNil)

	metadata := azfile.Metadata{
		"foo": "foovalue",
		"bar": "barvalue",
	}
	setResp2, err := fileURL.SetMetadata(context.Background(), metadata)
	c.Assert(err, chk.IsNil)
	c.Assert(setResp2.Response().StatusCode, chk.Equals, 200)

	resp, _ := shareURL.CreateSnapshot(ctx, azfile.Metadata{})
	snapshotURL := fileURL.WithSnapshot(resp.Snapshot())

	getResp, err := snapshotURL.GetProperties(context.Background())
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
	c.Assert(getResp.NewMetadata(), chk.DeepEquals, metadata)
}

func (s *FileURLSuite) TestGetSetMetadataNonDefault(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	metadata := azfile.Metadata{
		"foo": "foovalue",
		"bar": "barvalue",
	}
	setResp, err := fileURL.SetMetadata(context.Background(), metadata)
	c.Assert(err, chk.IsNil)
	c.Assert(setResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(setResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(setResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(setResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(setResp.Date().IsZero(), chk.Equals, false)
	c.Assert(setResp.IsServerEncrypted(), chk.NotNil)

	getResp, err := fileURL.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(getResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(getResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(getResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(getResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(getResp.Date().IsZero(), chk.Equals, false)
	md := getResp.NewMetadata()
	c.Assert(md, chk.DeepEquals, metadata)
}

func (s *FileURLSuite) TestFileSetMetadataNil(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	_, err := fileURL.SetMetadata(ctx, azfile.Metadata{"not": "nil"})
	c.Assert(err, chk.IsNil)

	_, err = fileURL.SetMetadata(ctx, nil)
	c.Assert(err, chk.IsNil)

	resp, err := fileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.NewMetadata(), chk.HasLen, 0)
}

func (s *FileURLSuite) TestFileSetMetadataDefaultEmpty(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	_, err := fileURL.SetMetadata(ctx, azfile.Metadata{"not": "nil"})
	c.Assert(err, chk.IsNil)

	_, err = fileURL.SetMetadata(ctx, azfile.Metadata{})
	c.Assert(err, chk.IsNil)

	resp, err := fileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.NewMetadata(), chk.HasLen, 0)
}

func (s *FileURLSuite) TestFileSetMetadataInvalidField(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	_, err := fileURL.SetMetadata(ctx, azfile.Metadata{"!@#$%^&*()": "!@#$%^&*()"})
	c.Assert(err, chk.NotNil)
}

func (s *FileURLSuite) TestStartCopyDefault(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	srcFile, _ := createNewFileFromShare(c, shareURL, 2048)
	defer delFile(c, srcFile)

	destFile, _ := getFileURLFromShare(c, shareURL)
	defer delFile(c, destFile)

	_, err := srcFile.UploadRange(context.Background(), 0, getReaderToRandomBytes(2048), nil)
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

func waitForCopy(c *chk.C, copyFileURL azfile.FileURL, fileCopyResponse *azfile.FileStartCopyResponse) {
	status := fileCopyResponse.CopyStatus()
	// Wait for the copy to finish. If the copy takes longer than a minute, we will fail
	start := time.Now()
	for status != azfile.CopyStatusSuccess {
		GetPropertiesResult, _ := copyFileURL.GetProperties(ctx)
		status = GetPropertiesResult.CopyStatus()
		currentTime := time.Now()
		if currentTime.Sub(start) >= time.Minute {
			c.Fail()
		}
	}
}

func (s *FileURLSuite) TestFileStartCopyDestEmpty(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShareWithDefaultData(c, shareURL)
	copyFileURL, _ := getFileURLFromShare(c, shareURL)

	fileCopyResponse, err := copyFileURL.StartCopy(ctx, fileURL.URL(), nil)
	c.Assert(err, chk.IsNil)
	waitForCopy(c, copyFileURL, fileCopyResponse)

	resp, err := copyFileURL.Download(ctx, 0, azfile.CountToEnd, false)
	c.Assert(err, chk.IsNil)

	// Read the file data to verify the copy
	data, _ := ioutil.ReadAll(resp.Response().Body)
	c.Assert(resp.ContentLength(), chk.Equals, int64(len(fileDefaultData)))
	c.Assert(string(data), chk.Equals, fileDefaultData)
	resp.Response().Body.Close()
}

func (s *FileURLSuite) TestFileStartCopyMetadata(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
	copyFileURL, _ := getFileURLFromShare(c, shareURL)

	resp, err := copyFileURL.StartCopy(ctx, fileURL.URL(), basicMetadata)
	c.Assert(err, chk.IsNil)
	waitForCopy(c, copyFileURL, resp)

	resp2, err := copyFileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(resp2.NewMetadata(), chk.DeepEquals, basicMetadata)
}

func (s *FileURLSuite) TestFileStartCopyMetadataNil(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
	copyFileURL, _ := getFileURLFromShare(c, shareURL)

	// Have the destination start with metadata so we ensure the nil metadata passed later takes effect
	_, err := copyFileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, basicMetadata)
	c.Assert(err, chk.IsNil)

	resp, err := copyFileURL.StartCopy(ctx, fileURL.URL(), nil)
	c.Assert(err, chk.IsNil)

	waitForCopy(c, copyFileURL, resp)

	resp2, err := copyFileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(resp2.NewMetadata(), chk.HasLen, 0)
}

func (s *FileURLSuite) TestFileStartCopyMetadataEmpty(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
	copyFileURL, _ := getFileURLFromShare(c, shareURL)

	// Have the destination start with metadata so we ensure the empty metadata passed later takes effect
	_, err := copyFileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, basicMetadata)
	c.Assert(err, chk.IsNil)

	resp, err := copyFileURL.StartCopy(ctx, fileURL.URL(), azfile.Metadata{})
	c.Assert(err, chk.IsNil)

	waitForCopy(c, copyFileURL, resp)

	resp2, err := copyFileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(resp2.NewMetadata(), chk.HasLen, 0)
}

func (s *FileURLSuite) TestFileStartCopyNegativeMetadataInvalidField(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
	copyFileURL, _ := getFileURLFromShare(c, shareURL)

	_, err := copyFileURL.StartCopy(ctx, fileURL.URL(), azfile.Metadata{"!@#$%^&*()": "!@#$%^&*()"})
	c.Assert(err, chk.NotNil)
}

func (s *FileURLSuite) TestFileStartCopySourceNonExistant(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := getFileURLFromShare(c, shareURL)
	copyFileURL, _ := getFileURLFromShare(c, shareURL)

	_, err := copyFileURL.StartCopy(ctx, fileURL.URL(), nil)
	validateStorageError(c, err, azfile.ServiceCodeResourceNotFound)
}

func (s *FileURLSuite) TestFileStartCopyUsingSASSrc(c *chk.C) {
	fsu := getFSU()
	shareURL, shareName := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, fileName := createNewFileFromShareWithDefaultData(c, shareURL)

	// Create sas values for the source file
	credential, _ := getCredential()
	serviceSASValues := azfile.FileSASSignatureValues{Version: "2015-04-05", StartTime: time.Now().Add(-1 * time.Hour).UTC(),
		ExpiryTime: time.Now().Add(time.Hour).UTC(), Permissions: azfile.FileSASPermissions{Read: true, Write: true, Create: true, Delete: true}.String(),
		ShareName: shareName, FilePath: fileName}
	queryParams, err := serviceSASValues.NewSASQueryParameters(credential)
	c.Assert(err, chk.IsNil)

	// Create URLs to the destination file with sas parameters
	sasURL := fileURL.URL()
	sasURL.RawQuery = queryParams.Encode()

	// Create a new container for the destination
	copyShareURL, _ := createNewShare(c, fsu)
	defer delShare(c, copyShareURL, azfile.DeleteSnapshotsOptionNone)
	copyFileURL, _ := getFileURLFromShare(c, copyShareURL)

	resp, err := copyFileURL.StartCopy(ctx, sasURL, nil)
	c.Assert(err, chk.IsNil)

	waitForCopy(c, copyFileURL, resp)

	resp2, err := copyFileURL.Download(ctx, 0, azfile.CountToEnd, false)
	c.Assert(err, chk.IsNil)

	data, err := ioutil.ReadAll(resp2.Response().Body)
	c.Assert(resp2.ContentLength(), chk.Equals, int64(len(fileDefaultData)))
	c.Assert(string(data), chk.Equals, fileDefaultData)
	resp2.Response().Body.Close()
}

func (s *FileURLSuite) TestFileStartCopyUsingSASDest(c *chk.C) {
	fsu := getFSU()
	shareURL, shareName := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, fileName := createNewFileFromShareWithDefaultData(c, shareURL)
	_ = fileURL

	// Generate SAS on the source
	serviceSASValues := azfile.FileSASSignatureValues{ExpiryTime: time.Now().Add(time.Hour).UTC(),
		Permissions: azfile.FileSASPermissions{Read: true, Write: true, Create: true}.String(), ShareName: shareName, FilePath: fileName}
	credentials, _ := getCredential()
	queryParams, err := serviceSASValues.NewSASQueryParameters(credentials)
	c.Assert(err, chk.IsNil)

	copyShareURL, copyShareName := createNewShare(c, fsu)
	defer delShare(c, copyShareURL, azfile.DeleteSnapshotsOptionNone)
	copyFileURL, copyFileName := getFileURLFromShare(c, copyShareURL)

	// Generate Sas for the destination
	copyServiceSASvalues := azfile.FileSASSignatureValues{StartTime: time.Now().Add(-1 * time.Hour).UTC(),
		ExpiryTime: time.Now().Add(time.Hour).UTC(), Permissions: azfile.FileSASPermissions{Read: true, Write: true}.String(),
		ShareName: copyShareName, FilePath: copyFileName}
	copyQueryParams, err := copyServiceSASvalues.NewSASQueryParameters(credentials)
	c.Assert(err, chk.IsNil)

	// Generate anonymous URL to destination with SAS
	anonURL := fsu.URL()
	anonURL.RawQuery = copyQueryParams.Encode()
	anonPipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
	anonFSU := azfile.NewServiceURL(anonURL, anonPipeline)
	anonFileURL := anonFSU.NewShareURL(copyShareName)
	anonfileURL := anonFileURL.NewRootDirectoryURL().NewFileURL(copyFileName)

	// Apply sas to source
	srcFileWithSasURL := fileURL.URL()
	srcFileWithSasURL.RawQuery = queryParams.Encode()

	resp, err := anonfileURL.StartCopy(ctx, srcFileWithSasURL, nil)
	c.Assert(err, chk.IsNil)

	// Allow copy to happen
	waitForCopy(c, anonfileURL, resp)

	resp2, err := copyFileURL.Download(ctx, 0, azfile.CountToEnd, false)
	c.Assert(err, chk.IsNil)

	data, err := ioutil.ReadAll(resp2.Response().Body)
	_, err = resp2.Body(azfile.RetryReaderOptions{}).Read(data)
	c.Assert(resp2.ContentLength(), chk.Equals, int64(len(fileDefaultData)))
	c.Assert(string(data), chk.Equals, fileDefaultData)
	resp2.Body(azfile.RetryReaderOptions{}).Close()
}

func (s *FileURLSuite) TestFileAbortCopyInProgress(c *chk.C) {
	fsu := getFSU()
	shareURL, shareName := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, fileName := getFileURLFromShare(c, shareURL)

	// Create a large file that takes time to copy
	fileSize := 12 * 1024 * 1024
	fileData := make([]byte, fileSize, fileSize)
	for i := range fileData {
		fileData[i] = byte('a' + i%26)
	}
	_, err := fileURL.Create(ctx, int64(fileSize), azfile.FileHTTPHeaders{}, nil)
	c.Assert(err, chk.IsNil)

	_, err = fileURL.UploadRange(ctx, 0, bytes.NewReader(fileData[0:4*1024*1024]), nil)
	c.Assert(err, chk.IsNil)
	_, err = fileURL.UploadRange(ctx, 4*1024*1024, bytes.NewReader(fileData[4*1024*1024:8*1024*1024]), nil)
	c.Assert(err, chk.IsNil)
	_, err = fileURL.UploadRange(ctx, 8*1024*1024, bytes.NewReader(fileData[8*1024*1024:]), nil)
	c.Assert(err, chk.IsNil)
	serviceSASValues := azfile.FileSASSignatureValues{ExpiryTime: time.Now().Add(time.Hour).UTC(),
		Permissions: azfile.FileSASPermissions{Read: true, Write: true, Create: true}.String(), ShareName: shareName, FilePath: fileName}
	credentials, _ := getCredential()
	queryParams, err := serviceSASValues.NewSASQueryParameters(credentials)
	c.Assert(err, chk.IsNil)
	srcFileWithSasURL := fileURL.URL()
	srcFileWithSasURL.RawQuery = queryParams.Encode()

	fsu2, err := getAlternateFSU()
	c.Assert(err, chk.IsNil)
	copyShareURL, _ := createNewShare(c, fsu2)
	copyFileURL, _ := getFileURLFromShare(c, copyShareURL)

	defer delShare(c, copyShareURL, azfile.DeleteSnapshotsOptionNone)

	resp, err := copyFileURL.StartCopy(ctx, srcFileWithSasURL, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.CopyStatus(), chk.Equals, azfile.CopyStatusPending)

	_, err = copyFileURL.AbortCopy(ctx, resp.CopyID())
	if err != nil {
		// If the error is nil, the test continues as normal.
		// If the error is not nil, we want to check if it's because the copy is finished and send a message indicating this.
		c.Assert((err.(azfile.StorageError)).Response().StatusCode, chk.Equals, 409)
		c.Error("The test failed because the copy completed because it was aborted")
	}

	resp2, _ := copyFileURL.GetProperties(ctx)
	c.Assert(resp2.CopyStatus(), chk.Equals, azfile.CopyStatusAborted)
}

func (s *FileURLSuite) TestFileAbortCopyNoCopyStarted(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)

	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	copyFileURL, _ := getFileURLFromShare(c, shareURL)
	_, err := copyFileURL.AbortCopy(ctx, "copynotstarted")
	validateStorageError(c, err, azfile.ServiceCodeInvalidQueryParameterValue)
}

func (s *FileURLSuite) TestResizeFile(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 1234)

	gResp, err := fileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.ContentLength(), chk.Equals, int64(1234))

	rResp, err := fileURL.Resize(context.Background(), 4096)
	c.Assert(err, chk.IsNil)
	c.Assert(rResp.Response().StatusCode, chk.Equals, 200)

	gResp, err = fileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.ContentLength(), chk.Equals, int64(4096))
}

func (s *FileURLSuite) TestFileResizeZero(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 10)

	// The default file is created with size > 0, so this should actually update
	_, err := fileURL.Resize(ctx, 0)
	c.Assert(err, chk.IsNil)

	resp, err := fileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.ContentLength(), chk.Equals, int64(0))
}

func (s *FileURLSuite) TestFileResizeInvalidSizeNegative(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	_, err := fileURL.Resize(ctx, -4)
	c.Assert(err, chk.NotNil)
	sErr := err.(azfile.StorageError)
	c.Assert(sErr.Response().StatusCode, chk.Equals, http.StatusBadRequest)
}

func (f *FileURLSuite) TestServiceSASShareSAS(c *chk.C) {
	fsu := getFSU()
	shareURL, shareName := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	credential, accountName := getCredential()

	sasQueryParams, err := azfile.FileSASSignatureValues{
		Protocol:    azfile.SASProtocolHTTPS,
		ExpiryTime:  time.Now().UTC().Add(48 * time.Hour),
		ShareName:   shareName,
		Permissions: azfile.ShareSASPermissions{Create: true, Read: true, Write: true, Delete: true, List: true}.String(),
	}.NewSASQueryParameters(credential)
	c.Assert(err, chk.IsNil)

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
	_, err = fileURL.Create(ctx, int64(len(s)), azfile.FileHTTPHeaders{}, azfile.Metadata{})
	c.Assert(err, chk.IsNil)
	_, err = fileURL.UploadRange(ctx, 0, bytes.NewReader([]byte(s)), nil)
	c.Assert(err, chk.IsNil)
	_, err = fileURL.Download(ctx, 0, azfile.CountToEnd, false)
	c.Assert(err, chk.IsNil)
	_, err = fileURL.Delete(ctx)
	c.Assert(err, chk.IsNil)

	_, err = dirURL.Create(ctx, azfile.Metadata{}, azfile.SMBProperties{})
	c.Assert(err, chk.IsNil)

	_, err = dirURL.ListFilesAndDirectoriesSegment(ctx, azfile.Marker{}, azfile.ListFilesAndDirectoriesOptions{})
	c.Assert(err, chk.IsNil)
}

func (f *FileURLSuite) TestServiceSASFileSAS(c *chk.C) {
	fsu := getFSU()
	shareURL, shareName := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	credential, accountName := getCredential()

	cacheControlVal := "cache-control-override"
	contentDispositionVal := "content-disposition-override"
	contentEncodingVal := "content-encoding-override"
	contentLanguageVal := "content-language-override"
	contentTypeVal := "content-type-override"

	sasQueryParams, err := azfile.FileSASSignatureValues{
		Protocol:           azfile.SASProtocolHTTPS,
		ExpiryTime:         time.Now().UTC().Add(48 * time.Hour),
		ShareName:          shareName,
		Permissions:        azfile.FileSASPermissions{Create: true, Read: true, Write: true, Delete: true}.String(),
		CacheControl:       cacheControlVal,
		ContentDisposition: contentDispositionVal,
		ContentEncoding:    contentEncodingVal,
		ContentLanguage:    contentLanguageVal,
		ContentType:        contentTypeVal,
	}.NewSASQueryParameters(credential)
	c.Assert(err, chk.IsNil)

	qp := sasQueryParams.Encode()

	fileName := "testFile"
	urlWithSAS := fmt.Sprintf("https://%s.file.core.windows.net/%s/%s?%s",
		accountName, shareName, fileName, qp)
	u, _ := url.Parse(urlWithSAS)

	fileURL := azfile.NewFileURL(*u, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))

	s := "Hello"
	_, err = fileURL.Create(ctx, int64(len(s)), azfile.FileHTTPHeaders{}, azfile.Metadata{})
	c.Assert(err, chk.IsNil)
	_, err = fileURL.UploadRange(ctx, 0, bytes.NewReader([]byte(s)), nil)
	c.Assert(err, chk.IsNil)
	dResp, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
	c.Assert(err, chk.IsNil)
	c.Assert(dResp.CacheControl(), chk.Equals, cacheControlVal)
	c.Assert(dResp.ContentDisposition(), chk.Equals, contentDispositionVal)
	c.Assert(dResp.ContentEncoding(), chk.Equals, contentEncodingVal)
	c.Assert(dResp.ContentLanguage(), chk.Equals, contentLanguageVal)
	c.Assert(dResp.ContentType(), chk.Equals, contentTypeVal)
	_, err = fileURL.Delete(ctx)
	c.Assert(err, chk.IsNil)
}

func (s *FileURLSuite) TestDownloadEmptyZeroSizeFile(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 0)
	defer delFile(c, fileURL)

	// Download entire fileURL, check status code 200.
	resp, err := fileURL.Download(context.Background(), 0, azfile.CountToEnd, false)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusOK)
	c.Assert(resp.ContentLength(), chk.Equals, int64(0))
	c.Assert(resp.FileContentMD5(), chk.IsNil) // Note: FileContentMD5 is returned, only when range is specified explicitly.

	download, err := ioutil.ReadAll(resp.Response().Body)
	c.Assert(err, chk.IsNil)
	c.Assert(download, chk.HasLen, 0)
	c.Assert(resp.AcceptRanges(), chk.Equals, "bytes")
	c.Assert(resp.CacheControl(), chk.Equals, "")
	c.Assert(resp.ContentDisposition(), chk.Equals, "")
	c.Assert(resp.ContentEncoding(), chk.Equals, "")
	c.Assert(resp.ContentRange(), chk.Equals, "") // Note: ContentRange is returned, only when range is specified explicitly.
	c.Assert(resp.ContentType(), chk.Equals, "application/octet-stream")
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

func (s *FileURLSuite) TestUploadDownloadDefaultNonDefaultMD5(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 2048)
	defer delFile(c, fileURL)

	contentR, contentD := getRandomDataAndReader(2048)

	pResp, err := fileURL.UploadRange(context.Background(), 0, contentR, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(pResp.ContentMD5(), chk.NotNil)
	c.Assert(pResp.StatusCode(), chk.Equals, http.StatusCreated)
	c.Assert(pResp.IsServerEncrypted(), chk.NotNil)
	c.Assert(pResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(pResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(pResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(pResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(pResp.Date().IsZero(), chk.Equals, false)

	// Get with rangeGetContentMD5 enabled.
	// Partial data, check status code 206.
	resp, err := fileURL.Download(context.Background(), 0, 1024, true)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusPartialContent)
	c.Assert(resp.ContentLength(), chk.Equals, int64(1024))
	c.Assert(resp.ContentMD5(), chk.NotNil)
	c.Assert(resp.ContentType(), chk.Equals, "application/octet-stream")
	c.Assert(resp.Status(), chk.Not(chk.Equals), "")

	download, err := ioutil.ReadAll(resp.Response().Body)
	c.Assert(err, chk.IsNil)
	c.Assert(download, chk.DeepEquals, contentD[:1024])

	// Set ContentMD5 for the entire file.
	_, err = fileURL.SetHTTPHeaders(context.Background(), azfile.FileHTTPHeaders{ContentMD5: pResp.ContentMD5(), ContentLanguage: "test"})
	c.Assert(err, chk.IsNil)

	// Test get with another type of range index, and validate if FileContentMD5 can be get correclty.
	resp, err = fileURL.Download(context.Background(), 1024, azfile.CountToEnd, false)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusPartialContent)
	c.Assert(resp.ContentLength(), chk.Equals, int64(1024))
	c.Assert(resp.ContentMD5(), chk.IsNil)
	c.Assert(resp.FileContentMD5(), chk.DeepEquals, pResp.ContentMD5())
	c.Assert(resp.ContentLanguage(), chk.Equals, "test")
	// Note: when it's downloading range, range's MD5 is returned, when set rangeGetContentMD5=true, currently set it to false, so should be empty
	c.Assert(resp.NewHTTPHeaders(), chk.DeepEquals, azfile.FileHTTPHeaders{ContentLanguage: "test"})

	download, err = ioutil.ReadAll(resp.Response().Body)
	c.Assert(err, chk.IsNil)
	c.Assert(download, chk.DeepEquals, contentD[1024:])

	c.Assert(resp.AcceptRanges(), chk.Equals, "bytes")
	c.Assert(resp.CacheControl(), chk.Equals, "")
	c.Assert(resp.ContentDisposition(), chk.Equals, "")
	c.Assert(resp.ContentEncoding(), chk.Equals, "")
	c.Assert(resp.ContentRange(), chk.Equals, "bytes 1024-2047/2048")
	c.Assert(resp.ContentType(), chk.Equals, "") // Note ContentType is set to empty during SetHTTPHeaders
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

	// Get entire fileURL, check status code 200.
	resp, err = fileURL.Download(context.Background(), 0, azfile.CountToEnd, false)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusOK)
	c.Assert(resp.ContentLength(), chk.Equals, int64(2048))
	c.Assert(resp.ContentMD5(), chk.DeepEquals, pResp.ContentMD5()) // Note: This case is inted to get entire fileURL, entire file's MD5 will be returned.
	c.Assert(resp.FileContentMD5(), chk.IsNil)                      // Note: FileContentMD5 is returned, only when range is specified explicitly.

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

func (s *FileURLSuite) TestFileDownloadDataNonExistantFile(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := getFileURLFromShare(c, shareURL)

	_, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
	validateStorageError(c, err, azfile.ServiceCodeResourceNotFound)
}

// Don't check offset by design.
// func (s *FileURLSuite) TestFileDownloadDataNegativeOffset(c *chk.C) {
// 	fsu := getFSU()
// 	shareURL, _ := createNewShare(c, fsu)
// 	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
// 	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

// 	_, err := fileURL.Download(ctx, -1, azfile.CountToEnd, false)
// 	c.Assert(err, chk.NotNil)
// 	c.Assert(strings.Contains(err.Error(), "offset must be >= 0"), chk.Equals, true)
// }

func (s *FileURLSuite) TestFileDownloadDataOffsetOutOfRange(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	_, err := fileURL.Download(ctx, int64(len(fileDefaultData)), azfile.CountToEnd, false)
	validateStorageError(c, err, azfile.ServiceCodeInvalidRange)
}

// Don't check count by design.
// func (s *FileURLSuite) TestFileDownloadDataInvalidCount(c *chk.C) {
// 	fsu := getFSU()
// 	shareURL, _ := createNewShare(c, fsu)
// 	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
// 	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

// 	_, err := fileURL.Download(ctx, 0, -100, false)
// 	c.Assert(err, chk.NotNil)
// 	c.Assert(strings.Contains(err.Error(), "count must be >= 0"), chk.Equals, true)
// }

func (s *FileURLSuite) TestFileDownloadDataEntireFile(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShareWithDefaultData(c, shareURL)

	resp, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
	c.Assert(err, chk.IsNil)

	// Specifying a count of 0 results in the value being ignored
	data, err := ioutil.ReadAll(resp.Response().Body)
	c.Assert(err, chk.IsNil)
	c.Assert(string(data), chk.Equals, fileDefaultData)
}

func (s *FileURLSuite) TestFileDownloadDataCountExact(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShareWithDefaultData(c, shareURL)

	resp, err := fileURL.Download(ctx, 0, int64(len(fileDefaultData)), false)
	c.Assert(err, chk.IsNil)

	data, err := ioutil.ReadAll(resp.Response().Body)
	c.Assert(err, chk.IsNil)
	c.Assert(string(data), chk.Equals, fileDefaultData)
}

func (s *FileURLSuite) TestFileDownloadDataCountOutOfRange(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShareWithDefaultData(c, shareURL)

	resp, err := fileURL.Download(ctx, 0, int64(len(fileDefaultData))*2, false)
	c.Assert(err, chk.IsNil)

	data, err := ioutil.ReadAll(resp.Response().Body)
	c.Assert(err, chk.IsNil)
	c.Assert(string(data), chk.Equals, fileDefaultData)
}

// Don't check offset by design.
// func (s *FileURLSuite) TestFileUploadRangeNegativeInvalidOffset(c *chk.C) {
// 	fsu := getFSU()
// 	shareURL, _ := createNewShare(c, fsu)
// 	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
// 	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

// 	_, err := fileURL.UploadRange(ctx, -2, strings.NewReader(fileDefaultData), nil)
// 	c.Assert(err, chk.NotNil)
// 	c.Assert(strings.Contains(err.Error(), "offset must be >= 0"), chk.Equals, true)
// }

func (s *FileURLSuite) TestFileUploadRangeNilBody(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	_, err := fileURL.UploadRange(ctx, 0, nil, nil)
	c.Assert(err, chk.NotNil)
	c.Assert(strings.Contains(err.Error(), "body must not be nil"), chk.Equals, true)
}

func (s *FileURLSuite) TestFileUploadRangeEmptyBody(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	_, err := fileURL.UploadRange(ctx, 0, bytes.NewReader([]byte{}), nil)
	c.Assert(err, chk.NotNil)
	c.Assert(strings.Contains(err.Error(), "body must contain readable data whose size is > 0"), chk.Equals, true)
}

func (s *FileURLSuite) TestFileUploadRangeNonExistantFile(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := getFileURLFromShare(c, shareURL)

	_, err := fileURL.UploadRange(ctx, 0, getReaderToRandomBytes(12), nil)
	validateStorageError(c, err, azfile.ServiceCodeResourceNotFound)
}

func (s *FileURLSuite) TestFileUploadRangeTransactionalMD5(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 2048)
	defer delFile(c, fileURL)

	contentR, contentD := getRandomDataAndReader(2048)
	md5 := md5.Sum(contentD)

	// Upload range with correct transactional MD5
	pResp, err := fileURL.UploadRange(context.Background(), 0, contentR, md5[:])
	c.Assert(err, chk.IsNil)
	c.Assert(pResp.ContentMD5(), chk.NotNil)
	c.Assert(pResp.StatusCode(), chk.Equals, http.StatusCreated)
	c.Assert(pResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(pResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(pResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(pResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(pResp.Date().IsZero(), chk.Equals, false)
	c.Assert(pResp.ContentMD5(), chk.DeepEquals, md5[:])

	// Upload range with empty MD5, nil MD5 is covered by other cases.
	pResp, err = fileURL.UploadRange(context.Background(), 1024, bytes.NewReader(contentD[1024:]), nil)
	c.Assert(err, chk.IsNil)
	c.Assert(pResp.ContentMD5(), chk.NotNil)
	c.Assert(pResp.StatusCode(), chk.Equals, http.StatusCreated)

	resp, err := fileURL.Download(context.Background(), 0, azfile.CountToEnd, false)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusOK)
	c.Assert(resp.ContentLength(), chk.Equals, int64(2048))

	download, err := ioutil.ReadAll(resp.Response().Body)
	c.Assert(err, chk.IsNil)
	c.Assert(download, chk.DeepEquals, contentD[:])
}

func (s *FileURLSuite) TestFileUploadRangeIncorrectTransactionalMD5(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 2048)
	defer delFile(c, fileURL)

	contentR, _ := getRandomDataAndReader(2048)
	_, incorrectMD5 := getRandomDataAndReader(16)

	// Upload range with incorrect transactional MD5
	_, err := fileURL.UploadRange(context.Background(), 0, contentR, incorrectMD5[:])
	validateStorageError(c, err, azfile.ServiceCodeMd5Mismatch)
}

func (f *FileURLSuite) TestUploadRangeFromURL(c *chk.C) {
	fsu := getFSU()
	shareURL, shareName := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	// create the source file and populate it with random data at a specific offset
	expectedDataSize := 2048
	totalFileSize := 4096
	srcOffset := 999
	expectedDataReader, expectedData := getRandomDataAndReader(expectedDataSize)
	srcFileURL, _ := createNewFileFromShare(c, shareURL, int64(totalFileSize))
	_, err := srcFileURL.UploadRange(context.Background(), int64(srcOffset), expectedDataReader, nil)
	c.Assert(err, chk.IsNil)

	// generate a URL with SAS pointing to the source file
	credential, _ := getCredential()
	sasQueryParams, err := azfile.FileSASSignatureValues{
		Protocol:    azfile.SASProtocolHTTPS,
		ExpiryTime:  time.Now().UTC().Add(48 * time.Hour),
		ShareName:   shareName,
		Permissions: azfile.FileSASPermissions{Create: true, Read: true, Write: true, Delete: true}.String(),
	}.NewSASQueryParameters(credential)
	c.Assert(err, chk.IsNil)
	rawSrcURL := srcFileURL.URL()
	rawSrcURL.RawQuery = sasQueryParams.Encode()

	// create the destination file
	dstFileURL, _ := createNewFileFromShare(c, shareURL, int64(totalFileSize))

	// invoke UploadRange on dstFileURL and put the data at a random range
	// source and destination have different offsets so we can test both values at the same time
	dstOffset := 100
	uploadFromURLResp, err := dstFileURL.UploadRangeFromURL(ctx, rawSrcURL, int64(srcOffset),
		int64(dstOffset), int64(expectedDataSize))
	c.Assert(err, chk.IsNil)
	c.Assert(uploadFromURLResp.StatusCode(), chk.Equals, 201)

	// verify the destination
	resp, err := dstFileURL.Download(context.Background(), int64(dstOffset), int64(expectedDataSize), false)
	c.Assert(err, chk.IsNil)
	download, err := ioutil.ReadAll(resp.Response().Body)
	c.Assert(err, chk.IsNil)
	c.Assert(download, chk.DeepEquals, expectedData)
}

// Testings for GetRangeList and ClearRange
func (s *FileURLSuite) TestGetRangeListNonDefaultExact(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := getFileURLFromShare(c, shareURL)

	fileSize := int64(512 * 10)

	fileURL.Create(context.Background(), fileSize, azfile.FileHTTPHeaders{}, nil)

	defer delFile(c, fileURL)

	putResp, err := fileURL.UploadRange(context.Background(), 0, getReaderToRandomBytes(1024), nil)
	c.Assert(err, chk.IsNil)
	c.Assert(putResp.Response().StatusCode, chk.Equals, 201)
	c.Assert(putResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(putResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(putResp.ContentMD5(), chk.NotNil)
	c.Assert(putResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(putResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(putResp.Date().IsZero(), chk.Equals, false)

	rangeList, err := fileURL.GetRangeList(context.Background(), 0, 1023)
	c.Assert(err, chk.IsNil)
	c.Assert(rangeList.Response().StatusCode, chk.Equals, 200)
	c.Assert(rangeList.LastModified().IsZero(), chk.Equals, false)
	c.Assert(rangeList.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(rangeList.FileContentLength(), chk.Equals, fileSize)
	c.Assert(rangeList.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(rangeList.Version(), chk.Not(chk.Equals), "")
	c.Assert(rangeList.Date().IsZero(), chk.Equals, false)
	c.Assert(rangeList.Ranges, chk.HasLen, 1)
	c.Assert(rangeList.Ranges[0], chk.DeepEquals, azfile.FileRange{XMLName: xml.Name{Space: "", Local: "Range"}, Start: 0, End: 1023})
}

// Default means clear the entire file's range
func (s *FileURLSuite) TestClearRangeDefault(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 2048)
	defer delFile(c, fileURL)

	_, err := fileURL.UploadRange(context.Background(), 0, getReaderToRandomBytes(2048), nil)
	c.Assert(err, chk.IsNil)

	clearResp, err := fileURL.ClearRange(context.Background(), 0, 2048)
	c.Assert(err, chk.IsNil)
	c.Assert(clearResp.Response().StatusCode, chk.Equals, 201)

	rangeList, err := fileURL.GetRangeList(context.Background(), 0, azfile.CountToEnd)
	c.Assert(err, chk.IsNil)
	c.Assert(rangeList.Ranges, chk.HasLen, 0)
}

func (s *FileURLSuite) TestClearRangeNonDefault(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 4096)
	defer delFile(c, fileURL)

	_, err := fileURL.UploadRange(context.Background(), 2048, getReaderToRandomBytes(2048), nil)
	c.Assert(err, chk.IsNil)

	clearResp, err := fileURL.ClearRange(context.Background(), 2048, 2048)
	c.Assert(err, chk.IsNil)
	c.Assert(clearResp.Response().StatusCode, chk.Equals, 201)

	rangeList, err := fileURL.GetRangeList(context.Background(), 0, azfile.CountToEnd)
	c.Assert(err, chk.IsNil)
	c.Assert(rangeList.Ranges, chk.HasLen, 0)
}

func (s *FileURLSuite) TestClearRangeMultipleRanges(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 2048)
	defer delFile(c, fileURL)

	_, err := fileURL.UploadRange(context.Background(), 0, getReaderToRandomBytes(2048), nil)
	c.Assert(err, chk.IsNil)

	clearResp, err := fileURL.ClearRange(context.Background(), 1024, 1024)
	c.Assert(err, chk.IsNil)
	c.Assert(clearResp.Response().StatusCode, chk.Equals, 201)

	rangeList, err := fileURL.GetRangeList(context.Background(), 0, azfile.CountToEnd)
	c.Assert(err, chk.IsNil)
	c.Assert(rangeList.Ranges, chk.HasLen, 1)
	c.Assert(rangeList.Ranges[0], chk.DeepEquals, azfile.FileRange{XMLName: xml.Name{Space: "", Local: "Range"}, Start: 0, End: 1023})
}

// When not 512 aligned, clear range will set 0 the non-512 aligned range, and will not eliminate the range.
func (s *FileURLSuite) TestClearRangeNonDefault1Count(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 1)
	defer delFile(c, fileURL)

	d := []byte{1}
	_, err := fileURL.UploadRange(context.Background(), 0, bytes.NewReader(d), nil)
	c.Assert(err, chk.IsNil)

	clearResp, err := fileURL.ClearRange(context.Background(), 0, 1)
	c.Assert(err, chk.IsNil)
	c.Assert(clearResp.Response().StatusCode, chk.Equals, 201)

	rangeList, err := fileURL.GetRangeList(context.Background(), 0, azfile.CountToEnd)
	c.Assert(err, chk.IsNil)
	c.Assert(rangeList.Ranges, chk.HasLen, 1)
	c.Assert(rangeList.Ranges[0], chk.DeepEquals, azfile.FileRange{XMLName: xml.Name{Space: "", Local: "Range"}, Start: 0, End: 0})

	dResp, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
	c.Assert(err, chk.IsNil)
	bytes, err := ioutil.ReadAll(dResp.Body(azfile.RetryReaderOptions{}))
	c.Assert(err, chk.IsNil)
	c.Assert(bytes, chk.DeepEquals, []byte{0})
}

// Don't check offset by design.
// func (s *FileURLSuite) TestFileClearRangeNegativeInvalidOffset(c *chk.C) {
// 	fsu := getFSU()
// 	shareURL, _ := getShareURL(c, fsu)
// 	fileURL, _ := getFileURLFromShare(c, shareURL)

// 	_, err := fileURL.ClearRange(ctx, -1, 1)
// 	c.Assert(err, chk.NotNil)
// 	c.Assert(strings.Contains(err.Error(), "offset must be >= 0"), chk.Equals, true)
// }

func (s *FileURLSuite) TestFileClearRangeNegativeInvalidCount(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := getShareURL(c, fsu)
	fileURL, _ := getFileURLFromShare(c, shareURL)

	_, err := fileURL.ClearRange(ctx, 0, 0)
	c.Assert(err, chk.NotNil)
	c.Assert(strings.Contains(err.Error(), "count cannot be CountToEnd, and must be > 0"), chk.Equals, true)
}

func setupGetRangeListTest(c *chk.C) (shareURL azfile.ShareURL, fileURL azfile.FileURL) {
	fsu := getFSU()
	shareURL, _ = createNewShare(c, fsu)
	fileURL, _ = createNewFileFromShare(c, shareURL, int64(testFileRangeSize))

	_, err := fileURL.UploadRange(ctx, 0, getReaderToRandomBytes(testFileRangeSize), nil)
	c.Assert(err, chk.IsNil)

	return
}

func validateBasicGetRangeList(c *chk.C, resp *azfile.ShareFileRangeList, err error) {
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Ranges, chk.HasLen, 1)
	c.Assert(resp.Ranges[0], chk.Equals, azfile.FileRange{XMLName: xml.Name{Space: "", Local: "Range"}, Start: 0, End: testFileRangeSize - 1})
}

func (s *FileURLSuite) TestFileGetRangeListDefaultEmptyFile(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	resp, err := fileURL.GetRangeList(ctx, 0, azfile.CountToEnd)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Ranges, chk.HasLen, 0)
}

func (s *FileURLSuite) TestFileGetRangeListDefault1Range(c *chk.C) {
	shareURL, fileURL := setupGetRangeListTest(c)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	resp, err := fileURL.GetRangeList(ctx, 0, azfile.CountToEnd)
	validateBasicGetRangeList(c, resp, err)
}

func (s *FileURLSuite) TestFileGetRangeListNonContiguousRanges(c *chk.C) {
	shareURL, fileURL := setupGetRangeListTest(c)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	_, err := fileURL.Resize(ctx, int64(testFileRangeSize*3))
	c.Assert(err, chk.IsNil)

	_, err = fileURL.UploadRange(ctx, testFileRangeSize*2, getReaderToRandomBytes(testFileRangeSize), nil)
	c.Assert(err, chk.IsNil)
	resp, err := fileURL.GetRangeList(ctx, 0, azfile.CountToEnd)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Ranges, chk.HasLen, 2)
	c.Assert(resp.Ranges[0], chk.Equals, azfile.FileRange{XMLName: xml.Name{Space: "", Local: "Range"}, Start: 0, End: testFileRangeSize - 1})
	c.Assert(resp.Ranges[1], chk.Equals, azfile.FileRange{XMLName: xml.Name{Space: "", Local: "Range"}, Start: testFileRangeSize * 2, End: (testFileRangeSize * 3) - 1})
}

func (s *FileURLSuite) TestFileGetRangeListNonContiguousRangesCountLess(c *chk.C) {
	shareURL, fileURL := setupGetRangeListTest(c)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	resp, err := fileURL.GetRangeList(ctx, 0, testFileRangeSize-1)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Ranges, chk.HasLen, 1)
	c.Assert(resp.Ranges[0], chk.Equals, azfile.FileRange{XMLName: xml.Name{Space: "", Local: "Range"}, Start: 0, End: testFileRangeSize - 1})
}

func (s *FileURLSuite) TestFileGetRangeListNonContiguousRangesCountExceed(c *chk.C) {
	shareURL, fileURL := setupGetRangeListTest(c)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	resp, err := fileURL.GetRangeList(ctx, 0, testFileRangeSize+1)
	c.Assert(err, chk.IsNil)
	validateBasicGetRangeList(c, resp, err)
}

func (s *FileURLSuite) TestFileGetRangeListSnapshot(c *chk.C) {
	shareURL, fileURL := setupGetRangeListTest(c)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionInclude)

	resp, _ := shareURL.CreateSnapshot(ctx, azfile.Metadata{})
	snapshotURL := fileURL.WithSnapshot(resp.Snapshot())
	resp2, err := snapshotURL.GetRangeList(ctx, 0, azfile.CountToEnd)
	c.Assert(err, chk.IsNil)
	validateBasicGetRangeList(c, resp2, err)
}

func (s *FileURLSuite) TestUnexpectedEOFRecovery(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionInclude)

	fileURL, _ := createNewFileFromShare(c, share, 2048)

	contentR, contentD := getRandomDataAndReader(2048)

	resp, err := fileURL.UploadRange(ctx, 0, contentR, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusCreated)
	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")

	dlResp, err := fileURL.Download(ctx, 0, 2048, false)
	c.Assert(err, chk.IsNil)

	// Verify that we can inject errors first.
	reader := dlResp.Body(azfile.InjectErrorInRetryReaderOptions(errors.New("unrecoverable error")))

	_, err = ioutil.ReadAll(reader)
	c.Assert(err, chk.NotNil)
	c.Assert(err.Error(), chk.Equals, "unrecoverable error")

	// Then inject the retryable error.
	reader = dlResp.Body(azfile.InjectErrorInRetryReaderOptions(io.ErrUnexpectedEOF))

	buf, err := ioutil.ReadAll(reader)
	c.Assert(err, chk.IsNil)
	c.Assert(buf, chk.DeepEquals, contentD)
}

func (s *FileURLSuite) TestCreateMaximumSizeFileShare(c *chk.C) {
	fsu := getFSU()
	share, _ := getShareURL(c, fsu)
	cResp, err := share.Create(ctx, nil, fileShareMaxQuota)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionInclude)
	dir := share.NewRootDirectoryURL()

	file, _ := getFileURLFromDirectory(c, dir)

	_, err = file.Create(ctx, fileMaxAllowedSizeInBytes, azfile.FileHTTPHeaders{}, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
}

func (s *FileURLSuite) TestRename(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	renamedFileName := generateFileName()
	renamedFileURL, err := fileURL.Rename(ctx, renamedFileName, nil, azfile.Metadata{}, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(renamedFileURL, chk.NotNil)

	_, err = fileURL.GetProperties(ctx)
	c.Assert(err, chk.NotNil)

	_, err = renamedFileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
}

func (s *FileURLSuite) TestRenameDifferentDirectory(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	directoryURL, _ := createNewDirectoryFromShare(c, shareURL)
	renamedFileName := generateFileName()
	destinationURL := directoryURL.NewFileURL(renamedFileName)
	destinationPath := azfile.NewFileURLParts(destinationURL.URL()).DirectoryOrFilePath

	renamedFileURL, err := fileURL.Rename(ctx, destinationPath, nil, azfile.Metadata{}, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(renamedFileURL, chk.NotNil)

	_, err = fileURL.GetProperties(ctx)
	c.Assert(err, chk.NotNil)

	_, err = renamedFileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(renamedFileURL.String(), chk.Equals, destinationURL.String())
}

func (s *FileURLSuite) TestRenameReplaceIfExistsTrue(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	replaceIfExists := true

	renamedFileName := generateFileName()
	shareURL.NewRootDirectoryURL().NewFileURL(renamedFileName).Create(ctx, 0, azfile.FileHTTPHeaders{}, nil)
	renamedFileURL, err := fileURL.Rename(ctx, renamedFileName, &replaceIfExists, azfile.Metadata{}, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(renamedFileURL, chk.NotNil)
}

func (s *FileURLSuite) TestRenameReplaceIfExistsFalse(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	replaceIfExists := false

	renamedFileName := generateFileName()
	shareURL.NewRootDirectoryURL().NewFileURL(renamedFileName).Create(ctx, 0, azfile.FileHTTPHeaders{}, nil)
	_, err := fileURL.Rename(ctx, renamedFileName, &replaceIfExists, azfile.Metadata{}, nil)
	c.Assert(err, chk.NotNil)
}

func (s *FileURLSuite) TestRenameMetadata(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	metadata := azfile.Metadata{}
	metadata["foo"] = "bar"
	renamedFileName := generateFileName()
	renamedFileURL, err := fileURL.Rename(ctx, renamedFileName, nil, metadata, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(renamedFileURL, chk.NotNil)

	props, err := renamedFileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(props.NewMetadata()["foo"], chk.Equals, "bar")
}

func (s *FileURLSuite) TestRenameContentType(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, shareURL, 0)

	contentType := "application/pdf"
	renamedFileName := generateFileName()
	renamedFileURL, err := fileURL.Rename(ctx, renamedFileName, nil, azfile.Metadata{}, &contentType)
	c.Assert(err, chk.IsNil)
	c.Assert(renamedFileURL, chk.NotNil)

	props, err := renamedFileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(props.ContentType(), chk.Equals, contentType)
}

func (s *FileURLSuite) TestRenameError(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	fileURL, _ := getFileURLFromDirectory(c, shareURL.NewRootDirectoryURL())

	renamedFileName := generateFileName()
	_, err := fileURL.Rename(ctx, renamedFileName, nil, azfile.Metadata{}, nil)
	c.Assert(err, chk.NotNil)
}
