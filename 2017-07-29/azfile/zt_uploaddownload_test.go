package azfile

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	chk "gopkg.in/check.v1"
)

type uploadDownloadSuite struct{}

var _ = chk.Suite(&uploadDownloadSuite{})

var ctx = context.Background() // Default never-expiring context

const (
	sharePrefix     = "go"
	directoryPrefix = "gotestdirectory"
	filePrefix      = "gotestfile"
)

// TODO: refactor white box testing utils.
func createNewLocalFile(c *chk.C, fileSizeInByte int64) (*os.File, []byte) {
	if fileSizeInByte < 0 {
		panic("fileSizeInByte must >=0")
	}

	name := generateFileName()

	f, err := os.Create(name)
	c.Assert(err, chk.IsNil)

	bigBuff := make([]byte, fileSizeInByte)
	if fileSizeInByte > 0 {
		_, err = rand.Read(bigBuff)
		c.Assert(err, chk.IsNil)

		_, err = f.Write(bigBuff)
		c.Assert(err, chk.IsNil)
	}

	return f, bigBuff
}

func delFile(c *chk.C, file FileURL) {
	resp, err := file.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Response().StatusCode, chk.Equals, 202)
}

func delShare(c *chk.C, share ShareURL, option DeleteSnapshotsOptionType) {
	resp, err := share.Delete(context.Background(), option)
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

func createNewShare(c *chk.C, fsu ServiceURL) (share ShareURL, name string) {
	share, name = getShareURL(c, fsu)

	cResp, err := share.Create(ctx, nil, 0)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	return share, name
}

// This function generates an entity name by concatenating the passed prefix,
// the name of the test requesting the entity name, and the minute, second, and nanoseconds of the call.
// This should make it easy to associate the entities with their test, uniquely identify
// them, and determine the order in which they were created.
// Note that this imposes a restriction on the length of test names
func generateName(prefix string) string {
	// These next lines up through the for loop are obtaining and walking up the stack
	// trace to extrat the test name, which is stored in name
	pc := make([]uintptr, 10)
	runtime.Callers(0, pc)
	f := runtime.FuncForPC(pc[0])
	name := f.Name()
	for i := 0; !strings.Contains(name, "Suite"); i++ { // The tests are all scoped to the suite, so this ensures getting the actual test name
		f = runtime.FuncForPC(pc[i])
		name = f.Name()
	}
	funcNameStart := strings.Index(name, "Test")
	name = name[funcNameStart+len("Test"):] // Just get the name of the test and not any of the garbage at the beginning
	name = strings.ToLower(name)            // Ensure it is a valid resource name
	currentTime := time.Now()
	name = fmt.Sprintf("%s%s%d%d%d", prefix, strings.ToLower(name), currentTime.Minute(), currentTime.Second(), currentTime.Nanosecond())
	return name
}

func generateFileName() string {
	return generateName(filePrefix)
}

func generateShareName() string {
	return generateName(sharePrefix)
}

func getFileURLFromDirectory(c *chk.C, directory DirectoryURL) (file FileURL, name string) {
	name = generateFileName()
	file = directory.NewFileURL(name)

	return file, name
}

// This is a convenience method, No public API to create file URL from share now. This method uses share's root directory.
func getFileURLFromShare(c *chk.C, share ShareURL) (file FileURL, name string) {
	name = generateFileName()
	file = share.NewRootDirectoryURL().NewFileURL(name)

	return file, name
}

func getAccountAndKey() (string, string) {
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	if name == "" || key == "" {
		panic("ACCOUNT_NAME and ACCOUNT_KEY environment vars must be set before running tests")
	}

	return name, key
}

func getFSU() ServiceURL {
	name, key := getAccountAndKey()
	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/", name))

	credential := NewSharedKeyCredential(name, key)
	pipeline := NewPipeline(credential, PipelineOptions{})
	return NewServiceURL(*u, pipeline)
}

func getShareURL(c *chk.C, fsu ServiceURL) (share ShareURL, name string) {
	name = generateShareName()
	share = fsu.NewShareURL(name)

	return share, name
}

func createNewFileFromShare(c *chk.C, share ShareURL, fileSize int64) (file FileURL, name string) {
	dir := share.NewRootDirectoryURL()

	file, name = getFileURLFromDirectory(c, dir)

	cResp, err := file.Create(ctx, fileSize, FileHTTPHeaders{}, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)

	return file, name
}

// Testings for FileURL's Download methods.
func (ud *uploadDownloadSuite) TestDownloadBasic(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, DeleteSnapshotsOptionNone)

	fileSize := 2048 //2048 bytes

	file, _ := createNewFileFromShare(c, share, int64(fileSize))
	defer delFile(c, file)

	contentR, contentD := getRandomDataAndReader(fileSize)

	pResp, err := file.UploadRange(context.Background(), 0, contentR)
	c.Assert(err, chk.IsNil)
	c.Assert(pResp.ContentMD5(), chk.Not(chk.Equals), [md5.Size]byte{})
	c.Assert(pResp.StatusCode(), chk.Equals, http.StatusCreated)
	c.Assert(pResp.IsServerEncrypted(), chk.NotNil)
	c.Assert(pResp.ETag(), chk.Not(chk.Equals), ETagNone)
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

	// Without Retry
	download, err := ioutil.ReadAll(resp.Body(RetryReaderOptions{}))
	c.Assert(err, chk.IsNil)
	c.Assert(download, chk.DeepEquals, contentD[:1024])

	// Set ContentMD5 for the entire file.
	_, err = file.SetHTTPHeaders(context.Background(), FileHTTPHeaders{ContentMD5: pResp.ContentMD5()})
	c.Assert(err, chk.IsNil)

	// Test get with another type of range index, and validate if FileContentMD5 can be get correclty.
	resp, err = file.Download(context.Background(), 1024, CountToEnd, false)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusPartialContent)
	c.Assert(resp.ContentLength(), chk.Equals, int64(1024))
	c.Assert(resp.ContentMD5(), chk.Equals, [md5.Size]byte{})
	c.Assert(resp.FileContentMD5(), chk.DeepEquals, pResp.ContentMD5())

	download, err = ioutil.ReadAll(resp.Body(RetryReaderOptions{MaxRetryRequests: 1}))
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
	c.Assert(resp.CopyStatus(), chk.Equals, CopyStatusNone)
	c.Assert(resp.CopyStatusDescription(), chk.Equals, "")
	c.Assert(resp.Date().IsZero(), chk.Equals, false)
	c.Assert(resp.ETag(), chk.Not(chk.Equals), ETagNone)
	c.Assert(resp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(resp.NewMetadata(), chk.DeepEquals, Metadata{})
	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(resp.Version(), chk.Not(chk.Equals), "")
	c.Assert(resp.IsServerEncrypted(), chk.NotNil)

	// Get entire file, check status code 200.
	resp, err = file.Download(context.Background(), 0, CountToEnd, false)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusOK)
	c.Assert(resp.ContentLength(), chk.Equals, int64(2048))
	c.Assert(resp.ContentMD5(), chk.Equals, pResp.ContentMD5())   // Note: This case is inted to get entire file, entire file's MD5 will be returned.
	c.Assert(resp.FileContentMD5(), chk.Equals, [md5.Size]byte{}) // Note: FileContentMD5 is returned, only when range is specified explicitly.

	download, err = ioutil.ReadAll(resp.Body(RetryReaderOptions{MaxRetryRequests: 2}))
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
	c.Assert(resp.CopyStatus(), chk.Equals, CopyStatusNone)
	c.Assert(resp.CopyStatusDescription(), chk.Equals, "")
	c.Assert(resp.Date().IsZero(), chk.Equals, false)
	c.Assert(resp.ETag(), chk.Not(chk.Equals), ETagNone)
	c.Assert(resp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(resp.NewMetadata(), chk.DeepEquals, Metadata{})
	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(resp.Version(), chk.Not(chk.Equals), "")
	c.Assert(resp.IsServerEncrypted(), chk.NotNil)
}

func (ud *uploadDownloadSuite) TestDownloadRetry(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, DeleteSnapshotsOptionNone)

	fileSize := 100 * 1024 //100 KB

	file, _ := createNewFileFromShare(c, share, int64(fileSize))
	defer delFile(c, file)

	contentR, contentD := getRandomDataAndReader(fileSize)

	pResp, err := file.UploadRange(context.Background(), 0, contentR)
	c.Assert(err, chk.IsNil)
	c.Assert(pResp.ContentMD5(), chk.Not(chk.Equals), [md5.Size]byte{})
	c.Assert(pResp.StatusCode(), chk.Equals, http.StatusCreated)
	c.Assert(pResp.IsServerEncrypted(), chk.NotNil)
	c.Assert(pResp.ETag(), chk.Not(chk.Equals), ETagNone)
	c.Assert(pResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(pResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(pResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(pResp.Date().IsZero(), chk.Equals, false)

	_, err = file.SetHTTPHeaders(context.Background(), FileHTTPHeaders{ContentMD5: pResp.ContentMD5()})
	c.Assert(err, chk.IsNil)

	// Download entire file with retry, check status code 200.
	resp, err := file.Download(context.Background(), 0, CountToEnd, false)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusOK)
	c.Assert(resp.ContentLength(), chk.Equals, int64(102400))
	c.Assert(resp.ContentMD5(), chk.Equals, pResp.ContentMD5())   // Note: This case is intend to get entire file, entire file's MD5 will be returned.
	c.Assert(resp.FileContentMD5(), chk.Equals, [md5.Size]byte{}) // Note: FileContentMD5 is returned, only when range is specified explicitly.

	download, err := ioutil.ReadAll(resp.Body(RetryReaderOptions{MaxRetryRequests: 2, doInjectError: true, doInjectErrorRound: 0}))
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
	c.Assert(resp.CopyStatus(), chk.Equals, CopyStatusNone)
	c.Assert(resp.CopyStatusDescription(), chk.Equals, "")
	c.Assert(resp.Date().IsZero(), chk.Equals, false)
	c.Assert(resp.ETag(), chk.Not(chk.Equals), ETagNone)
	c.Assert(resp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(resp.NewMetadata(), chk.DeepEquals, Metadata{})
	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(resp.Version(), chk.Not(chk.Equals), "")
	c.Assert(resp.IsServerEncrypted(), chk.NotNil)
}

func (ud *uploadDownloadSuite) TestDownloadDefaultParam(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, DeleteSnapshotsOptionNone)

	fileSize := int64(100 * 1024) //100 KB

	file, _ := createNewFileFromShare(c, share, fileSize)
	defer delFile(c, file)

	resp, err := file.Download(context.Background(), 0, 0, false)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.ContentLength(), chk.Equals, fileSize)

	retryReader := resp.Body(RetryReaderOptions{})
	bytes, err := ioutil.ReadAll(retryReader)
	zeroBytes := make([]byte, fileSize, fileSize)
	c.Assert(err, chk.IsNil)
	c.Assert(int64(len(bytes)), chk.Equals, fileSize)
	c.Assert(zeroBytes, chk.DeepEquals, bytes)
}

func (ud *uploadDownloadSuite) TestDownloadNegativePanic(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, DeleteSnapshotsOptionNone)

	fileSize := 100 * 1024 //100 KB

	file, _ := createNewFileFromShare(c, share, int64(fileSize))
	defer delFile(c, file)

	// Check illegal count
	c.Assert(func() { file.Download(context.Background(), 0, -2, false) }, chk.Panics, "The range count must be either equal to CountToEnd (0) or > 0")

	// Check illegal offset
	c.Assert(func() { file.Download(context.Background(), -1, 3, false) }, chk.Panics, "The range offset must be >= 0")

	// Check illegal rangeGetContentMD5
	c.Assert(func() { file.Download(context.Background(), 0, CountToEnd, true) }, chk.Panics, "rangeGetContentMD5 only work with partial data downloading")
}

func (ud *uploadDownloadSuite) TestDownloadNegativeError(c *chk.C) {
	fsu := getFSU()
	shareU, _ := getShareURL(c, fsu)
	fileU, _ := getFileURLFromDirectory(c, shareU.NewRootDirectoryURL())

	// Download a non-exist file should report 404 status code.
	_, err := fileU.Download(ctx, 0, CountToEnd, false)
	c.Assert(err, chk.NotNil)

	stgErr := err.(StorageError)
	c.Assert(stgErr, chk.NotNil)

	// Check not found
	c.Assert(stgErr.Response().StatusCode, chk.Equals, http.StatusNotFound)
}

// End testings for FileURL Download

// Following are testings for highlevel APIs.
func (ud *uploadDownloadSuite) TestUploadDownloadBufferParallelNonDefault(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, DeleteSnapshotsOptionNone)

	fileSize := 2048 //2048 bytes
	fileSize2 := 2048 * 3

	file, _ := createNewFileFromShare(c, share, int64(fileSize))
	defer delFile(c, file)

	ctx = context.Background()
	_, srcBytes := getRandomDataAndReader(fileSize)
	_, srcBytes2 := getRandomDataAndReader(fileSize2)

	md5Str := "MDAwMDAwMDA="
	var testMd5 [md5.Size]byte
	copy(testMd5[:], md5Str)

	md5Str2 := "MDAwMDAwMDAAAA="
	var testMd52 [md5.Size]byte
	copy(testMd52[:], md5Str2)

	headers := FileHTTPHeaders{
		ContentType:        "application/octet-stream",
		ContentEncoding:    "ContentEncoding",
		ContentLanguage:    "tr,en",
		ContentMD5:         testMd5,
		CacheControl:       "no-transform",
		ContentDisposition: "attachment",
	}

	metadata := Metadata{
		"foo": "foovalue",
		"bar": "barvalue",
	}

	headers2 := FileHTTPHeaders{
		ContentType:        "test",
		ContentEncoding:    "test",
		ContentLanguage:    "test",
		ContentMD5:         testMd52,
		CacheControl:       "test",
		ContentDisposition: "test",
	}

	metadata2 := Metadata{
		"overwrite": "overwrite",
	}

	err := UploadBufferToAzureFile(ctx, srcBytes, file, UploadToAzureFileOptions{FileHTTPHeaders: headers, Metadata: metadata})
	c.Assert(err, chk.IsNil)

	destBytes := make([]byte, fileSize)
	resp, err := DownloadAzureFileToBuffer(ctx, file, destBytes, DownloadFromAzureFileOptions{})
	c.Assert(err, chk.IsNil)
	c.Assert(resp.ContentType(), chk.Equals, "application/octet-stream")
	c.Assert(resp.ContentLength(), chk.Equals, int64(fileSize))
	c.Assert(resp.ContentEncoding(), chk.Equals, "ContentEncoding")
	c.Assert(resp.ContentLanguage(), chk.Equals, "tr,en")
	c.Assert(resp.ContentMD5(), chk.Equals, testMd5)
	c.Assert(resp.CacheControl(), chk.Equals, "no-transform")
	c.Assert(resp.ContentDisposition(), chk.Equals, "attachment")
	c.Assert(resp.NewMetadata(), chk.DeepEquals, metadata)

	c.Assert(destBytes, chk.DeepEquals, srcBytes)

	// Test overwrite scenario
	err = UploadBufferToAzureFile(ctx, srcBytes2, file, UploadToAzureFileOptions{FileHTTPHeaders: headers2, Metadata: metadata2})
	c.Assert(err, chk.IsNil)

	destBytes2 := make([]byte, fileSize2)
	resp2, err := DownloadAzureFileToBuffer(ctx, file, destBytes2, DownloadFromAzureFileOptions{})
	c.Assert(err, chk.IsNil)
	c.Assert(resp2.ContentType(), chk.Equals, "test")
	c.Assert(resp2.ContentLength(), chk.Equals, int64(fileSize2))
	c.Assert(resp2.ContentEncoding(), chk.Equals, "test")
	c.Assert(resp2.ContentLanguage(), chk.Equals, "test")
	c.Assert(resp2.ContentMD5(), chk.Equals, testMd52)
	c.Assert(resp2.CacheControl(), chk.Equals, "test")
	c.Assert(resp2.ContentDisposition(), chk.Equals, "test")
	c.Assert(resp2.NewMetadata(), chk.DeepEquals, metadata2)

	c.Assert(destBytes2, chk.DeepEquals, srcBytes2)
}

// Customzied range size, parallel count and progress update.
func (ud *uploadDownloadSuite) TestUploadDownloadBufferParallelCheckProgress(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, DeleteSnapshotsOptionNone)

	fileSize := 4 * 1024 * 1024 //4MB
	blockSize := 512 * 1024     // 512KB

	file, _ := createNewFileFromShare(c, share, int64(fileSize))
	defer delFile(c, file)

	ctx = context.Background()
	_, srcBytes := getRandomDataAndReader(fileSize)

	uLogBuffer := bytes.Buffer{}
	dLogBuffer := bytes.Buffer{}

	err := UploadBufferToAzureFile(
		ctx, srcBytes, file,
		UploadToAzureFileOptions{
			RangeSize:   int64(blockSize),
			Parallelism: 3,
			Progress: func(b int64) {
				fmt.Fprintf(&uLogBuffer, "Write: %d", b)
			},
		})
	c.Assert(err, chk.IsNil)
	c.Assert(strings.Contains(uLogBuffer.String(), fmt.Sprintf("Write: %d", fileSize)), chk.Equals, true)

	destBytes := make([]byte, fileSize)
	_, err = DownloadAzureFileToBuffer(
		ctx, file, destBytes,
		DownloadFromAzureFileOptions{
			RangeSize:   int64(blockSize),
			Parallelism: 3,
			Progress: func(b int64) {
				fmt.Fprintf(&dLogBuffer, "Write: %d", b)
			},
		})
	c.Assert(err, chk.IsNil)
	c.Assert(strings.Contains(dLogBuffer.String(), fmt.Sprintf("Write: %d", fileSize)), chk.Equals, true)

	c.Assert(destBytes, chk.DeepEquals, srcBytes)
}

func validateFileExists(c *chk.C, fileURL FileURL) {
	_, err := fileURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
}

func (ud *uploadDownloadSuite) TestUploadDownloadFileParallelDefaultEmpty(c *chk.C) {
	testUploadDownloadFileParallelDefault(c, 0)
}

func (ud *uploadDownloadSuite) TestUploadDownloadFileParallelDefault1Byte(c *chk.C) {
	testUploadDownloadFileParallelDefault(c, 1)
}

func (ud *uploadDownloadSuite) TestUploadDownloadFileParallelDefaultBlockSizeLess(c *chk.C) {
	testUploadDownloadFileParallelDefault(c, FileMaxUploadRangeBytes-1)
}

func (ud *uploadDownloadSuite) TestUploadDownloadFileParallelDefaultBlockSize(c *chk.C) {
	testUploadDownloadFileParallelDefault(c, FileMaxUploadRangeBytes)
}

func (ud *uploadDownloadSuite) TestUploadDownloadFileParallelDefaultBlockSizeMore(c *chk.C) {
	testUploadDownloadFileParallelDefault(c, FileMaxUploadRangeBytes+1)
}

func (ud *uploadDownloadSuite) TestUploadDownloadFileParallelDefaultBlockSizeMulti(c *chk.C) {
	testUploadDownloadFileParallelDefault(c, FileMaxUploadRangeBytes*12+1)
}

func testUploadDownloadFileParallelDefault(c *chk.C, fileSize int64) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, DeleteSnapshotsOptionNone)

	fileURL, _ := getFileURLFromShare(c, share)

	file, bytes := createNewLocalFile(c, fileSize)
	defer func() {
		file.Close()
		os.Remove(file.Name())
	}()

	err := UploadFileToAzureFile(ctx, file, fileURL, UploadToAzureFileOptions{RangeSize: 500 * 1024})
	c.Assert(err, chk.IsNil)

	validateFileExists(c, fileURL)

	file2Name := generateFileName()
	file2, err := os.Create(file2Name)
	c.Assert(err, chk.IsNil)
	resp, err := DownloadAzureFileToFile(ctx, fileURL, file2, DownloadFromAzureFileOptions{})
	c.Assert(err, chk.IsNil)
	c.Assert(resp.ETag(), chk.Not(chk.Equals), ETagNone)

	defer func() {
		file2.Close()
		os.Remove(file2Name)
	}()

	// Check local file still exists
	_, err = file2.Stat()
	c.Assert(err, chk.IsNil)
	c.Assert(os.IsNotExist(err), chk.Equals, false) // Actually equivalent

	// Check bytes same
	destBytes, err := ioutil.ReadFile(file2Name)
	c.Assert(err, chk.IsNil)

	c.Assert(bytes, chk.DeepEquals, destBytes)
}

func (ud *uploadDownloadSuite) TestUploadFileToAzureFileNegativeInvalidRangeSize(c *chk.C) {
	_, srcBytes := getRandomDataAndReader(1)

	fsu := getFSU()
	shareURL, _ := getShareURL(c, fsu)
	fileURL, _ := getFileURLFromShare(c, shareURL)

	c.Assert(
		func() { UploadBufferToAzureFile(ctx, srcBytes, fileURL, UploadToAzureFileOptions{RangeSize: -1}) },
		chk.Panics,
		"o.RangeSize must be >= 0 and <= 4194304, in bytes")
}

func (ud *uploadDownloadSuite) TestUploadFileToAzureFileNegativeInvalidRangeSize2(c *chk.C) {
	_, srcBytes := getRandomDataAndReader(1)

	fsu := getFSU()
	shareURL, _ := getShareURL(c, fsu)
	fileURL, _ := getFileURLFromShare(c, shareURL)

	c.Assert(
		func() {
			UploadBufferToAzureFile(ctx, srcBytes, fileURL, UploadToAzureFileOptions{RangeSize: FileMaxUploadRangeBytes + 1})
		},
		chk.Panics,
		"o.RangeSize must be >= 0 and <= 4194304, in bytes")
}

func (ud *uploadDownloadSuite) TestUploadFileToAzureFileNegativeInvalidLocalFile(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, DeleteSnapshotsOptionNone)

	fileURL, _ := getFileURLFromShare(c, share)

	file, _ := createNewLocalFile(c, 0)

	file.Close()
	os.Remove(file.Name())

	err := UploadFileToAzureFile(ctx, file, fileURL, UploadToAzureFileOptions{})
	c.Assert(err, chk.NotNil)
}

func (ud *uploadDownloadSuite) TestDownloadAzureFileToFileNegativeInvalidLocalFile(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, DeleteSnapshotsOptionNone)

	fileURL, _ := createNewFileFromShare(c, share, 1)

	c.Assert(func() { DownloadAzureFileToFile(ctx, fileURL, nil, DownloadFromAzureFileOptions{}) }, chk.Panics, "file should not be nil")
}

// Download Azure file to a larger existing file, which need overwrite and truncate
func (ud *uploadDownloadSuite) TestDownloadFileParallelOverwriteLocalFile(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, DeleteSnapshotsOptionNone)

	fileSize := 2048 //2048 bytes
	fileSize2 := 2048 * 3

	fileURL, _ := createNewFileFromShare(c, share, int64(fileSize))
	defer delFile(c, fileURL)

	localFile, _ := createNewLocalFile(c, int64(fileSize2))
	defer func() {
		localFile.Close()
		os.Remove(localFile.Name())
	}()

	ctx = context.Background()
	_, srcBytes := getRandomDataAndReader(fileSize)

	md5Str := "MDAwMDAwMDA="
	var testMd5 [md5.Size]byte
	copy(testMd5[:], md5Str)

	headers := FileHTTPHeaders{
		ContentType:        "application/octet-stream",
		ContentEncoding:    "ContentEncoding",
		ContentLanguage:    "tr,en",
		ContentMD5:         testMd5,
		CacheControl:       "no-transform",
		ContentDisposition: "attachment",
	}

	metadata := Metadata{
		"foo": "foovalue",
		"bar": "barvalue",
	}

	err := UploadBufferToAzureFile(ctx, srcBytes, fileURL, UploadToAzureFileOptions{FileHTTPHeaders: headers, Metadata: metadata})
	c.Assert(err, chk.IsNil)

	resp, err := DownloadAzureFileToFile(ctx, fileURL, localFile, DownloadFromAzureFileOptions{})
	c.Assert(err, chk.IsNil)
	c.Assert(resp.ContentType(), chk.Equals, "application/octet-stream")
	c.Assert(resp.ContentLength(), chk.Equals, int64(fileSize))
	c.Assert(resp.ContentEncoding(), chk.Equals, "ContentEncoding")
	c.Assert(resp.ContentLanguage(), chk.Equals, "tr,en")
	c.Assert(resp.ContentMD5(), chk.Equals, testMd5)
	c.Assert(resp.CacheControl(), chk.Equals, "no-transform")
	c.Assert(resp.ContentDisposition(), chk.Equals, "attachment")
	c.Assert(resp.NewMetadata(), chk.DeepEquals, metadata)

	destBytes, err := ioutil.ReadFile(localFile.Name())

	c.Assert(destBytes, chk.DeepEquals, srcBytes)
}
