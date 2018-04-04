package azfile_test

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-file-go/2017-04-17/azfile"
	chk "gopkg.in/check.v1"
)

func Test(t *testing.T) { chk.TestingT(t) }

type aztestsSuite struct{}

var _ = chk.Suite(&aztestsSuite{})

const (
	sharePrefix              = "go"
	directoryPrefix          = "gotestdirectory"
	filePrefix               = "gotestfile"
	validationErrorSubstring = "validation failed"
	fileDefaultData          = "file default data"
)

var ctx = context.Background()
var basicHeaders = azfile.FileHTTPHeaders{ContentType: "my_type", ContentDisposition: "my_disposition",
	CacheControl: "control", ContentMD5: md5.Sum([]byte("")), ContentLanguage: "my_language", ContentEncoding: "my_encoding"}
var basicMetadata = azfile.Metadata{"foo": "bar"}

func getAccountAndKey() (string, string) {
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	if name == "" || key == "" {
		panic("ACCOUNT_NAME and ACCOUNT_KEY environment vars must be set before running tests")
	}

	return name, key
}

func getFSU() azfile.ServiceURL {
	name, key := getAccountAndKey()
	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/", name))

	credential := azfile.NewSharedKeyCredential(name, key)
	pipeline := azfile.NewPipeline(credential, azfile.PipelineOptions{})
	return azfile.NewServiceURL(*u, pipeline)
}

func getAlternateFSU() (azfile.ServiceURL, error) {
	secondaryAccountName, secondaryAccountKey := os.Getenv("SECONDARY_ACCOUNT_NAME"), os.Getenv("SECONDARY_ACCOUNT_KEY")
	if secondaryAccountName == "" || secondaryAccountKey == "" {
		return azfile.ServiceURL{}, errors.New("SECONDARY_ACCOUNT_NAME and/or SECONDARY_ACCOUNT_KEY environment variables not specified.")
	}
	fsURL, _ := url.Parse("https://" + secondaryAccountName + ".file.core.windows.net/")

	credentials := azfile.NewSharedKeyCredential(secondaryAccountName, secondaryAccountKey)
	pipeline := azfile.NewPipeline(credentials, azfile.PipelineOptions{ /*Log: pipeline.NewLogWrapper(pipeline.LogInfo, log.New(os.Stderr, "", log.LstdFlags))*/ })

	return azfile.NewServiceURL(*fsURL, pipeline), nil
}

func getCredential() (*azfile.SharedKeyCredential, string) {
	name, key := getAccountAndKey()

	return azfile.NewSharedKeyCredential(name, key), name
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

func generateShareName() string {
	return generateName(sharePrefix)
}

func generateDirectoryName() string {
	return generateName(directoryPrefix)
}

func generateFileName() string {
	return generateName(filePrefix)
}

func getShareURL(c *chk.C, fsu azfile.ServiceURL) (share azfile.ShareURL, name string) {
	name = generateShareName()
	share = fsu.NewShareURL(name)

	return share, name
}

func getDirectoryURLFromShare(c *chk.C, share azfile.ShareURL) (directory azfile.DirectoryURL, name string) {
	name = generateDirectoryName()
	directory = share.NewDirectoryURL(name)

	return directory, name
}

func getDirectoryURLFromDirectory(c *chk.C, parentDirectory azfile.DirectoryURL) (directory azfile.DirectoryURL, name string) {
	name = generateDirectoryName()
	directory = parentDirectory.NewDirectoryURL(name)

	return directory, name
}

// This is a convenience method, No public API to create file URL from share now. This method uses share's root directory.
func getFileURLFromShare(c *chk.C, share azfile.ShareURL) (file azfile.FileURL, name string) {
	name = generateFileName()
	file = share.NewRootDirectoryURL().NewFileURL(name)

	return file, name
}

func getFileURLFromDirectory(c *chk.C, directory azfile.DirectoryURL) (file azfile.FileURL, name string) {
	name = generateFileName()
	file = directory.NewFileURL(name)

	return file, name
}

func createNewShare(c *chk.C, fsu azfile.ServiceURL) (share azfile.ShareURL, name string) {
	share, name = getShareURL(c, fsu)

	cResp, err := share.Create(ctx, nil, 0)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	return share, name
}

func createNewShareWithPrefix(c *chk.C, fsu azfile.ServiceURL, prefix string) (share azfile.ShareURL, name string) {
	name = generateName(prefix)
	share = fsu.NewShareURL(name)

	cResp, err := share.Create(ctx, nil, 0)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	return share, name
}

func createNewDirectoryWithPrefix(c *chk.C, parentDirectory azfile.DirectoryURL, prefix string) (dir azfile.DirectoryURL, name string) {
	name = generateName(prefix)
	dir = parentDirectory.NewDirectoryURL(name)

	cResp, err := dir.Create(ctx, azfile.Metadata{})
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	return dir, name
}

func createNewFileWithPrefix(c *chk.C, dir azfile.DirectoryURL, prefix string, size int64) (file azfile.FileURL, name string) {
	name = generateName(prefix)
	file = dir.NewFileURL(name)

	cResp, err := file.Create(ctx, size, azfile.FileHTTPHeaders{}, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	return file, name
}

func createNewDirectoryFromShare(c *chk.C, share azfile.ShareURL) (dir azfile.DirectoryURL, name string) {
	dir, name = getDirectoryURLFromShare(c, share)

	cResp, err := dir.Create(ctx, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	return dir, name
}

func createNewDirectoryFromDirectory(c *chk.C, parentDirectory azfile.DirectoryURL) (dir azfile.DirectoryURL, name string) {
	dir, name = getDirectoryURLFromDirectory(c, parentDirectory)

	cResp, err := dir.Create(ctx, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	return dir, name
}

// This is a convenience method, No public API to create file URL from share now. This method uses share's root directory.
func createNewFileFromShare(c *chk.C, share azfile.ShareURL, fileSize int64) (file azfile.FileURL, name string) {
	dir := share.NewRootDirectoryURL()

	file, name = getFileURLFromDirectory(c, dir)

	cResp, err := file.Create(ctx, fileSize, azfile.FileHTTPHeaders{}, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)

	return file, name
}

// This is a convenience method, No public API to create file URL from share now. This method uses share's root directory.
func createNewFileFromShareWithDefaultData(c *chk.C, share azfile.ShareURL) (file azfile.FileURL, name string) {
	dir := share.NewRootDirectoryURL()

	file, name = getFileURLFromDirectory(c, dir)

	cResp, err := file.Create(ctx, int64(len(fileDefaultData)), azfile.FileHTTPHeaders{}, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)

	_, err = file.UploadRange(ctx, 0, strings.NewReader(fileDefaultData))
	c.Assert(err, chk.IsNil)

	return file, name
}

func createNewFileFromDirectory(c *chk.C, directory azfile.DirectoryURL, fileSize int64) (file azfile.FileURL, name string) {
	file, name = getFileURLFromDirectory(c, directory)

	cResp, err := file.Create(ctx, fileSize, azfile.FileHTTPHeaders{}, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)

	return file, name
}

func validateStorageError(c *chk.C, err error, code azfile.ServiceCodeType) {
	c.Assert(err, chk.NotNil)

	serr, _ := err.(azfile.StorageError)
	c.Assert(serr.ServiceCode(), chk.Equals, code)
}
