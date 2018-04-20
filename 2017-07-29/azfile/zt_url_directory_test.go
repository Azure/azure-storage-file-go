package azfile_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
	chk "gopkg.in/check.v1"
)

type DirectoryURLSuite struct{}

var _ = chk.Suite(&DirectoryURLSuite{})

func delDirectory(c *chk.C, directory azfile.DirectoryURL) {
	resp, err := directory.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Response().StatusCode, chk.Equals, 202)
}

func (s *DirectoryURLSuite) TestDirNewDirectoryURL(c *chk.C) {
	fsu := getFSU()
	testURL := fsu.NewShareURL(sharePrefix).NewDirectoryURL(directoryPrefix).NewDirectoryURL(directoryPrefix)

	correctURL := "https://" + os.Getenv("ACCOUNT_NAME") + ".file.core.windows.net/" + sharePrefix + "/" + directoryPrefix + "/" + directoryPrefix
	temp := testURL.URL()
	c.Assert(temp.String(), chk.Equals, correctURL)
	c.Assert(testURL.String(), chk.Equals, correctURL)
}

func (s *DirectoryURLSuite) TestDirNewDirectoryURLNegative(c *chk.C) {
	c.Assert(func() { azfile.NewDirectoryURL(url.URL{}, nil) }, chk.Panics, "p can't be nil")
}

func (s *DirectoryURLSuite) TestDirCreateFileURL(c *chk.C) {
	fsu := getFSU()
	testURL := fsu.NewShareURL(sharePrefix).NewDirectoryURL(directoryPrefix).NewFileURL(filePrefix)

	correctURL := "https://" + os.Getenv("ACCOUNT_NAME") + ".file.core.windows.net/" + sharePrefix + "/" + directoryPrefix + "/" + filePrefix
	temp := testURL.URL()
	c.Assert(temp.String(), chk.Equals, correctURL)
}

func (s *DirectoryURLSuite) TestDirWithNewPipeline(c *chk.C) {
	fsu := getFSU()
	dirURL := fsu.NewShareURL(sharePrefix).NewDirectoryURL(directoryPrefix)

	newDirURL := dirURL.WithPipeline(testPipeline{})
	_, err := newDirURL.Create(ctx, azfile.Metadata{})
	c.Assert(err, chk.NotNil)
	c.Assert(err.Error(), chk.Equals, testPipelineMessage)
}

func (s *DirectoryURLSuite) TestDirCreateDeleteDefault(c *chk.C) {
	directoryName := generateDirectoryName()
	sa := getFSU()
	share, _ := createNewShare(c, sa)

	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	directory := share.NewDirectoryURL(directoryName)

	cResp, err := directory.Create(context.Background(), azfile.Metadata{})
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.Response().StatusCode, chk.Equals, 201)
	c.Assert(cResp.Date().IsZero(), chk.Equals, false)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(cResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(cResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Version(), chk.Not(chk.Equals), "")

	gResp, err := directory.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.StatusCode(), chk.Equals, 200)

	defer delDirectory(c, directory)
}

func (s *DirectoryURLSuite) TestDirCreateDeleteNonDefault(c *chk.C) {
	directoryName := generateDirectoryName()
	sa := getFSU()
	share, _ := createNewShare(c, sa)

	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	directory := share.NewDirectoryURL(directoryName)

	md := azfile.Metadata{
		"foo": "FooValuE",
		"bar": "bArvaLue",
	}

	cResp, err := directory.Create(context.Background(), md)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.Response().StatusCode, chk.Equals, 201)
	c.Assert(cResp.Date().IsZero(), chk.Equals, false)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(cResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(cResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Version(), chk.Not(chk.Equals), "")

	gResp, err := directory.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.StatusCode(), chk.Equals, 200)

	// Creating again will result in 409 and ResourceAlreadyExists.
	cResp, err = directory.Create(context.Background(), md)
	c.Assert(err, chk.Not(chk.IsNil))
	serr := err.(azfile.StorageError)
	c.Assert(serr.Response().StatusCode, chk.Equals, 409)
	c.Assert(serr.ServiceCode(), chk.Equals, azfile.ServiceCodeResourceAlreadyExists)

	dResp, err := directory.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(dResp.Response().StatusCode, chk.Equals, 202)
	c.Assert(dResp.Date().IsZero(), chk.Equals, false)
	c.Assert(dResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(dResp.Version(), chk.Not(chk.Equals), "")

	gResp, err = directory.GetProperties(context.Background())
	c.Assert(err, chk.NotNil)
	serr = err.(azfile.StorageError)
	c.Assert(serr.Response().StatusCode, chk.Equals, 404)
}

func (s *DirectoryURLSuite) TestDirCreateDeleteNegativeMultiLevelDir(c *chk.C) {
	parentDirName := generateDirectoryName()
	subDirName := generateDirectoryName()
	sa := getFSU()
	share, _ := createNewShare(c, sa)

	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	parentDirURL := share.NewDirectoryURL(parentDirName)

	subDirURL := parentDirURL.NewDirectoryURL(subDirName)

	// Directory create with subDirURL
	cResp, err := subDirURL.Create(context.Background(), nil)
	c.Assert(err, chk.NotNil)
	serr := err.(azfile.StorageError)
	c.Assert(serr.Response().StatusCode, chk.Equals, 404)
	c.Assert(serr.ServiceCode(), chk.Equals, azfile.ServiceCodeParentNotFound)

	cResp, err = parentDirURL.Create(context.Background(), nil)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.Response().StatusCode, chk.Equals, 201)

	cResp, err = subDirURL.Create(context.Background(), nil)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.Response().StatusCode, chk.Equals, 201)

	gResp, err := subDirURL.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.StatusCode(), chk.Equals, 200)

	// Delete level by level
	// Delete Non-empty directory should fail
	_, err = parentDirURL.Delete(context.Background())
	c.Assert(err, chk.NotNil)
	serr = err.(azfile.StorageError)
	c.Assert(serr.ServiceCode(), chk.Equals, azfile.ServiceCodeDirectoryNotEmpty)

	dResp, err := subDirURL.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(dResp.StatusCode(), chk.Equals, 202)

	dResp, err = parentDirURL.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(dResp.StatusCode(), chk.Equals, 202)
}

func (s *DirectoryURLSuite) TestDirCreateEndWithSlash(c *chk.C) {
	directoryName := generateDirectoryName() + "/"
	sa := getFSU()
	share, _ := createNewShare(c, sa)

	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	directory := share.NewDirectoryURL(directoryName)

	defer delDirectory(c, directory)

	cResp, err := directory.Create(context.Background(), nil)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.Response().StatusCode, chk.Equals, 201)
	c.Assert(cResp.Date().IsZero(), chk.Equals, false)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(cResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(cResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Version(), chk.Not(chk.Equals), "")

	gResp, err := directory.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.StatusCode(), chk.Equals, 200)
}

func (s *DirectoryURLSuite) TestDirGetSetMetadataDefault(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	directory, _ := createNewDirectoryFromShare(c, share)
	defer delDirectory(c, directory)

	sResp, err := directory.SetMetadata(context.Background(), azfile.Metadata{})
	c.Assert(err, chk.IsNil)
	c.Assert(sResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(sResp.Date().IsZero(), chk.Equals, false)
	c.Assert(sResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(sResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(sResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(sResp.IsServerEncrypted(), chk.NotNil)

	gResp, err := directory.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(gResp.Date().IsZero(), chk.Equals, false)
	c.Assert(gResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(gResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(gResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(gResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(gResp.IsServerEncrypted(), chk.NotNil)
	c.Assert(gResp.NewMetadata(), chk.DeepEquals, azfile.Metadata{})
}

func (s *DirectoryURLSuite) TestDirGetSetMetadataNonDefault(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	directory, _ := createNewDirectoryFromShare(c, share)
	defer delDirectory(c, directory)

	md := azfile.Metadata{
		"foo": "FooValuE",
		"bar": "bArvaLue",
	}

	sResp, err := directory.SetMetadata(context.Background(), md)
	c.Assert(err, chk.IsNil)
	c.Assert(sResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(sResp.Date().IsZero(), chk.Equals, false)
	c.Assert(sResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(sResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(sResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(sResp.IsServerEncrypted(), chk.NotNil)

	gResp, err := directory.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(gResp.Date().IsZero(), chk.Equals, false)
	c.Assert(gResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(gResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(gResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(gResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(gResp.IsServerEncrypted(), chk.NotNil)
	nmd := gResp.NewMetadata()
	c.Assert(nmd, chk.DeepEquals, md)
}

func (s *DirectoryURLSuite) TestDirSetMetadataNegative(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	directory, _ := createNewDirectoryFromShare(c, share)
	defer delDirectory(c, directory)

	md := azfile.Metadata{
		"foo 123": "FooValuE",
	}

	_, err := directory.SetMetadata(context.Background(), md)
	c.Assert(err, chk.NotNil)
	c.Assert(strings.Contains(err.Error(), validationErrorSubstring), chk.Equals, true)
}

func (s *DirectoryURLSuite) TestDirGetPropertiesNegative(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	dir, _ := getDirectoryURLFromShare(c, share)

	_, err := dir.GetProperties(ctx)
	c.Assert(err, chk.NotNil)
	validateStorageError(c, err, azfile.ServiceCodeResourceNotFound)
}

func (s *DirectoryURLSuite) TestDirGetPropertiesWithBaseDirectory(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	directory := share.NewRootDirectoryURL()

	gResp, err := directory.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(gResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(gResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(gResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(gResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(gResp.Date().IsZero(), chk.Equals, false)
	c.Assert(gResp.IsServerEncrypted(), chk.NotNil)
}

// Merge is not supported, as the key of metadata would be canonicalized
func (s *DirectoryURLSuite) TestDirGetSetMetadataMergeAndReplace(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	directory, _ := createNewDirectoryFromShare(c, share)
	defer delDirectory(c, directory)

	md := azfile.Metadata{
		"color": "RED",
	}

	sResp, err := directory.SetMetadata(context.Background(), md)
	c.Assert(err, chk.IsNil)
	c.Assert(sResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(sResp.Date().IsZero(), chk.Equals, false)
	c.Assert(sResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(sResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(sResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(sResp.IsServerEncrypted(), chk.NotNil)

	gResp, err := directory.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(gResp.Date().IsZero(), chk.Equals, false)
	c.Assert(gResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(gResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(gResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(gResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(gResp.IsServerEncrypted(), chk.NotNil)
	nmd := gResp.NewMetadata()
	c.Assert(nmd, chk.DeepEquals, md)
	// c.Assert(nmd, chk.DeepEquals, azfile.Metadata{
	// 	"color": "RED,green",
	// })

	md2 := azfile.Metadata{
		"color": "WHITE",
		//"COLOR": "black", // Note: metadata's key should only be lowercase
	}

	sResp, err = directory.SetMetadata(context.Background(), md2)
	c.Assert(err, chk.IsNil)
	c.Assert(sResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(sResp.Date().IsZero(), chk.Equals, false)
	c.Assert(sResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(sResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(sResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(sResp.IsServerEncrypted(), chk.NotNil)

	gResp, err = directory.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(gResp.Date().IsZero(), chk.Equals, false)
	c.Assert(gResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(gResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(gResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(gResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(gResp.IsServerEncrypted(), chk.NotNil)
	nmd2 := gResp.NewMetadata()
	c.Assert(nmd2, chk.DeepEquals, md2)
	// c.Assert(nmd2, chk.DeepEquals, azfile.Metadata{
	// 	"color": "WHITE,black",
	// })
}

func (s *DirectoryURLSuite) TestDirListDefault(c *chk.C) {
	fsu := getFSU()
	share, shareName := createNewShare(c, fsu)

	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	dir, dirName := createNewDirectoryFromShare(c, share)

	defer delDirectory(c, dir)

	// Empty directory
	lResp, err := dir.ListFilesAndDirectoriesSegment(context.Background(), azfile.Marker{}, azfile.ListFilesAndDirectoriesOptions{})
	c.Assert(err, chk.IsNil)
	c.Assert(lResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(lResp.StatusCode(), chk.Equals, 200)
	c.Assert(lResp.Status(), chk.Not(chk.Equals), "")
	c.Assert(lResp.ContentType(), chk.Not(chk.Equals), "")
	c.Assert(lResp.Date().IsZero(), chk.Equals, false)
	c.Assert(lResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(lResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(lResp.DirectoryPath, chk.Equals, dirName)
	c.Assert(lResp.ShareName, chk.Equals, shareName)
	c.Assert(lResp.ServiceEndpoint, chk.NotNil)
	c.Assert(lResp.ShareSnapshot, chk.IsNil)
	c.Assert(lResp.Prefix, chk.Equals, "")
	c.Assert(lResp.MaxResults, chk.IsNil)
	c.Assert(lResp.Files, chk.HasLen, 0)
	c.Assert(lResp.Directories, chk.HasLen, 0)

	innerDir, innerDirName := createNewDirectoryWithPrefix(c, dir, "111")
	defer delDirectory(c, innerDir)

	innerFile, innerFileName := createNewFileWithPrefix(c, dir, "111", 0)
	defer delFile(c, innerFile)

	// List 1 file, 1 directory
	lResp2, err := dir.ListFilesAndDirectoriesSegment(context.Background(), azfile.Marker{}, azfile.ListFilesAndDirectoriesOptions{})
	c.Assert(err, chk.IsNil)
	c.Assert(lResp2.Response().StatusCode, chk.Equals, 200)
	c.Assert(lResp2.Directories, chk.HasLen, 1)
	c.Assert(lResp2.Directories[0].Name, chk.Equals, innerDirName)
	c.Assert(lResp2.Files, chk.HasLen, 1)
	c.Assert(lResp2.Files[0].Name, chk.Equals, innerFileName)
	c.Assert(lResp2.Files[0].Properties.ContentLength, chk.Equals, int64(0))

	innerDir2, innerDirName2 := createNewDirectoryWithPrefix(c, dir, "222")
	defer delDirectory(c, innerDir2)

	innerFile2, innerFileName2 := createNewFileWithPrefix(c, dir, "222", 2)
	defer delFile(c, innerFile2)

	// List 2 files and 2 directories
	lResp3, err := dir.ListFilesAndDirectoriesSegment(context.Background(), azfile.Marker{}, azfile.ListFilesAndDirectoriesOptions{})
	c.Assert(err, chk.IsNil)
	c.Assert(lResp3.Response().StatusCode, chk.Equals, 200)
	c.Assert(lResp3.Directories, chk.HasLen, 2)
	c.Assert(lResp3.Directories[0].Name, chk.Equals, innerDirName)
	c.Assert(lResp3.Directories[1].Name, chk.Equals, innerDirName2)
	c.Assert(lResp3.Files, chk.HasLen, 2)
	c.Assert(lResp3.Files[0].Name, chk.Equals, innerFileName)
	c.Assert(lResp3.Files[0].Properties.ContentLength, chk.Equals, int64(0))
	c.Assert(lResp3.Files[1].Name, chk.Equals, innerFileName2)
	c.Assert(lResp3.Files[1].Properties.ContentLength, chk.Equals, int64(2))
}

func (s *DirectoryURLSuite) TestDirListNonDefault(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)

	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	dir, _ := createNewDirectoryFromShare(c, share)

	defer delDirectory(c, dir)

	const testPrefix = "pagedprefix"
	const maxResultsPerPage = 2

	dir1, dir1Name := createNewDirectoryWithPrefix(c, dir, testPrefix+"1")
	file1, file1Name := createNewFileWithPrefix(c, dir, testPrefix+"2", 0)
	dir2, dir2Name := createNewDirectoryWithPrefix(c, dir, testPrefix+"3")
	file2, file2Name := createNewFileWithPrefix(c, dir, testPrefix+"4", 0)

	defer func() {
		delDirectory(c, dir1)
		delDirectory(c, dir2)
		delFile(c, file1)
		delFile(c, file2)
	}()

	marker := azfile.Marker{}

	lResp, err := dir.ListFilesAndDirectoriesSegment(context.Background(), marker, azfile.ListFilesAndDirectoriesOptions{MaxResults: maxResultsPerPage, Prefix: testPrefix})
	c.Assert(err, chk.IsNil)
	c.Assert(lResp.Files, chk.HasLen, 1)
	c.Assert(lResp.Files[0].Name, chk.Equals, file1Name)
	c.Assert(lResp.Directories, chk.HasLen, 1)
	c.Assert(lResp.Directories[0].Name, chk.Equals, dir1Name)

	c.Assert(lResp.NextMarker.NotDone(), chk.Equals, true)
	marker = lResp.NextMarker

	lResp, err = dir.ListFilesAndDirectoriesSegment(context.Background(), marker, azfile.ListFilesAndDirectoriesOptions{MaxResults: maxResultsPerPage, Prefix: testPrefix})
	c.Assert(err, chk.IsNil)
	c.Assert(lResp.Files, chk.HasLen, 1)
	c.Assert(lResp.Files[0].Name, chk.Equals, file2Name)
	c.Assert(lResp.Directories, chk.HasLen, 1)
	c.Assert(lResp.Directories[0].Name, chk.Equals, dir2Name)

	c.Assert(lResp.NextMarker.NotDone(), chk.Equals, false)
}

func (s *DirectoryURLSuite) TestDirListNegativeNonexistantPrefix(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	createNewFileFromShare(c, shareURL, 0)

	dirURL := shareURL.NewRootDirectoryURL()

	resp, err := dirURL.ListFilesAndDirectoriesSegment(ctx, azfile.Marker{}, azfile.ListFilesAndDirectoriesOptions{Prefix: filePrefix + filePrefix})

	c.Assert(err, chk.IsNil)
	c.Assert(resp.Files, chk.HasLen, 0)
}

func (s *DirectoryURLSuite) TestDirListNegativeMaxResults(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	createNewFileFromShare(c, shareURL, 0)
	dirURL := shareURL.NewRootDirectoryURL()

	// If ListFilesAndDirectoriesSegment panics, as it should, this function will be called and recover from the panic, allowing the test to pass
	defer func() {
		recover()
	}()
	dirURL.ListFilesAndDirectoriesSegment(ctx, azfile.Marker{}, azfile.ListFilesAndDirectoriesOptions{MaxResults: -2})

	// We will only reach this if we did not panic
	c.Fail()
}

func (s *DirectoryURLSuite) TestDirListNonDefaultMaxResultsZero(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	createNewFileFromShare(c, shareURL, 0)
	dirURL := shareURL.NewRootDirectoryURL()

	resp, err := dirURL.ListFilesAndDirectoriesSegment(ctx, azfile.Marker{}, azfile.ListFilesAndDirectoriesOptions{MaxResults: 0})

	c.Assert(err, chk.IsNil)
	c.Assert(resp.Files, chk.HasLen, 1)
}

func (s *DirectoryURLSuite) TestDirListNonDefaultMaxResultsExact(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	dirURL := shareURL.NewRootDirectoryURL()

	additionalPrefix := strconv.Itoa(time.Now().Nanosecond())
	_, dirName1 := createNewDirectoryWithPrefix(c, dirURL, additionalPrefix+"a")
	_, dirName2 := createNewDirectoryWithPrefix(c, dirURL, additionalPrefix+"b")

	resp, err := dirURL.ListFilesAndDirectoriesSegment(ctx, azfile.Marker{}, azfile.ListFilesAndDirectoriesOptions{MaxResults: 2, Prefix: additionalPrefix})

	c.Assert(err, chk.IsNil)
	c.Assert(resp.Directories, chk.HasLen, 2)
	c.Assert(resp.Directories[0].Name, chk.Equals, dirName1)
	c.Assert(resp.Directories[1].Name, chk.Equals, dirName2)
}

// Test list directories with SAS
func (s *DirectoryURLSuite) TestDirListWithShareSAS(c *chk.C) {
	fsu := getFSU()
	credential, accountName := getCredential()
	share, shareName := createNewShare(c, fsu)

	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	dir, dirName := createNewDirectoryFromShare(c, share)

	defer delDirectory(c, dir)

	// Create share service SAS
	sasQueryParams := azfile.FileSASSignatureValues{
		Protocol:    azfile.SASProtocolHTTPS,              // Users MUST use HTTPS (not HTTP)
		ExpiryTime:  time.Now().UTC().Add(48 * time.Hour), // 48-hours before expiration
		ShareName:   shareName,
		Permissions: azfile.ShareSASPermissions{Read: true, Write: true, List: true}.String(),
	}.NewSASQueryParameters(credential)

	// Create the URL of the resource you wish to access and append the SAS query parameters.
	// Since this is a file SAS, the URL is to the Azure storage file.
	qp := sasQueryParams.Encode()
	urlToSendToSomeone := fmt.Sprintf("https://%s.file.core.windows.net/%s/%s?%s",
		accountName, shareName, dirName, qp)

	u, _ := url.Parse(urlToSendToSomeone)
	dirURL := azfile.NewDirectoryURL(*u, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))

	marker := azfile.Marker{}
	lResp, err := dirURL.ListFilesAndDirectoriesSegment(context.Background(), marker, azfile.ListFilesAndDirectoriesOptions{})
	c.Assert(err, chk.IsNil)
	c.Assert(lResp.NextMarker.NotDone(), chk.Equals, false)
}
