package azfile_test

import (
	"context"

	"github.com/Azure/azure-storage-file-go/2017-04-17/azfile"
	chk "gopkg.in/check.v1"
)

type DirectoryURLSuite struct{}

var _ = chk.Suite(&DirectoryURLSuite{})

func delDirectory(c *chk.C, directory azfile.DirectoryURL) {
	resp, err := directory.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Response().StatusCode, chk.Equals, 202)
}

func (s *DirectoryURLSuite) TestDirCreateDelete(c *chk.C) {
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

	gResp, err := directory.GetPropertiesAndMetadata(context.Background())
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

	gResp, err = directory.GetPropertiesAndMetadata(context.Background())
	c.Assert(err, chk.NotNil)
	serr = err.(azfile.StorageError)
	c.Assert(serr.Response().StatusCode, chk.Equals, 404)
}

// Negative
func (s *DirectoryURLSuite) TestDirCreateMultiLevelDir(c *chk.C) {
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

	gResp, err := subDirURL.GetPropertiesAndMetadata(context.Background())
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

	gResp, err := directory.GetPropertiesAndMetadata(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.StatusCode(), chk.Equals, 200)
}

func (s *DirectoryURLSuite) TestDirGetSetMetadata(c *chk.C) {
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

	gResp, err := directory.GetPropertiesAndMetadata(context.Background())
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

func (s *DirectoryURLSuite) TestDirGetPropertiesWithBaseDirectory(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	directory := share.NewRootDirectoryURL()

	gResp, err := directory.GetPropertiesAndMetadata(context.Background())
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
		//"COLOR": "green", // TODO: case sensitive metadata
	}

	sResp, err := directory.SetMetadata(context.Background(), md)
	c.Assert(err, chk.IsNil)
	c.Assert(sResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(sResp.Date().IsZero(), chk.Equals, false)
	c.Assert(sResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(sResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(sResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(sResp.IsServerEncrypted(), chk.NotNil)

	gResp, err := directory.GetPropertiesAndMetadata(context.Background())
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
		//"COLOR": "black", // TODO: case sensitive metadata
	}

	sResp, err = directory.SetMetadata(context.Background(), md2)
	c.Assert(err, chk.IsNil)
	c.Assert(sResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(sResp.Date().IsZero(), chk.Equals, false)
	c.Assert(sResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(sResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(sResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(sResp.IsServerEncrypted(), chk.NotNil)

	gResp, err = directory.GetPropertiesAndMetadata(context.Background())
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

func (s *DirectoryURLSuite) TestListFilesAndDirectoriesBasic(c *chk.C) {
	fsu := getFSU()
	share, shareName := createNewShare(c, fsu)

	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	dir, dirName := createNewDirectoryFromShare(c, share)

	defer delDirectory(c, dir)

	// Empty directory
	lResp, err := dir.ListDirectoriesAndFiles(context.Background(), azfile.Marker{}, azfile.ListDirectoriesAndFilesOptions{})
	c.Assert(err, chk.IsNil)
	c.Assert(lResp.Response().StatusCode, chk.Equals, 200)
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

	innerDir, innerDirName := createNewDirectoryFromDirectory(c, dir)
	defer delDirectory(c, innerDir)

	innerFile, innerFileName := createNewFileFromDirectory(c, dir, 0)
	defer delFile(c, innerFile)

	// List 1 file, 1 directory
	lResp2, err := dir.ListDirectoriesAndFiles(context.Background(), azfile.Marker{}, azfile.ListDirectoriesAndFilesOptions{})
	c.Assert(err, chk.IsNil)
	c.Assert(lResp2.Response().StatusCode, chk.Equals, 200)
	c.Assert(lResp2.Directories, chk.HasLen, 1)
	c.Assert(lResp2.Directories[0].Name, chk.Equals, innerDirName)
	c.Assert(lResp2.Files, chk.HasLen, 1)
	c.Assert(lResp2.Files[0].Name, chk.Equals, innerFileName)
	c.Assert(lResp2.Files[0].Properties.ContentLength, chk.Equals, int64(0))

	innerDir2, innerDirName2 := createNewDirectoryFromDirectory(c, dir)
	defer delDirectory(c, innerDir2)

	innerFile2, innerFileName2 := createNewFileFromDirectory(c, dir, 2)
	defer delFile(c, innerFile2)

	// List 2 files and 2 directories
	lResp3, err := dir.ListDirectoriesAndFiles(context.Background(), azfile.Marker{}, azfile.ListDirectoriesAndFilesOptions{})
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

func (s *DirectoryURLSuite) TestListFilesAndDirectoriesWithPrefix(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)

	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	dir, _ := createNewDirectoryFromShare(c, share)

	defer delDirectory(c, dir)

	const testPrefix = "pagedprefix"
	const maxResultsPerPage = 2

	dir1, dir1Name := createNewDirectoryWithPrefix(c, dir, testPrefix+"1")
	file1, file1Name := createNewFileWithPrefix(c, dir, testPrefix+"2")
	dir2, dir2Name := createNewDirectoryWithPrefix(c, dir, testPrefix+"3")
	file2, file2Name := createNewFileWithPrefix(c, dir, testPrefix+"4")

	defer func() {
		delDirectory(c, dir1)
		delDirectory(c, dir2)
		delFile(c, file1)
		delFile(c, file2)
	}()

	marker := azfile.Marker{}

	lResp, err := dir.ListDirectoriesAndFiles(context.Background(), marker, azfile.ListDirectoriesAndFilesOptions{MaxResults: maxResultsPerPage, Prefix: testPrefix})
	c.Assert(err, chk.IsNil)
	c.Assert(lResp.Files, chk.HasLen, 1)
	c.Assert(lResp.Files[0].Name, chk.Equals, file1Name)
	c.Assert(lResp.Directories, chk.HasLen, 1)
	c.Assert(lResp.Directories[0].Name, chk.Equals, dir1Name)

	c.Assert(lResp.NextMarker.NotDone(), chk.Equals, true)
	marker = lResp.NextMarker

	lResp, err = dir.ListDirectoriesAndFiles(context.Background(), marker, azfile.ListDirectoriesAndFilesOptions{MaxResults: maxResultsPerPage, Prefix: testPrefix})
	c.Assert(err, chk.IsNil)
	c.Assert(lResp.Files, chk.HasLen, 1)
	c.Assert(lResp.Files[0].Name, chk.Equals, file2Name)
	c.Assert(lResp.Directories, chk.HasLen, 1)
	c.Assert(lResp.Directories[0].Name, chk.Equals, dir2Name)

	c.Assert(lResp.NextMarker.NotDone(), chk.Equals, false)
}
