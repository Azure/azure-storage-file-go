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
		//"COLOR": "green", // TODO case sensitive metadata
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
		//"COLOR": "black", // TODO case sensitive metadata
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

// TODO: Test ListDirectoriesAndFiles
