package azfile_test

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-file-go/2017-04-17/azfile"
	chk "gopkg.in/check.v1"
)

type ShareURLSuite struct{}

var _ = chk.Suite(&ShareURLSuite{})

func delShare(c *chk.C, share azfile.ShareURL, option azfile.DeleteSnapshotsOptionType) {
	resp, err := share.Delete(context.Background(), option)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Response().StatusCode, chk.Equals, 202)
}

func (s *ShareURLSuite) TestShareCreateDelete(c *chk.C) {
	shareName := generateShareName()
	sa := getFSU()
	share := sa.NewShareURL(shareName)

	md := azfile.Metadata{
		"foo": "FooValuE",
		"bar": "bArvaLue",
	}

	quota := int32(1000)

	cResp, err := share.Create(context.Background(), md, quota)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.Response().StatusCode, chk.Equals, 201)
	c.Assert(cResp.Date().IsZero(), chk.Equals, false)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(cResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(cResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Version(), chk.Not(chk.Equals), "")

	shares, err := sa.ListSharesSegment(context.Background(), azfile.Marker{}, azfile.ListSharesOptions{Prefix: shareName, Detail: azfile.ListSharesDetail{Metadata: true}})
	c.Assert(err, chk.IsNil)
	c.Assert(shares.Shares, chk.HasLen, 1)
	c.Assert(shares.Shares[0].Name, chk.Equals, shareName)
	c.Assert(shares.Shares[0].Metadata, chk.DeepEquals, md)
	c.Assert(shares.Shares[0].Properties.Quota, chk.Equals, quota)

	dResp, err := share.Delete(context.Background(), azfile.DeleteSnapshotsOptionNone)
	c.Assert(err, chk.IsNil)
	c.Assert(dResp.Response().StatusCode, chk.Equals, 202)
	c.Assert(dResp.Date().IsZero(), chk.Equals, false)
	c.Assert(dResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(dResp.Version(), chk.Not(chk.Equals), "")

	shares, err = sa.ListSharesSegment(context.Background(), azfile.Marker{}, azfile.ListSharesOptions{Prefix: shareName})
	c.Assert(err, chk.IsNil)
	c.Assert(shares.Shares, chk.HasLen, 0)
}

func (s *ShareURLSuite) TestShareGetSetProperties(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	newQuota := int32(1234)

	sResp, err := share.SetQuota(ctx, newQuota)
	c.Assert(err, chk.IsNil)
	c.Assert(sResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(sResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(sResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(sResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(sResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(sResp.Date().IsZero(), chk.Equals, false)

	props, err := share.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(props.Response().StatusCode, chk.Equals, 200)
	c.Assert(props.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(props.LastModified().IsZero(), chk.Equals, false)
	c.Assert(props.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(props.Version(), chk.Not(chk.Equals), "")
	c.Assert(props.Date().IsZero(), chk.Equals, false)
	c.Assert(props.Quota(), chk.Equals, newQuota)
}

func (s *ShareURLSuite) TestShareGetSetPermissions(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	now := time.Now().UTC().Truncate(10000 * time.Millisecond) // Enough resolution
	expiryTIme := now.Add(5 * time.Minute).UTC()
	permission := azfile.AccessPolicyPermission{
		Read:  true,
		Write: true,
	}.String()

	accessPolicy := azfile.AccessPolicy{
		Start:      &now,
		Expiry:     &expiryTIme,
		Permission: &permission,
	}

	permissions := []azfile.SignedIdentifier{
		{
			ID:           "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI=",
			AccessPolicy: &accessPolicy,
		}}

	sResp, err := share.SetPermissions(context.Background(), permissions)
	c.Assert(err, chk.IsNil)
	c.Assert(sResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(sResp.Date().IsZero(), chk.Equals, false)
	c.Assert(sResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(sResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(sResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(sResp.Version(), chk.Not(chk.Equals), "")

	gResp, err := share.GetPermissions(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(gResp.Date().IsZero(), chk.Equals, false)
	c.Assert(gResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(gResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(gResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(gResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(gResp.Value, chk.HasLen, 1)
	c.Assert(gResp.Value[0], chk.DeepEquals, permissions[0])
}

func (s *ShareURLSuite) TestShareGetSetMetadata(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	md := azfile.Metadata{
		"foo": "FooValuE",
		"bar": "bArvaLue", // Note: As testing result, currently only support case-insensitive keys(key will be saved in lower-case).
	}
	sResp, err := share.SetMetadata(context.Background(), md)
	c.Assert(err, chk.IsNil)
	c.Assert(sResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(sResp.Date().IsZero(), chk.Equals, false)
	c.Assert(sResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(sResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(sResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(sResp.Version(), chk.Not(chk.Equals), "")

	gResp, err := share.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(gResp.Date().IsZero(), chk.Equals, false)
	c.Assert(gResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(gResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(gResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(gResp.Version(), chk.Not(chk.Equals), "")
	nmd := gResp.NewMetadata()
	c.Assert(nmd, chk.DeepEquals, md)
}

func (s *ShareURLSuite) TestShareGetStats(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	newQuota := int32(300)

	// In order to test and get LastModified property.
	sResp, err := share.SetQuota(context.Background(), newQuota)
	c.Assert(err, chk.IsNil)
	c.Assert(sResp.Response().StatusCode, chk.Equals, 200)

	gResp, err := share.GetStatistics(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(gResp.Date().IsZero(), chk.Equals, false)
	// c.Assert(gResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone) // TODO: The ETag would be ""
	// c.Assert(gResp.LastModified().IsZero(), chk.Equals, false) // TODO: Even share is once updated, no LastModified would be returned.
	c.Assert(gResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(gResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(gResp.ShareUsage, chk.Equals, int32(0)) // TODO: Create and transfer one file, and get stats again.
}

func (s *ShareURLSuite) TestShareCreateSnapshot(c *chk.C) {
	fsu := getFSU()
	share, shareName := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionInclude)

	ctx := context.Background()

	md := azfile.Metadata{
		"foo": "FooValuE",
		"bar": "bArvaLue",
	}

	cResp, err := share.CreateSnapshot(ctx, md)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.Response().StatusCode, chk.Equals, 201)
	c.Assert(cResp.Date().IsZero(), chk.Equals, false)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(cResp.LastModified().IsZero(), chk.Equals, false)
	c.Assert(cResp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Version(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Snapshot(), chk.Not(chk.Equals), nil)

	cSnapshot := cResp.Snapshot()

	lResp, err := fsu.ListSharesSegment(
		ctx, azfile.Marker{},
		azfile.ListSharesOptions{
			Detail: azfile.ListSharesDetail{
				Metadata:  true,
				Snapshots: true,
			},
			Prefix: shareName,
		})

	c.Assert(err, chk.IsNil)
	c.Assert(lResp.Response().StatusCode, chk.Equals, 200)
	c.Assert(lResp.Shares, chk.HasLen, 2)

	if lResp.Shares[0].Snapshot != nil {
		c.Assert(*(lResp.Shares[0].Snapshot), chk.Equals, cSnapshot)
		c.Assert(lResp.Shares[0].Metadata, chk.DeepEquals, md)
		c.Assert(len(lResp.Shares[1].Metadata), chk.Equals, 0)
	} else {
		c.Assert(*(lResp.Shares[1].Snapshot), chk.Equals, cSnapshot)
		c.Assert(lResp.Shares[1].Metadata, chk.DeepEquals, md)
		c.Assert(len(lResp.Shares[0].Metadata), chk.Equals, 0)
	}

}

func (s *ShareURLSuite) TestShareSnapshot(c *chk.C) {
	credential, accountName := getCredential()

	ctx := context.Background()

	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net", accountName))
	serviceURL := azfile.NewServiceURL(*u, azfile.NewPipeline(credential, azfile.PipelineOptions{}))

	shareName := generateShareName()
	shareURL := serviceURL.NewShareURL(shareName)

	_, err := shareURL.Create(ctx, azfile.Metadata{}, 0)
	c.Assert(err, chk.IsNil)

	defer shareURL.Delete(ctx, azfile.DeleteSnapshotsOptionInclude)

	// Let's create a file in the base share.
	fileURL := shareURL.NewRootDirectoryURL().NewFileURL("myfile")
	_, err = fileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, azfile.Metadata{})
	c.Assert(err, chk.IsNil)

	// Create share snapshot, the snapshot contains the create file.
	snapshotShare, err := shareURL.CreateSnapshot(ctx, azfile.Metadata{})
	c.Assert(err, chk.IsNil)

	// Delete file in base share.
	_, err = fileURL.Delete(ctx)
	c.Assert(err, chk.IsNil)

	// Restore file from share snapshot.
	// Create a SAS.
	sasQueryParams := azfile.FileSASSignatureValues{
		Protocol:   azfile.SASProtocolHTTPS,              // Users MUST use HTTPS (not HTTP)
		ExpiryTime: time.Now().UTC().Add(48 * time.Hour), // 48-hours before expiration
		ShareName:  shareName,

		// To produce a share SAS (as opposed to a file SAS), assign to Permissions using
		// ShareSASPermissions and make sure the DirectoryAndFilePath field is "" (the default).
		Permissions: azfile.ShareSASPermissions{Read: true, Write: true}.String(),
	}.NewSASQueryParameters(credential)

	// Build a file snapshot URL.
	fileParts := azfile.NewFileURLParts(fileURL.URL())
	fileParts.ShareSnapshot = snapshotShare.Snapshot()
	fileParts.SAS = sasQueryParams
	sourceURL := fileParts.URL()

	// Do restore.
	_, err = fileURL.StartCopy(ctx, sourceURL, azfile.Metadata{})
	c.Assert(err, chk.IsNil)
}
