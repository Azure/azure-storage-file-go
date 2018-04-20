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

type ShareURLSuite struct{}

var _ = chk.Suite(&ShareURLSuite{})

func delShare(c *chk.C, share azfile.ShareURL, option azfile.DeleteSnapshotsOptionType) {
	resp, err := share.Delete(context.Background(), option)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Response().StatusCode, chk.Equals, 202)
}

func (s *ShareURLSuite) TestShareCreateRootDirectoryURL(c *chk.C) {
	fsu := getFSU()
	testURL := fsu.NewShareURL(sharePrefix).NewRootDirectoryURL()

	correctURL := "https://" + os.Getenv("ACCOUNT_NAME") + ".file.core.windows.net/" + sharePrefix
	temp := testURL.URL()
	c.Assert(temp.String(), chk.Equals, correctURL)
}

func (s *ShareURLSuite) TestShareCreateDirectoryURL(c *chk.C) {
	fsu := getFSU()
	testURL := fsu.NewShareURL(sharePrefix).NewDirectoryURL(directoryPrefix)

	correctURL := "https://" + os.Getenv("ACCOUNT_NAME") + ".file.core.windows.net/" + sharePrefix + "/" + directoryPrefix
	temp := testURL.URL()
	c.Assert(temp.String(), chk.Equals, correctURL)
	c.Assert(testURL.String(), chk.Equals, correctURL)
}

func (s *ShareURLSuite) TestShareNewShareURLNegative(c *chk.C) {
	c.Assert(func() { azfile.NewShareURL(url.URL{}, nil) }, chk.Panics, "p can't be nil")
}

func (s *ShareURLSuite) TestShareWithNewPipeline(c *chk.C) {
	fsu := getFSU()
	pipeline := testPipeline{}
	shareURL, _ := getShareURL(c, fsu)
	shareURL = shareURL.WithPipeline(pipeline)

	_, err := shareURL.Create(ctx, azfile.Metadata{}, 0)

	c.Assert(err, chk.NotNil)
	c.Assert(err.Error(), chk.Equals, testPipelineMessage)
}

// Note: test share create with default parameter is covered with preparing phase for FileURL and etc.
func (s *ShareURLSuite) TestShareCreateDeleteNonDefault(c *chk.C) {
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

func (s *ShareURLSuite) TestShareCreateNilMetadata(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := getShareURL(c, fsu)

	_, err := shareURL.Create(ctx, nil, 0)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	c.Assert(err, chk.IsNil)

	response, err := shareURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(response.NewMetadata(), chk.HasLen, 0)
}

func (s *ShareURLSuite) TestShareCreateNegativeInvalidName(c *chk.C) {
	fsu := getFSU()
	shareURL := fsu.NewShareURL("foo bar")

	_, err := shareURL.Create(ctx, azfile.Metadata{}, 0)

	validateStorageError(c, err, azfile.ServiceCodeInvalidResourceName)
}

func (s *ShareURLSuite) TestShareCreateNegativeInvalidMetadata(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := getShareURL(c, fsu)

	_, err := shareURL.Create(ctx, azfile.Metadata{"1 foo": "bar"}, 0)

	c.Assert(err, chk.NotNil)
	c.Assert(strings.Contains(err.Error(), validationErrorSubstring), chk.Equals, true)
}

func (s *ShareURLSuite) TestShareDeleteNegativeNonExistant(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := getShareURL(c, fsu)

	_, err := shareURL.Delete(ctx, azfile.DeleteSnapshotsOptionNone)
	validateStorageError(c, err, azfile.ServiceCodeShareNotFound)
}

func (s *ShareURLSuite) TestShareGetSetPropertiesNonDefault(c *chk.C) {
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

func (s *ShareURLSuite) TestShareGetSetPropertiesDefault(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	sResp, err := share.SetQuota(ctx, 0)
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
	c.Assert(props.Quota() >= 0, chk.Equals, true) // When using service default quota, it could be any value
}

func (s *ShareURLSuite) TestShareSetQuotaNegative(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	_, err := share.SetQuota(ctx, -1)
	c.Assert(err, chk.NotNil)
	c.Assert(strings.Contains(err.Error(), validationErrorSubstring), chk.Equals, true)
}

func (s *ShareURLSuite) TestShareGetPropertiesNegative(c *chk.C) {
	fsu := getFSU()
	share, _ := getShareURL(c, fsu)

	_, err := share.GetProperties(ctx)
	c.Assert(err, chk.NotNil)
	validateStorageError(c, err, azfile.ServiceCodeShareNotFound)
}

func (s *ShareURLSuite) TestShareGetSetPermissionsNonDefault(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	now := time.Now().UTC().Truncate(10000 * time.Millisecond) // Enough resolution
	expiryTIme := now.Add(5 * time.Minute).UTC()
	pS := azfile.AccessPolicyPermission{
		Read:   true,
		Write:  true,
		Create: true,
		Delete: true,
		List:   true,
	}
	pS2 := &azfile.AccessPolicyPermission{}
	pS2.Parse("ldcwr")
	c.Assert(*pS2, chk.DeepEquals, pS)

	permission := pS.String()

	permissions := []azfile.SignedIdentifier{
		{
			ID: "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI=",
			AccessPolicy: &azfile.AccessPolicy{
				Start:      &now,
				Expiry:     &expiryTIme,
				Permission: &permission,
			},
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

func (s *ShareURLSuite) TestShareGetSetPermissionsNonDefaultMultiple(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	now := time.Now().UTC().Truncate(10000 * time.Millisecond) // Enough resolution
	expiryTIme := now.Add(5 * time.Minute).UTC()
	permission := azfile.AccessPolicyPermission{
		Read:  true,
		Write: true,
	}.String()

	permissions := []azfile.SignedIdentifier{
		{
			ID: "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI=",
			AccessPolicy: &azfile.AccessPolicy{
				Start:      &now,
				Expiry:     &expiryTIme,
				Permission: &permission,
			},
		},
		{
			ID: "2",
			AccessPolicy: &azfile.AccessPolicy{
				Start:      &now,
				Expiry:     &expiryTIme,
				Permission: &permission,
			},
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
	c.Assert(gResp.Value, chk.HasLen, 2)
	c.Assert(gResp.Value[0], chk.DeepEquals, permissions[0])
}

func (s *ShareURLSuite) TestShareGetSetPermissionsDefault(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	sResp, err := share.SetPermissions(context.Background(), []azfile.SignedIdentifier{})
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
	c.Assert(gResp.Value, chk.HasLen, 0)
}

func (s *ShareURLSuite) TestShareGetPermissionNegative(c *chk.C) {
	fsu := getFSU()
	share, _ := getShareURL(c, fsu)

	_, err := share.GetPermissions(ctx)
	c.Assert(err, chk.NotNil)
	validateStorageError(c, err, azfile.ServiceCodeShareNotFound)
}

func (s *ShareURLSuite) TestShareSetPermissionsNonDefaultDeleteAndModifyACL(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)

	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	start := time.Now().UTC().Truncate(10000 * time.Millisecond)
	expiry := start.Add(5 * time.Minute).UTC()
	accessPermission := azfile.AccessPolicyPermission{List: true}.String()
	permissions := make([]azfile.SignedIdentifier, 2, 2)
	for i := 0; i < 2; i++ {
		permissions[i] = azfile.SignedIdentifier{
			ID: "000" + strconv.Itoa(i),
			AccessPolicy: &azfile.AccessPolicy{
				Start:      &start,
				Expiry:     &expiry,
				Permission: &accessPermission,
			},
		}
	}

	_, err := shareURL.SetPermissions(ctx, permissions)
	c.Assert(err, chk.IsNil)

	resp, err := shareURL.GetPermissions(ctx)
	c.Assert(err, chk.IsNil)

	c.Assert(resp.Value, chk.DeepEquals, permissions)

	permissions = resp.Value[:1] // Delete the first policy by removing it from the slice
	permissions[0].ID = "0004"   // Modify the remaining policy which is at index 0 in the new slice
	_, err = shareURL.SetPermissions(ctx, permissions)

	resp, err = shareURL.GetPermissions(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Value, chk.HasLen, 1)

	c.Assert(resp.Value, chk.DeepEquals, permissions)
}

func (s *ShareURLSuite) TestShareSetPermissionsDeleteAllPolicies(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)

	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	start := time.Now().UTC()
	expiry := start.Add(5 * time.Minute).UTC()
	accessPermission := azfile.AccessPolicyPermission{List: true}.String()
	permissions := make([]azfile.SignedIdentifier, 2, 2)
	for i := 0; i < 2; i++ {
		permissions[i] = azfile.SignedIdentifier{
			ID: "000" + strconv.Itoa(i),
			AccessPolicy: &azfile.AccessPolicy{
				Start:      &start,
				Expiry:     &expiry,
				Permission: &accessPermission,
			},
		}
	}

	_, err := shareURL.SetPermissions(ctx, permissions)
	c.Assert(err, chk.IsNil)

	_, err = shareURL.SetPermissions(ctx, []azfile.SignedIdentifier{})
	c.Assert(err, chk.IsNil)

	resp, err := shareURL.GetPermissions(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Value, chk.HasLen, 0)
}

// Note: No error happend
func (s *ShareURLSuite) TestShareSetPermissionsNegativeInvalidPolicyTimes(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)

	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	// Swap start and expiry
	expiry := time.Now().UTC()
	start := expiry.Add(5 * time.Minute).UTC()
	accessPermission := azfile.AccessPolicyPermission{List: true}.String()
	permissions := make([]azfile.SignedIdentifier, 2, 2)
	for i := 0; i < 2; i++ {
		permissions[i] = azfile.SignedIdentifier{
			ID: "000" + strconv.Itoa(i),
			AccessPolicy: &azfile.AccessPolicy{
				Start:      &start,
				Expiry:     &expiry,
				Permission: &accessPermission,
			},
		}
	}

	_, err := shareURL.SetPermissions(ctx, permissions)
	c.Assert(err, chk.IsNil)
}

func (s *ShareURLSuite) TestShareSetPermissionsNilPolicySlice(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)

	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	_, err := shareURL.SetPermissions(ctx, nil)
	c.Assert(err, chk.IsNil)
}

// SignedIdentifier ID too long
func (s *ShareURLSuite) TestShareSetPermissionsNegative(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)

	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	id := ""
	for i := 0; i < 65; i++ {
		id += "a"
	}
	expiry := time.Now().UTC()
	start := expiry.Add(5 * time.Minute).UTC()
	accessPermission := azfile.AccessPolicyPermission{List: true}.String()
	permissions := make([]azfile.SignedIdentifier, 2, 2)
	for i := 0; i < 2; i++ {
		permissions[i] = azfile.SignedIdentifier{
			ID: id,
			AccessPolicy: &azfile.AccessPolicy{
				Start:      &start,
				Expiry:     &expiry,
				Permission: &accessPermission,
			},
		}
	}

	_, err := shareURL.SetPermissions(ctx, permissions)
	validateStorageError(c, err, azfile.ServiceCodeInvalidXMLDocument)
}

func (s *ShareURLSuite) TestShareGetSetMetadataDefault(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	sResp, err := share.SetMetadata(context.Background(), azfile.Metadata{})
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
	c.Assert(gResp.NewMetadata(), chk.HasLen, 0)
}

func (s *ShareURLSuite) TestShareGetSetMetadataNonDefault(c *chk.C) {
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

func (s *ShareURLSuite) TestShareSetMetadataNegative(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	md := azfile.Metadata{
		"1 foo": "FooValuE",
	}
	_, err := share.SetMetadata(context.Background(), md)
	c.Assert(err, chk.NotNil)
	c.Assert(strings.Contains(err.Error(), validationErrorSubstring), chk.Equals, true)
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
	c.Assert(gResp.ShareUsage, chk.Equals, int32(0))
}

func (s *ShareURLSuite) TestShareGetStatsNegative(c *chk.C) {
	fsu := getFSU()
	share, _ := getShareURL(c, fsu)

	_, err := share.GetStatistics(ctx)
	c.Assert(err, chk.NotNil)
	validateStorageError(c, err, azfile.ServiceCodeShareNotFound)
}

func (s *ShareURLSuite) TestShareCreateSnapshotNonDefault(c *chk.C) {
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

func (s *ShareURLSuite) TestShareCreateSnapshotDefault(c *chk.C) {
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

	_, err = shareURL.WithSnapshot(snapshotShare.Snapshot()).Delete(ctx, azfile.DeleteSnapshotsOptionNone)
	c.Assert(err, chk.IsNil)
}

func (s *ShareURLSuite) TestShareCreateSnapshotNegativeShareNotExist(c *chk.C) {
	fsu := getFSU()
	share, _ := getShareURL(c, fsu)

	_, err := share.CreateSnapshot(ctx, azfile.Metadata{})
	c.Assert(err, chk.NotNil)
	validateStorageError(c, err, azfile.ServiceCodeShareNotFound)
}

func (s *ShareURLSuite) TestShareCreateSnapshotNegativeMetadataInvalid(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	_, err := share.CreateSnapshot(ctx, azfile.Metadata{"Invalid Field!": "value"})
	c.Assert(err, chk.NotNil)
	c.Assert(strings.Contains(err.Error(), validationErrorSubstring), chk.Equals, true)
}

// Note behavior is different from blob's snapshot.
func (s *ShareURLSuite) TestShareCreateSnapshotNegativeSnapshotOfSnapshot(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionInclude)

	snapshotURL := share.WithSnapshot(time.Now().UTC().String())
	cResp, err := snapshotURL.CreateSnapshot(ctx, nil)
	c.Assert(err, chk.IsNil) //Note: this would not fail, snapshot would be ignored.

	snapshotRecursiveURL := share.WithSnapshot(cResp.Snapshot())
	_, err = snapshotRecursiveURL.CreateSnapshot(ctx, nil)
	c.Assert(err, chk.IsNil) //Note: this would not fail, snapshot would be ignored.
}

func validateShareDeleted(c *chk.C, shareURL azfile.ShareURL) {
	_, err := shareURL.GetProperties(ctx)
	validateStorageError(c, err, azfile.ServiceCodeShareNotFound)
}

func (s *ShareURLSuite) TestShareDeleteSnapshot(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	resp, err := share.CreateSnapshot(ctx, nil)
	c.Assert(err, chk.IsNil)
	snapshotURL := share.WithSnapshot(resp.Snapshot())

	_, err = snapshotURL.Delete(ctx, azfile.DeleteSnapshotsOptionNone)
	c.Assert(err, chk.IsNil)

	validateShareDeleted(c, snapshotURL)
}

func (s *ShareURLSuite) TestShareDeleteSnapshotsInclude(c *chk.C) {
	fsu := getFSU()
	share, shareName := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionNone)

	_, err := share.CreateSnapshot(ctx, nil)
	c.Assert(err, chk.IsNil)
	_, err = share.Delete(ctx, azfile.DeleteSnapshotsOptionInclude)
	c.Assert(err, chk.IsNil)

	lResp, _ := fsu.ListSharesSegment(ctx, azfile.Marker{}, azfile.ListSharesOptions{Detail: azfile.ListSharesDetail{Snapshots: true}, Prefix: shareName})
	c.Assert(lResp.Shares, chk.HasLen, 0)
}

func (s *ShareURLSuite) TestShareDeleteSnapshotsNoneWithSnapshots(c *chk.C) {
	fsu := getFSU()
	share, _ := createNewShare(c, fsu)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionInclude)

	_, err := share.CreateSnapshot(ctx, nil)
	c.Assert(err, chk.IsNil)
	_, err = share.Delete(ctx, azfile.DeleteSnapshotsOptionNone)
	validateStorageError(c, err, azfile.ServiceCodeShareHasSnapshots)
}
