package azfile_test

import (
	"context"

	"github.com/Azure/azure-storage-file-go/2017-04-17/azfile"
	chk "gopkg.in/check.v1"
)

type StorageAccountSuite struct{}

var _ = chk.Suite(&StorageAccountSuite{})

func (s *StorageAccountSuite) TestAccountGetSetProperties(c *chk.C) {
	sa := getFSU()
	setProps := azfile.StorageServiceProperties{}
	resp, err := sa.SetProperties(context.Background(), setProps)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Response().StatusCode, chk.Equals, 202)
	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(resp.Version(), chk.Not(chk.Equals), "")

	props, err := sa.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(props.Response().StatusCode, chk.Equals, 200)
	c.Assert(props.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(props.Version(), chk.Not(chk.Equals), "")
	c.Assert(props.HourMetrics, chk.NotNil)
	c.Assert(props.MinuteMetrics, chk.NotNil)
	c.Assert(props.Cors, chk.HasLen, 0)
}

func (s *StorageAccountSuite) TestAccountListShares(c *chk.C) {
	sa := getFSU()
	ctx := context.Background()
	resp, err := sa.ListShares(ctx, azfile.Marker{}, azfile.ListSharesOptions{Prefix: sharePrefix})
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Response().StatusCode, chk.Equals, 200)
	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(resp.Version(), chk.Not(chk.Equals), "")
	c.Assert(resp.ServiceEndpoint, chk.NotNil)
	c.Assert(*(resp.Prefix), chk.Equals, sharePrefix)

	share, shareName := createNewShare(c, sa)
	defer delShare(c, share, azfile.DeleteSnapshotsOptionInclude)

	shareMetadata := azfile.Metadata{
		"foo": "foovalue",
		"bar": "barvalue",
	}

	_, err = share.SetMetadata(ctx, shareMetadata)
	c.Assert(err, chk.IsNil)

	_, err = share.CreateSnapshot(ctx, nil)
	c.Assert(err, chk.IsNil)

	resp, err = sa.ListShares(ctx, azfile.Marker{}, azfile.ListSharesOptions{Detail: azfile.ListSharesDetail{Metadata: true, Snapshots: true}, Prefix: shareName})
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Shares, chk.HasLen, 2)
	c.Assert(resp.Shares[0].Name, chk.NotNil)
	c.Assert(resp.Shares[0].Properties, chk.NotNil)
	c.Assert(resp.Shares[0].Properties.LastModified, chk.NotNil)
	c.Assert(resp.Shares[0].Properties.Etag, chk.NotNil)
	c.Assert(resp.Shares[0].Properties.Quota, chk.Not(chk.Equals), 0)
	c.Assert(resp.Shares[0].Metadata, chk.DeepEquals, shareMetadata)

	if resp.Shares[0].Snapshot == nil {
		c.Assert(resp.Shares[1].Snapshot, chk.NotNil)
	}
}

func (s *StorageAccountSuite) TestAccountListSharesPaged(c *chk.C) {
	sa := getFSU()

	const numShares = 4
	const maxResultsPerPage = 2
	const pagedSharesPrefix = sharePrefix + "azfilesharepagedtest"

	shares := make([]azfile.ShareURL, numShares)
	for i := 0; i < numShares; i++ {
		shares[i], _ = createNewShareWithPrefix(c, sa, pagedSharesPrefix)
	}

	defer func() {
		for i := range shares {
			delShare(c, shares[i], azfile.DeleteSnapshotsOptionNone)
		}
	}()

	marker := azfile.Marker{}
	iterations := numShares / maxResultsPerPage

	for i := 0; i < iterations; i++ {
		resp, err := sa.ListShares(context.Background(), marker, azfile.ListSharesOptions{MaxResults: maxResultsPerPage, Prefix: pagedSharesPrefix})
		c.Assert(err, chk.IsNil)
		c.Assert(resp.Shares, chk.HasLen, maxResultsPerPage)

		hasMore := i < iterations-1
		c.Assert(resp.NextMarker.NotDone(), chk.Equals, hasMore)
		marker = resp.NextMarker
	}
}
