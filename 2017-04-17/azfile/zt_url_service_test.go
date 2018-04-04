package azfile_test

import (
	"context"
	"errors"
	"net/url"
	"os"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-file-go/2017-04-17/azfile"
	chk "gopkg.in/check.v1"
)

type StorageAccountSuite struct{}

var _ = chk.Suite(&StorageAccountSuite{})

func (s *StorageAccountSuite) TestAccountNewShareURLValidName(c *chk.C) {
	fsu := getFSU()
	testURL := fsu.NewShareURL(sharePrefix)

	correctURL := "https://" + os.Getenv("ACCOUNT_NAME") + ".file.core.windows.net/" + sharePrefix
	temp := testURL.URL()
	c.Assert(temp.String(), chk.Equals, correctURL)
	c.Assert(testURL.String(), chk.Equals, correctURL)
}

func (s *StorageAccountSuite) TestAccountNewServiceURLValidName(c *chk.C) {
	fsu := getFSU()

	correctURL := "https://" + os.Getenv("ACCOUNT_NAME") + ".file.core.windows.net/"
	c.Assert(fsu.String(), chk.Equals, correctURL)
}

func (s *StorageAccountSuite) TestAccountNewServiceURLNegative(c *chk.C) {
	c.Assert(func() { azfile.NewServiceURL(url.URL{}, nil) }, chk.Panics, "p can't be nil")
}

type testPipeline struct{}

const testPipelineMessage string = "Test factory invoked"

func (tm testPipeline) Do(ctx context.Context, methodFactory pipeline.Factory, request pipeline.Request) (pipeline.Response, error) {
	return nil, errors.New(testPipelineMessage)
}

func (s *StorageAccountSuite) TestAccountWithPipeline(c *chk.C) {
	fsu := getFSU()
	fsu = fsu.WithPipeline(testPipeline{}) // testPipeline returns an identifying message as an error
	shareURL := fsu.NewShareURL("name")

	_, err := shareURL.Create(ctx, azfile.Metadata{}, 0)

	c.Assert(err.Error(), chk.Equals, testPipelineMessage)
}

// This case is not stable, as service side returns 202, if it previously has value,
// it need unpredictable time to make updates take effect.
func (s *StorageAccountSuite) TestAccountGetSetPropertiesDefault(c *chk.C) {
	sa := getFSU()
	setProps := azfile.FileServiceProperties{}
	resp, err := sa.SetProperties(context.Background(), setProps)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Response().StatusCode, chk.Equals, 202)
	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(resp.Version(), chk.Not(chk.Equals), "")

	time.Sleep(time.Second * 15)

	// Note: service side is 202, might depend on timing
	props, err := sa.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(props.Response().StatusCode, chk.Equals, 200)
	c.Assert(props.RequestID(), chk.Not(chk.Equals), "")
	c.Assert(props.Version(), chk.Not(chk.Equals), "")
	c.Assert(props.HourMetrics, chk.NotNil)
	c.Assert(props.MinuteMetrics, chk.NotNil)
	//c.Assert(props.Cors, chk.HasLen, 0) //Unstable evaluation
}

func (s *StorageAccountSuite) TestAccountGetSetPropertiesNonDefaultWithEnable(c *chk.C) {
	sa := getFSU()

	setProps := azfile.FileServiceProperties{
		HourMetrics: azfile.MetricProperties{
			MetricEnabled:          true,
			IncludeAPIs:            true,
			RetentionPolicyEnabled: true,
			RetentionDays:          1,
		},
		MinuteMetrics: azfile.MetricProperties{
			MetricEnabled:          true,
			IncludeAPIs:            false,
			RetentionPolicyEnabled: true,
			RetentionDays:          2,
		},
		Cors: []azfile.CorsRule{
			azfile.CorsRule{
				AllowedOrigins:  "*",
				AllowedMethods:  "PUT",
				AllowedHeaders:  "x-ms-client-request-id",
				ExposedHeaders:  "x-ms-*",
				MaxAgeInSeconds: 2,
			},
		},
	}
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
	c.Assert(props.HourMetrics, chk.DeepEquals, azfile.MetricProperties{
		MetricEnabled:          true,
		IncludeAPIs:            true,
		RetentionPolicyEnabled: true,
		RetentionDays:          1,
	})
	c.Assert(props.MinuteMetrics, chk.DeepEquals, azfile.MetricProperties{
		MetricEnabled:          true,
		IncludeAPIs:            false,
		RetentionPolicyEnabled: true,
		RetentionDays:          2,
	})
	c.Assert(props.Cors, chk.DeepEquals, []azfile.CorsRule{
		azfile.CorsRule{
			AllowedOrigins:  "*",
			AllowedMethods:  "PUT",
			AllowedHeaders:  "x-ms-client-request-id",
			ExposedHeaders:  "x-ms-*",
			MaxAgeInSeconds: 2,
		},
	})
}

// TODO: This case is not stable... As SetProperties returns 202 Accepted, it depends on server side how fast properties would be set.
// func (s *StorageAccountSuite) TestAccountGetSetPropertiesNonDefaultWithDisable(c *chk.C) {
// 	sa := getFSU()

// 	setProps := azfile.FileServiceProperties{
// 		HourMetrics: azfile.MetricProperties{
// 			MetricEnabled: false,
// 		},
// 		MinuteMetrics: azfile.MetricProperties{
// 			MetricEnabled: false,
// 		},
// 	}
// 	resp, err := sa.SetProperties(context.Background(), setProps)
// 	c.Assert(err, chk.IsNil)
// 	c.Assert(resp.Response().StatusCode, chk.Equals, 202)
// 	c.Assert(resp.RequestID(), chk.Not(chk.Equals), "")
// 	c.Assert(resp.Version(), chk.Not(chk.Equals), "")

// 	time.Sleep(time.Second * 5)

// 	props, err := sa.GetProperties(context.Background())
// 	c.Assert(err, chk.IsNil)
// 	c.Assert(props.Response().StatusCode, chk.Equals, 200)
// 	c.Assert(props.RequestID(), chk.Not(chk.Equals), "")
// 	c.Assert(props.Version(), chk.Not(chk.Equals), "")
// 	c.Assert(props.HourMetrics, chk.DeepEquals, azfile.MetricProperties{MetricEnabled: false})
// 	c.Assert(props.MinuteMetrics, chk.DeepEquals, azfile.MetricProperties{MetricEnabled: false})
// 	c.Assert(props.Cors, chk.IsNil)
// }

func (s *StorageAccountSuite) TestAccountListSharesDefault(c *chk.C) {
	fsu := getFSU()
	shareURL1, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL1, azfile.DeleteSnapshotsOptionNone)
	shareURL2, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL2, azfile.DeleteSnapshotsOptionNone)

	response, err := fsu.ListSharesSegment(ctx, azfile.Marker{}, azfile.ListSharesOptions{})

	c.Assert(err, chk.IsNil)
	c.Assert(len(response.Shares) >= 2, chk.Equals, true) // The response should contain at least the two created containers. Probably many more
}

func (s *StorageAccountSuite) TestAccountListSharesNonDefault(c *chk.C) {
	sa := getFSU()
	ctx := context.Background()
	resp, err := sa.ListSharesSegment(ctx, azfile.Marker{}, azfile.ListSharesOptions{Prefix: sharePrefix})
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

	resp, err = sa.ListSharesSegment(ctx, azfile.Marker{}, azfile.ListSharesOptions{Detail: azfile.ListSharesDetail{Metadata: true, Snapshots: true}, Prefix: shareName})
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

func (s *StorageAccountSuite) TestAccountListSharesMaxResultsZero(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)

	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)

	// Max Results = 0 means the value will be ignored, the header not set, and the server default used
	resp, err := fsu.ListSharesSegment(ctx,
		azfile.Marker{}, azfile.ListSharesOptions{Prefix: sharePrefix, MaxResults: 0})

	c.Assert(err, chk.IsNil)
	c.Assert(len(resp.Shares) >= 1, chk.Equals, true) // At least 1 share.
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
		resp, err := sa.ListSharesSegment(context.Background(), marker, azfile.ListSharesOptions{MaxResults: maxResultsPerPage, Prefix: pagedSharesPrefix})
		c.Assert(err, chk.IsNil)
		c.Assert(resp.Shares, chk.HasLen, maxResultsPerPage)

		hasMore := i < iterations-1
		c.Assert(resp.NextMarker.NotDone(), chk.Equals, hasMore)
		marker = resp.NextMarker
	}
}

func (s *StorageAccountSuite) TestAccountListSharesNegativeMaxResults(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)

	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionNone)
	// The library should panic if MaxResults < -1
	defer func() {
		recover()
	}()

	fsu.ListSharesSegment(ctx,
		azfile.Marker{}, *(&azfile.ListSharesOptions{Prefix: sharePrefix, MaxResults: -2}))

	c.Fail() // If the list call doesn't panic, we fail
}
