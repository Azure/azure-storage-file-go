package azfile_test

import (
	"net/url"
	"time"

	"github.com/Azure/azure-storage-file-go/v10/azfile"
	chk "gopkg.in/check.v1"
)

type ParsingURLSuite struct{}

var _ = chk.Suite(&ParsingURLSuite{})

func (s *ParsingURLSuite) testFileURLPartsWithIPEndpointStyle(c *chk.C, urlStr string) *azfile.FileURLParts {
	u, err := url.Parse(urlStr)
	c.Assert(err, chk.IsNil)
	parts := azfile.NewFileURLParts(*u)
	pu := parts.URL()
	c.Assert(urlStr, chk.Equals, pu.String())

	return &parts
}

// Positive cases for parsing path with IPEndpointStyle
func (s *ParsingURLSuite) TestFileURLPartsWithIPEndpointStyle(c *chk.C) {
	p := s.testFileURLPartsWithIPEndpointStyle(c, "https://105.232.1.23:80/accountname")
	c.Assert(p.Host, chk.Equals, "105.232.1.23:80")

	p = s.testFileURLPartsWithIPEndpointStyle(c, "http://105.232.1.23/accountname")
	c.Assert(p.Host, chk.Equals, "105.232.1.23")
	c.Assert(p.Scheme, chk.Equals, "http")
	c.Assert(p.ShareName, chk.Equals, "")
	c.Assert(p.DirectoryOrFilePath, chk.Equals, "")

	p = s.testFileURLPartsWithIPEndpointStyle(c, "https://255.255.255.255/accountname/sharename")
	c.Assert(p.Host, chk.Equals, "255.255.255.255")
	c.Assert(p.Scheme, chk.Equals, "https")
	c.Assert(p.ShareName, chk.Equals, "sharename")
	c.Assert(p.DirectoryOrFilePath, chk.Equals, "")

	p = s.testFileURLPartsWithIPEndpointStyle(c, "https://255.255.255.255/accountname/sharename")
	c.Assert(p.Host, chk.Equals, "255.255.255.255")
	c.Assert(p.Scheme, chk.Equals, "https")
	c.Assert(p.ShareName, chk.Equals, "sharename")
	c.Assert(p.DirectoryOrFilePath, chk.Equals, "")

	p = s.testFileURLPartsWithIPEndpointStyle(c, "https://255.255.255.255:4392/accountname/sharename/directory/")
	c.Assert(p.Host, chk.Equals, "255.255.255.255:4392")
	c.Assert(p.ShareName, chk.Equals, "sharename")
	c.Assert(p.DirectoryOrFilePath, chk.Equals, "directory/")

	p = s.testFileURLPartsWithIPEndpointStyle(c, "https://255.255.255.255:4392/accountname/sharename/file")
	c.Assert(p.Host, chk.Equals, "255.255.255.255:4392")
	c.Assert(p.ShareName, chk.Equals, "sharename")
	c.Assert(p.DirectoryOrFilePath, chk.Equals, "file")

	p = s.testFileURLPartsWithIPEndpointStyle(c, "https://255.255.255.255:4392/accountname/sharename/directory/file")
	c.Assert(p.Host, chk.Equals, "255.255.255.255:4392")
	c.Assert(p.ShareName, chk.Equals, "sharename")
	c.Assert(p.DirectoryOrFilePath, chk.Equals, "directory/file")

	// IPv6 case.
	p = s.testFileURLPartsWithIPEndpointStyle(c, "https://[1080:0:0:0:8:800:200C:417A]:1234/accountname/sharename/directory/file")
	c.Assert(p.Host, chk.Equals, "[1080:0:0:0:8:800:200C:417A]:1234")
	c.Assert(p.ShareName, chk.Equals, "sharename")
	c.Assert(p.DirectoryOrFilePath, chk.Equals, "directory/file")
}

// Positive cases for composing URL with FilrURLParts
func (s *ParsingURLSuite) TestFileURLPartsComposing(c *chk.C) {
	p := azfile.FileURLParts{
		Scheme:              "http",
		Host:                "105.232.1.23:80",
		ShareName:           "sharename",
		DirectoryOrFilePath: "dir/",
		IPEndpointStyleInfo: azfile.IPEndpointStyleInfo{AccountName: "accountname"},
	}
	u := p.URL()
	c.Assert(u.String(), chk.Equals, "http://105.232.1.23:80/accountname/sharename/dir/")

	p = azfile.FileURLParts{
		Scheme:              "https",
		Host:                "105.232.1.23",
		ShareName:           "sharename",
		IPEndpointStyleInfo: azfile.IPEndpointStyleInfo{AccountName: "accountname"},
	}
	u = p.URL()
	c.Assert(u.String(), chk.Equals, "https://105.232.1.23/accountname/sharename")

	p = azfile.FileURLParts{
		Scheme:              "https",
		Host:                "[1080:0:0:0:8:800:200C:417A]",
		ShareName:           "sharename",
		IPEndpointStyleInfo: azfile.IPEndpointStyleInfo{AccountName: "accountname"},
	}
	u = p.URL()
	c.Assert(u.String(), chk.Equals, "https://[1080:0:0:0:8:800:200C:417A]/accountname/sharename")

	p = azfile.FileURLParts{
		Scheme:              "https",
		Host:                "accountName.blob.core.windows.net",
		ShareName:           "sharename",
		IPEndpointStyleInfo: azfile.IPEndpointStyleInfo{AccountName: "fakeaccount"},
	}
	u = p.URL()
	c.Assert(u.String(), chk.Equals, "https://accountName.blob.core.windows.net/sharename")
}

// Positive cases for parsing path with domain hostname.
func (s *ParsingURLSuite) TestFileURLPartsWithDomainHostname(c *chk.C) {
	p := s.testFileURLPartsWithIPEndpointStyle(c, "https://accountName.blob.core.windows.net")
	c.Assert(p.Host, chk.Equals, "accountName.blob.core.windows.net")
	c.Assert(p.ShareName, chk.Equals, "")
	c.Assert(p.DirectoryOrFilePath, chk.Equals, "")

	p = s.testFileURLPartsWithIPEndpointStyle(c, "http://accountName.blob.core.windows.net/sharename")
	c.Assert(p.Host, chk.Equals, "accountName.blob.core.windows.net")
	c.Assert(p.Scheme, chk.Equals, "http")
	c.Assert(p.ShareName, chk.Equals, "sharename")
	c.Assert(p.DirectoryOrFilePath, chk.Equals, "")

	p = s.testFileURLPartsWithIPEndpointStyle(c, "https://accountName.blob.core.windows.net/sharename/directory/")
	c.Assert(p.Host, chk.Equals, "accountName.blob.core.windows.net")
	c.Assert(p.Scheme, chk.Equals, "https")
	c.Assert(p.ShareName, chk.Equals, "sharename")
	c.Assert(p.DirectoryOrFilePath, chk.Equals, "directory/")

	p = s.testFileURLPartsWithIPEndpointStyle(c, "http://accountName.blob.core.windows.net/sharename/file")
	c.Assert(p.Host, chk.Equals, "accountName.blob.core.windows.net")
	c.Assert(p.ShareName, chk.Equals, "sharename")
	c.Assert(p.DirectoryOrFilePath, chk.Equals, "file")

	p = s.testFileURLPartsWithIPEndpointStyle(c, "http://accountName.blob.core.windows.net/sharename/directory/file.txt")
	c.Assert(p.Host, chk.Equals, "accountName.blob.core.windows.net")
	c.Assert(p.ShareName, chk.Equals, "sharename")
	c.Assert(p.DirectoryOrFilePath, chk.Equals, "directory/file.txt")

	p = s.testFileURLPartsWithIPEndpointStyle(c, "http://accountName.blob.core.windows.net/sharename/directory/d2/d3/d4/")
	c.Assert(p.Host, chk.Equals, "accountName.blob.core.windows.net")
	c.Assert(p.ShareName, chk.Equals, "sharename")
	c.Assert(p.DirectoryOrFilePath, chk.Equals, "directory/d2/d3/d4/")
}

// Negative cases for parsing path with IPEndpointStyle
func (s *ParsingURLSuite) TestFileURLPartsWithIPEndpointStyleNegative(c *chk.C) {
	// invalid IP, should fallback to non-IP endpoint parsing, where accoutname will be regarded as share name.
	p := s.testFileURLPartsWithIPEndpointStyle(c, "https://12303.232.1.23:80/accountname")
	c.Assert(p.ShareName, chk.Equals, "accountname")
}

// Parsing endpoint with snapshot and SAS
func (s *ParsingURLSuite) TestFileURLPartsWithSnapshotAndSAS(c *chk.C) {
	fsu := getFSU()
	shareURL, shareName := getShareURL(c, fsu)
	fileURL, fileName := getFileURLFromShare(c, shareURL)

	currentTime := time.Now().UTC()
	credential, accountName := getCredential()
	sasQueryParams, err := azfile.AccountSASSignatureValues{
		Protocol:      azfile.SASProtocolHTTPS,
		ExpiryTime:    currentTime.Add(48 * time.Hour),
		Permissions:   azfile.AccountSASPermissions{Read: true, List: true}.String(),
		Services:      azfile.AccountSASServices{File: true}.String(),
		ResourceTypes: azfile.AccountSASResourceTypes{Container: true, Object: true}.String(),
	}.NewSASQueryParameters(credential)
	c.Assert(err, chk.IsNil)

	parts := azfile.NewFileURLParts(fileURL.URL())
	parts.SAS = sasQueryParams
	parts.ShareSnapshot = currentTime.Format("2006-01-02T15:04:05.0000000Z07:00")
	testURL := parts.URL()

	// The snapshot format string is taken from the snapshotTimeFormat value in parsing_urls.go. The field is not public, so
	// it is copied here
	correctURL := "https://" + accountName + ".file.core.windows.net/" + shareName + "/" + fileName +
		"?" + "sharesnapshot=" + currentTime.Format("2006-01-02T15:04:05.0000000Z07:00") + "&" + sasQueryParams.Encode()
	c.Assert(testURL.String(), chk.Equals, correctURL)
}
