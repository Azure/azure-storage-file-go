package azfile_test

import (
	"strings"

	chk "gopkg.in/check.v1"

	"github.com/Azure/azure-storage-file-go/azfile"
)

type FileAttributeFlagsSuite struct{
	// attributeTestList defines a list containing tests that can be used against both parsing and stringifying.
	attributeTestList map[azfile.FileAttributeFlags]string
	// parseOnlyList defines tests that should apply only to parsing,
	// because order shouldn't matter in parsing, but it does in stringifying.
	parseOnlyList map[azfile.FileAttributeFlags]string
}

var _ = chk.Suite(&FileAttributeFlagsSuite{
	attributeTestList: map[azfile.FileAttributeFlags]string{
		azfile.FileAttributeNone: "None",
		// Try handling multiple
		azfile.FileAttributeReadonly.Add(
			azfile.FileAttributeHidden): "ReadOnly|Hidden",

		// Ensure that all attribs work
		azfile.FileAttributeReadonly.Add(
		azfile.FileAttributeHidden.Add(
		azfile.FileAttributeSystem.Add(
		azfile.FileAttributeArchive.Add(
		azfile.FileAttributeTemporary.Add(
		azfile.FileAttributeOffline.Add(
		azfile.FileAttributeNotContentIndexed.Add(
		azfile.FileAttributeNoScrubData))))))):"ReadOnly|Hidden|System|Archive|Temporary|Offline|NotContentIndexed|NoScrubData",
	},
	parseOnlyList: map[azfile.FileAttributeFlags]string{
		// Handle multiple but out of order.
		azfile.FileAttributeOffline.Add(
			azfile.FileAttributeArchive).
			Add(azfile.FileAttributeNoScrubData): "NoScrubData|Offline|Archive",
	},
})

func (s *FileAttributeFlagsSuite) appendMaps(a, b map[azfile.FileAttributeFlags]string) map[azfile.FileAttributeFlags]string {
	out := map[azfile.FileAttributeFlags]string{}

	for k,v := range a {
		out[k] = v
	}

	for k,v := range b {
		out[k] = v
	}

	return out
}

func (s *FileAttributeFlagsSuite) TestOperations(c *chk.C) {
	// start blank
	attribs := azfile.FileAttributeNone

	// attempt an add operation and confirm it works.
	expected := azfile.FileAttributeReadonly
	attribs = attribs.Add(azfile.FileAttributeReadonly)

	c.Assert(attribs, chk.Equals, expected)

	// Try adding more than one. This _should_ be readonly, hidden, and system
	expected = azfile.FileAttributeFlags(0b111)
	attribs = attribs.Add(azfile.FileAttributeHidden).Add(azfile.FileAttributeSystem)

	c.Assert(attribs, chk.Equals, expected)

	// Try removing the hidden attribute.
	expected = azfile.FileAttributeFlags(0b101)
	attribs = attribs.Remove(azfile.FileAttributeHidden)

	c.Assert(attribs, chk.Equals, expected)

	// Try removing it again, and make sure it's still not there.
	attribs = attribs.Remove(azfile.FileAttributeHidden)
	c.Assert(attribs, chk.Equals, expected)

	// Try checking that we have the system attribute.
	c.Assert(attribs.Has(azfile.FileAttributeSystem), chk.Equals, true)

	// Try checking a superset of the attributes. Should be false.
	c.Assert(attribs.Has(azfile.FileAttributeSystem.Add(azfile.FileAttributeArchive)), chk.Equals, false)

	// Add a few more attributes, don't bother checking since it's a known good operation at this point.
	attribs = attribs.Add(azfile.FileAttributeHidden).Add(azfile.FileAttributeOffline)

	// Try checking for a larger subset.
	c.Assert(attribs.Has(azfile.FileAttributeSystem.Add(azfile.FileAttributeHidden)), chk.Equals, true)

	// Try removing multiple attributes via a pre-emptive add to the removed attribute.
	expected = azfile.FileAttributeFlags(0b101)
	attribs = attribs.Remove(azfile.FileAttributeHidden.Add(azfile.FileAttributeOffline))
	c.Assert(attribs, chk.Equals, expected)
}

func (s *FileAttributeFlagsSuite) TestParseFileAttributeFlags(c *chk.C) {
	for k,v := range s.appendMaps(s.attributeTestList, s.parseOnlyList) {
		flags := azfile.ParseFileAttributeFlagsString(v)

		c.Assert(flags, chk.Equals, k)
	}
}

func (s *FileAttributeFlagsSuite) TestStringifyFileAttributeFlags(c *chk.C) {
	for k,v := range s.attributeTestList {
		flags := k.String()

		c.Assert(flags, chk.Equals, v)
	}
}

func (s *FileAttributeFlagsSuite) TestRoundTrippedFlags(c *chk.C) {
	fsu := getFSU()
	shareURL, _ := createNewShare(c, fsu)
	defer delShare(c, shareURL, azfile.DeleteSnapshotsOptionInclude)
	fileURL, _ := createNewFileFromShare(c, shareURL, 1)

	fileAttribs := azfile.FileAttributeHidden.Add(azfile.FileAttributeReadonly)

	// We don't need to worry about getting the headers back because the new values are returned in here.
	setResp, err := fileURL.SetHTTPHeaders(
		ctx,
		azfile.FileHTTPHeaders{
			SMBProperties:azfile.SMBProperties{
				FileAttributes: &fileAttribs,
			},
		},
	)

	c.Assert(err, chk.IsNil)
	c.Assert(setResp.StatusCode(), chk.Equals, 200)
	// Ensure the response parses how we expect.
	c.Assert(azfile.ParseFileAttributeFlagsString(setResp.FileAttributes()), chk.Equals, fileAttribs)
	// Ensure that the response (once we clean spaces) equals our returned string.
	c.Assert(strings.ReplaceAll(setResp.FileAttributes(), " ", ""), chk.Equals, fileAttribs.String())
}