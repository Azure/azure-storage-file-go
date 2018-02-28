package azfile

import (
	"net/url"
	"strings"
	"time"
)

const (
	snapshotTimeFormat = "2006-01-02T15:04:05.0000000Z07:00"
	shareSnapshot      = "sharesnapshot"
)

// A FileURLParts object represents the components that make up an Azure Storage Share/Directory/File URL. You parse an
// existing URL into its parts by calling NewFileURLParts(). You construct a URL from parts by calling URL().
// NOTE: Changing any SAS-related field requires computing a new SAS signature.
type FileURLParts struct {
	Scheme         string    // Ex: "https://"
	Host           string    // Ex: "account.share.core.windows.net"
	ShareName      string    // Share name, Ex: "myshare"
	Path           string    // Path of directory or file, Ex: "mydirectory/myfile"
	ShareSnapshot  time.Time // IsZero is true if not a snapshot
	SAS            SASQueryParameters
	UnparsedParams string
}

// NewFileURLParts parses a URL initializing FileURLParts' fields including any SAS-related & sharesnapshot query parameters. Any other
// query parameters remain in the UnparsedParams field. This method overwrites all fields in the FileURLParts object.
func NewFileURLParts(u url.URL) FileURLParts {
	up := FileURLParts{
		Scheme: u.Scheme,
		Host:   u.Host,
	}

	if u.Path != "" {
		path := u.Path

		if path[0] == '/' {
			path = path[1:]
		}

		// Find the next slash (if it exists)
		shareEndIndex := strings.Index(path, "/")
		if shareEndIndex == -1 { // Slash not found; path has share name & no path of directory or file
			up.ShareName = path
		} else { // Slash found; path has share name & path of directory or file
			up.ShareName = path[:shareEndIndex]
			up.Path = path[shareEndIndex+1:]
		}
	}

	// Convert the query parameters to a case-sensitive map & trim whitespace
	paramsMap := u.Query()

	up.ShareSnapshot = time.Time{} // Assume no snapshot
	if snapshotStr, ok := caseInsensitiveValues(paramsMap).Get(shareSnapshot); ok {
		up.ShareSnapshot, _ = time.Parse(snapshotTimeFormat, snapshotStr[0])
		// If we recognized the query parameter, remove it from the map
		delete(paramsMap, shareSnapshot)
	}
	up.SAS = NewSASQueryParameters(paramsMap, true)
	up.UnparsedParams = paramsMap.Encode()
	return up
}

type caseInsensitiveValues url.Values // map[string][]string
func (values caseInsensitiveValues) Get(key string) ([]string, bool) {
	key = strings.ToLower(key)
	for k, v := range values {
		if strings.ToLower(k) == key {
			return v, true
		}
	}
	return []string{}, false
}

// URL returns a URL object whose fields are initialized from the FileURLParts fields. The URL's RawQuery
// field contains the SAS, snapshot, and unparsed query parameters.
func (up FileURLParts) URL() url.URL {
	path := ""
	// Concatenate share & path of directory or file (if they exist)
	if up.ShareName != "" {
		path += "/" + up.ShareName
		if up.Path != "" {
			path += "/" + up.Path
		}
	}

	rawQuery := up.UnparsedParams

	// Concatenate share snapshot query parameter (if it exists)
	if !up.ShareSnapshot.IsZero() {
		if len(rawQuery) > 0 {
			rawQuery += "&"
		}
		rawQuery += shareSnapshot + "=" + up.ShareSnapshot.Format(snapshotTimeFormat)
	}
	sas := up.SAS.Encode()
	if sas != "" {
		if len(rawQuery) > 0 {
			rawQuery += "&"
		}
		rawQuery += sas
	}
	u := url.URL{
		Scheme:   up.Scheme,
		Host:     up.Host,
		Path:     path,
		RawQuery: rawQuery,
	}
	return u
}
