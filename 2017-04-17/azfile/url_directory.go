package azfile

import (
	"context"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

// A DirectoryURL represents a URL to the Azure Storage directory allowing you to manipulate its directories and files.
type DirectoryURL struct {
	directoryClient directoryClient
}

// NewDirectoryURL creates a DirectoryURL object using the specified URL and request policy pipeline.
func NewDirectoryURL(url url.URL, p pipeline.Pipeline) DirectoryURL {
	if p == nil {
		panic("p can't be nil")
	}
	directoryClient := newDirectoryClient(url, p)
	return DirectoryURL{directoryClient: directoryClient}
}

// URL returns the URL endpoint used by the DirectoryURL object.
func (d DirectoryURL) URL() url.URL {
	return d.directoryClient.URL()
}

// String returns the URL as a string.
func (d DirectoryURL) String() string {
	u := d.URL()
	return u.String()
}

// WithPipeline creates a new DirectoryURL object identical to the source but with the specified request policy pipeline.
func (d DirectoryURL) WithPipeline(p pipeline.Pipeline) DirectoryURL {
	return NewDirectoryURL(d.URL(), p)
}

// NewFileURL creates a new FileURL object by concatenating fileName to the end of
// DirectoryURL's URL. The new FileURL uses the same request policy pipeline as the DirectoryURL.
// To change the pipeline, create the FileURL and then call its WithPipeline method passing in the
// desired pipeline object. Or, call this package's NewFileURL instead of calling this object's
// NewFileURL method.
func (d DirectoryURL) NewFileURL(fileName string) FileURL {
	fileURL := appendToURLPath(d.URL(), fileName)
	return NewFileURL(fileURL, d.directoryClient.Pipeline())
}

// NewDirectoryURL creates a new DirectoryURL object by concatenating directoryName to the end of
// DirectoryURL's URL. The new DirectoryURL uses the same request policy pipeline as the DirectoryURL.
// To change the pipeline, create the DirectoryURL and then call its WithPipeline method passing in the
// desired pipeline object. Or, call this package's NewDirectoryURL instead of calling this object's
// NewDirectoryURL method.
func (d DirectoryURL) NewDirectoryURL(directoryName string) DirectoryURL {
	directoryURL := appendToURLPath(d.URL(), directoryName)
	return NewDirectoryURL(directoryURL, d.directoryClient.Pipeline())
}

// Create creates a new directory within a storage account.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/create-directory.
func (d DirectoryURL) Create(ctx context.Context, metadata Metadata) (*DirectoryCreateResponse, error) {
	return d.directoryClient.Create(ctx, nil, metadata)
}

// Delete removes the specified empty directory. Note that the directory must be empty before it can be deleted..
// For more information, see https://docs.microsoft.com/rest/api/storageservices/delete-directory.
func (d DirectoryURL) Delete(ctx context.Context) (*DirectoryDeleteResponse, error) {
	return d.directoryClient.Delete(ctx, nil)
}

// GetPropertiesAndMetadata returns the directory's metadata and system properties.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/get-directory-properties.
func (d DirectoryURL) GetPropertiesAndMetadata(ctx context.Context) (*DirectoryGetPropertiesResponse, error) {
	return d.directoryClient.GetProperties(ctx, nil, nil)
}

// SetMetadata sets the directory's metadata.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/set-directory-metadata.
func (d DirectoryURL) SetMetadata(ctx context.Context, metadata Metadata) (*DirectorySetMetadataResponse, error) {
	return d.directoryClient.SetMetadata(ctx, nil, metadata)
}

// TODO: ListDirectoriesAndFiles
