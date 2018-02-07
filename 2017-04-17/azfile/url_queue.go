package azfile

import (
	"bytes"
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-storage-go-proto/2016-05-31/pipeline"
)
/*
https://docs.microsoft.com/en-us/rest/api/storageservices/operations-on-shares--file-service-
Get Share Properties
Set Share Properties
Get Share Stats

https://docs.microsoft.com/en-us/rest/api/storageservices/operations-on-directories
List Directories and Files
Create Directory
Get Directory Properties
Delete Directory
Get Directory Metadata
Set Directory Metadata

https://docs.microsoft.com/en-us/rest/api/storageservices/operations-on-files
Create File
Get File
Get File Properties
Set File Properties
Put Range
List Ranges
Get File Metadata
Set File Metadata
Delete File
Copy File
Abort Copy File
*/

const (
	// QueueMaxGetMessages indicates the maximum number of messages you can retrieve
	// with each call to GetMessages (32).
	QueueMaxGetMessages = 32

	// QueueMessageMaxBytes indicates the maximum number of bytes allowed for a message's text.
	QueueMessageMaxBytes = 64 * 1024 // 64KB
)

// A ShareURL represents a URL to the Azure Storage queue allowing you to work with messages.
type ShareURL struct {
	client queueClient
}

// NewShareURL creates a ShareURL object using the specified URL and request policy pipeline.
func NewShareURL(url url.URL, p pipeline.Pipeline) QueueURL {
	if p == nil {
		panic("p can't be nil")
	}
	client := shareClient{} //"newQueueClient(url, p)"
	return ShareURL{client: client}
}

// URL returns the URL endpoint used by the QueueURL object.
func (s ShareURL) URL() url.URL {
	return q.client.URL()
}

// String returns the URL as a string.
func (s ShareURL) String() string {
	u := s.URL()
	return u.String()
}

// WithPipeline creates a new QueueURL object identical to the source but with the specified request policy pipeline.
func (s ShareURL) WithPipeline(p pipeline.Pipeline) ShareURL {
	return NewShareURL(s.URL(), p)
}

// Create creates a new container within a storage account. If a container with the same name already exists, the operation fails.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/create-container.
func (s ShareURL) Create(ctx context.Context, metadata Metadata) (*ShareCreateResponse, error) {
	return nil, nil // q.client.Create(ctx, nil, metadata, nil)
}

// Delete marks the specified queue for deletion. The queue and any messages contained within it are later deleted during garbage collection.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/delete-container.
func (s ShareURL) Delete(ctx context.Context) (*ShareDeleteResponse, error) {
	return nil, nil // q.client.Delete(ctx, nil, nil, nil)
}

// GetMetadata returns the container's metadata and system properties.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/get-container-metadata.
func (s ShareURL) GetMetadata(ctx context.Context) (*ShareGetPropertiesResponse, error) {
	// NOTE: GetMetadata actually calls GetProperties internally because GetProperties returns the metadata AND the properties.
	// This allows us to not expose a GetProperties method at all simplifying the API.
	return nil, nil // c.client.GetProperties(ctx, nil, ac.pointers(), nil)
}

// SetMetadata sets the container's metadata.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/set-container-metadata.
func (s ShareURL) SetMetadata(ctx context.Context, metadata Metadata) (*ShareSetMetadataResponse, error) {
	return nil, nil // q.client.SetMetadata(ctx, nil, metadata, ifModifiedSince, nil)
}

// GetPermissions returns the container's permissions. The permissions indicate whether container's blobs may be accessed publicly.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/get-container-acl.
func (s ShareURL) GetPermissions(ctx context.Context) (*SignedIdentifiers, error) {
	return nil, nil // q.client.GetACL(ctx, nil, ac.pointers(), nil)
}

// The AccessPolicyPermission type simplifies creating the permissions string for a container's access policy.
// Initialize an instance of this type and then call its String method to set AccessPolicy's Permission field.
type AccessPolicyPermission struct {
	Read, Add, Create, Write, Delete, List bool
}

// String produces the access policy permission string for an Azure Storage container.
// Call this method to set AccessPolicy's Permission field.
func (p AccessPolicyPermission) String() string {
	var b bytes.Buffer
	if p.Read {
		b.WriteRune('r')
	}
	if p.Add {
		b.WriteRune('a')
	}
	if p.Create {
		b.WriteRune('c')
	}
	if p.Write {
		b.WriteRune('w')
	}
	if p.Delete {
		b.WriteRune('d')
	}
	if p.List {
		b.WriteRune('l')
	}
	return b.String()
}

// Parse initializes the AccessPolicyPermission's fields from a string.
func (p *AccessPolicyPermission) Parse(s string) {
	p.Read = strings.ContainsRune(s, 'r')
	p.Add = strings.ContainsRune(s, 'a')
	p.Create = strings.ContainsRune(s, 'c')
	p.Write = strings.ContainsRune(s, 'w')
	p.Delete = strings.ContainsRune(s, 'd')
	p.List = strings.ContainsRune(s, 'l')
}

// SetPermissions sets the container's permissions. The permissions indicate whether blobs in a container may be accessed publicly.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/set-container-acl.
func (s ShareURL) SetPermissions(ctx context.Context, permissions []SignedIdentifier) (*ShareSetACLResponse, error) {
	return nil, nil // q.client.SetACL(ctx, permissions, nil, nil, nil)
}

func (q QueueURL) Clear(ctx context.Context) (*QueueClearResponse, error) {
	return nil, nil
}

func (q QueueURL) PutMessage(ctx context.Context, timeToLiveSeconds int, messageText string) (*QueuePutMessageResponse, error) {
	return nil, nil
}

func (q QueueURL) GetMessages(ctx context.Context, maxMessages int, visibilityTimeout time.Duration) (*QueueGetMessagesResponse, error) {
	return nil, nil
}

func (q QueueURL) PeekMessages(ctx context.Context, maxMessages int) (*QueuePeekMessagesResponse, error) {
	return nil, nil
}

func (q QueueURL) DeleteMessage(ctx context.Context, pr PopReceipt) (*QueueDeleteMessageResponse, error) {
	return nil, nil
}
func (q QueueURL) UpdateMessage(ctx context.Context, pr PopReceipt, visibilityTimeout time.Duration) (*QueueUpdateMessage	Response, error) {
	return nil, nil
}
