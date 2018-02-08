package azfile

import (
	"net/url"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

// Metadata contains metadata key/value pairs.
type Metadata map[string]string
type SignedIdentifier struct{}

type QueueCreateResponse struct{}
type QueueDeleteResponse struct{}
type QueueGetPropertiesResponse struct{}
type QueueSetMetadataResponse struct{}
type QueueSetACLResponse struct{}
type QueueClearResponse struct{}
type QueuePutMessageResponse struct{}
type QueueGetMessagesResponse struct{}
type QueuePeekMessagesResponse struct{}
type QueueDeleteMessageResponse struct{}
type QueueUpdateMessageResponse struct{}

type SignedIdentifiers struct{}

type ListQueuesResponse struct{}
type Marker struct{}
type ListQueueDetail struct{}

type serviceClient struct {
	client string
}

func (serviceClient) URL() url.URL {
	return url.URL{}
}
func (serviceClient) Pipeline() pipeline.Pipeline {
	return nil
}

type queueClient struct {
	client string
}

func (queueClient) URL() url.URL {
	return url.URL{}
}

// ListQueuesIncludeType enumerates the values for list queues include type.
type ListQueuesIncludeType string

const (
	// ListContainersIncludeMetadata ...
	ListQueuesIncludeMetadata ListQueuesIncludeType = "metadata"
	// ListContainersIncludeNone represents an empty ListContainersIncludeType.
	ListQueuesIncludeNone ListQueuesIncludeType = ""
)

type PutMessage struct { // Returned from PutMessage
	id              string //TODO: GUID??
	insertionTime   time.Time
	expirationTime  time.Time
	popReceipt      PopReceipt
	nextVisibleTime time.Time
}

func (m *PutMessage) ID() string                 { return m.ID }
func (m *PutMessage) InsertionTime() time.Time   { return m.insertionTime }
func (m *PutMessage) ExpirationTime() time.Time  { return m.expirationTime }
func (m *PutMessage) PopReceipt() PopReceipt     { return m.popReceipt }
func (m *PutMessage) NextVisibleTime() time.Time { return m.nextVisibleTime }

type PeekedMessage struct { // Returned from PeekMessage
	id             string //TODO: GUID??
	insertionTime  time.Time
	expirationTime time.Time
	dequeueCount   int32
	text           string // TODO: []byte after base-64 decoding?
}

func (m *PeekedMessage) ID() string                { return m.ID }
func (m *PeekedMessage) InsertionTime() time.Time  { return m.insertionTime }
func (m *PeekedMessage) ExpirationTime() time.Time { return m.expirationTime }
func (m *PeekedMessage) DequeueCount() int         { return m.dequeueCount }
func (m *PeekedMessage) Text() string              { return m.text }

type DequeuedMessage struct { // Returned from GetMessages
	id              string //TODO: GUID??
	insertionTime   time.Time
	expirationTime  time.Time
	popReceipt      PopReceipt
	nextVisibleTime time.Time
	dequeueCount    int32
	text            string // TODO: []byte after base-64 decoding?
}

func (m *DequeuedMessage) ID() string                 { return m.ID }
func (m *DequeuedMessage) InsertionTime() time.Time   { return m.insertionTime }
func (m *DequeuedMessage) ExpirationTime() time.Time  { return m.expirationTime }
func (m *DequeuedMessage) PopReceipt() PopReceipt     { return m.popReceipt }
func (m *DequeuedMessage) NextVisibleTime() time.Time { return m.nextVisibleTime }
func (m *DequeuedMessage) DequeueCount() int          { return m.dequeueCount }
func (m *DequeuedMessage) Text() string               { return m.text }

type PopReceipt string
