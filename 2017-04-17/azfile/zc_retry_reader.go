package azfile

import (
	"context"
	"io"
	"net"
	"net/http"
)

// HTTPGetter is a function type that refers to a method that performs an HTTP GET operation.
type HTTPGetter func(ctx context.Context, i HTTPGetterInfo) (*http.Response, error)

// HTTPGetterInfo is passed to an HTTPGetter function passing it parameters
// that should be used to make an HTTP GET request.
type HTTPGetterInfo struct {
	// Offset specifies the start offset that should be used when
	// creating the HTTP GET request's Range header
	Offset int64

	// Count specifies the count of bytes that should be used to calculate
	// the end offset when creating the HTTP GET request's Range header
	Count int64

	// ETag specifies the resource's etag that should be used when creating
	// the HTTP GET request's If-Match header
	ETag ETag
}

// RetryReaderOptions contains properties which can help to decide when to do retry.
type RetryReaderOptions struct {
	// MaxRetryRequests specifies the maximum number of HTTP GET requests that will be made
	// while reading from a RetryReader. A value of zero means that no additional HTTP
	// GET requests will be made.
	MaxRetryRequests   int
	doInjectError      bool
	doInjectErrorRound int
}

// retryReader implements io.ReaderCloser methods.
// retryReader tries to read from response, and if there is retriable network error
// returned during reading, it will retry according to retry reader option through executing
// user defined action with provided data to get a new response, and continue the overall reading process
// through reading from the new response.
type retryReader struct {
	ctx      context.Context
	response *http.Response

	info HTTPGetterInfo
	o    RetryReaderOptions

	getter HTTPGetter
}

// NewRetryReader creates a retry reader.
func NewRetryReader(ctx context.Context, initialResponse *http.Response,
	info HTTPGetterInfo, o RetryReaderOptions, getter HTTPGetter) io.ReadCloser {

	if initialResponse == nil {
		panic("initialResponse must not be nil")
	}
	if getter == nil {
		panic("getter must not be nil")
	}
	if info.Count < 0 {
		panic("info.Count must be >= 0")
	}
	if o.MaxRetryRequests < 0 {
		panic("o.MaxRetryRequests must be >= 0")
	}

	return &retryReader{ctx: ctx, getter: getter, info: info, response: initialResponse, o: o}
}

func (s *retryReader) Read(p []byte) (n int, err error) {
	try := 0
	for ; ; try++ {
		if s.info.Count == 0 { // When there is no more bytes to read, return with error io.EOF directly
			return 0, io.EOF
		}

		//fmt.Println(try)       // Comment out for debugging.
		n, err := s.response.Body.Read(p) // Read from the stream

		// Injection mechanism for testing.
		if s.o.doInjectError && try == s.o.doInjectErrorRound {
			err = &net.DNSError{IsTemporary: true}
		}

		// We successfully read data or end EOF.
		if err == nil || err == io.EOF {
			s.info.Offset += int64(n) // Increments the start offset in case we need to make a new HTTP request in the future
			if s.info.Count != 0 {
				s.info.Count -= int64(n) // Decrement the count in case we need to make a new HTTP request in the future
			}
			return n, err // Return the return to the caller
		}

		s.Close()
		s.response = nil // Something went wrong; our stream is no longer good

		// Check the retry count and error code, and decide whether to retry.
		if try >= s.o.MaxRetryRequests {
			return n, err // No retry, or retry exhausted
		}

		if netErr, ok := err.(net.Error); ok && (netErr.Timeout() || netErr.Temporary()) {
			// Do retry. We don't have a healthy response stream to read from, try to get one.
			response, err := s.getter(s.ctx, s.info)
			if err != nil {
				return 0, err // No retry when fail to execute getter, only retry for retriable read errors
			} else if response == nil {
				panic("getter should not return nil response when there is no error.")
			}
			// Successful GET; this is the network stream we'll read from.
			s.response = response

			// Loop around and try to read from this stream.
		} else {
			return n, err // Not retryable, just return
		}
	}
}

func (s *retryReader) Close() error {
	if s.response != nil && s.response.Body != nil {
		return s.response.Body.Close()
	}
	return nil
}
