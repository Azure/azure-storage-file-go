package azfile

import (
	"context"
	"io"
	"net"
	"net/http"
)

// Getter defines action which will be used to get a new response.
type Getter func(ctx context.Context, getInfo GetInfo) (*http.Response, error)

// GetInfo contains properties which will be passed to Getter.
type GetInfo struct {
	offset, count int64
	eTag          ETag
	userDefined   interface{} // User definied struct pointers or values.
}

// ResilientReaderOptions contains properties which can help to decide when to do retry.
type ResilientReaderOptions struct {
	MaxRetryRequests   int // TODO: rename or make this better (maxRetryFailures)
	doInjectError      bool
	doInjectErrorRound int
}

// resilientReader implements io.ReaderCloser methods.
// resilientReader tries to read from response, and if the response is nil or there is retriable network error
// returned during reading, it will retry according to resilient reader option through executing
// user defined action with provided data to get a new response, and continue the overall reading process
// through reading from the new response.
type resilientReader struct {
	ctx      context.Context
	response *http.Response
	getter   Getter
	getInfo  GetInfo

	o ResilientReaderOptions
}

// NewResilientReader creates a resilient reader.
func NewResilientReader(
	ctx context.Context,
	startResponse *http.Response,
	getter Getter, // User defined action
	getInfo GetInfo, // Data which will be passed to user definied action
	o ResilientReaderOptions) io.ReadCloser {

	if getter == nil {
		panic("getter must not be nil")
	}
	return &resilientReader{ctx: ctx, getter: getter, getInfo: getInfo, response: startResponse, o: o}
}

func (s *resilientReader) Read(p []byte) (n int, err error) {
	try := 0
	for ; try <= s.o.MaxRetryRequests; try++ {
		//fmt.Println(try)       // Comment out for debugging.
		if s.response != nil { // We working with a successful response
			n, err := s.response.Body.Read(p) // Read from the stream

			// Injection mechanism for testing.
			if s.o.doInjectError && try == s.o.doInjectErrorRound {
				err = &net.DNSError{IsTemporary: true}
			}

			// We successfully read data or end EOF.
			if err == nil || err == io.EOF {
				s.getInfo.offset += int64(n) // Increments the start offset in case we need to make a new HTTP request in the future
				if s.getInfo.count != 0 {
					s.getInfo.count -= int64(n) // Decrement the count in case we need to make a new HTTP request in the future
				}
				return n, err // Return the return to the caller
			}

			s.Close()
			s.response = nil // Something went wrong; our stream is no longer good

			// Check the retry count and error code, and decide whether to retry.
			if try == s.o.MaxRetryRequests {
				return n, err // No retry, or retry exhausted
			} else if netErr, ok := err.(net.Error); ok {
				if !netErr.Timeout() && !netErr.Temporary() {
					return n, err // Not retryable
				}
			} else {
				return n, err // Not retryable, just return
			}
		}

		// We don't have a response stream to read from, try to get one.
		response, err := s.getter(s.ctx, s.getInfo)
		if err != nil {
			return 0, err
		}
		// Successful GET; this is the network stream we'll read from.
		s.response = response

		// Loop around and try to read from this stream.
	}

	if s.o.doInjectError &&
		s.o.doInjectErrorRound <= s.o.MaxRetryRequests &&
		s.o.doInjectErrorRound > 0 &&
		try < s.o.doInjectErrorRound {
		panic("invalid status, internal error, stream read retry is not working properly.")
	}

	return 0, nil // The compiler is wrong; we never actually get here
}

func (s *resilientReader) Close() error {
	if s.response != nil && s.response.Body != nil {
		return s.response.Body.Close()
	}
	return nil
}
