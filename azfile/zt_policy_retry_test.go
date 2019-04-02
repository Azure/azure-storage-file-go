package azfile

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	chk "gopkg.in/check.v1"
)

type policyRetrySuite struct{}

var _ = chk.Suite(&policyRetrySuite{})

const testRetryErrorMockURL = "https://mockaccount.file.core.windows.net/"

type testRetryTempError struct{} // This can be extended to be more flexible.

const testRetryErrorMessage = "Test retry error message."

func (e *testRetryTempError) Error() string {
	return testRetryErrorMessage
}

// The test error is said to be a Temporary error.
func (e *testRetryTempError) Temporary() bool {
	return true
}

// The test error is said to be not a Timeout error.
func (e *testRetryTempError) Timeout() bool {
	return false
}

func newTestRetryPipeline(retryOptions RetryOptions) pipeline.Pipeline {
	f := []pipeline.Factory{
		NewRetryPolicyFactory(retryOptions),
		pipeline.MethodFactoryMarker(),
		newTestRetryPolicyFactory(),
	}

	return pipeline.NewPipeline(f, pipeline.Options{})
}

func newTestRetryPolicyFactory() pipeline.Factory {
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		return func(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
			return nil, &testRetryTempError{} // Never goes to wire.
		}
	})
}

func (s *policyRetrySuite) TestLinearRetry(c *chk.C) {
	buffer := bytes.Buffer{}
	logf = func(format string, a ...interface{}) {
		fmt.Fprintf(&buffer, format, a...)
	}

	defer func() {
		logf = func(format string, a ...interface{}) {}
	}()

	mockURL, _ := url.Parse(testRetryErrorMockURL)

	retryOption := RetryOptions{
		Policy:        RetryPolicyFixed,
		MaxTries:      3,
		RetryDelay:    time.Duration(2) * time.Second,
		MaxRetryDelay: time.Duration(2) * time.Second,
	}

	fsu := NewServiceURL(*mockURL, newTestRetryPipeline(retryOption))

	_, err := fsu.GetProperties(context.Background())

	c.Assert(err.Error(), chk.Equals, testRetryErrorMessage)

	str := buffer.String()

	c.Assert(strings.Contains(str, "Try=1"), chk.Equals, true)
	c.Assert(strings.Contains(str, "try=1, Delay=0s"), chk.Equals, true)
	c.Assert(strings.Contains(str, "Try=2"), chk.Equals, true)
	c.Assert(strings.Contains(str, "try=2, Delay=1") || strings.Contains(str, "try=2, Delay=2s"), chk.Equals, true) // Note the jitter: [0.0, 1.0) / 2 = [0.0, 0.5) + 0.8 = [0.8, 1.3)
	c.Assert(strings.Contains(str, "Try=3"), chk.Equals, true)
	c.Assert(strings.Contains(str, "try=3, Delay=1") || strings.Contains(str, "try=3, Delay=2s"), chk.Equals, true)
}

func (s *policyRetrySuite) TestExponentialRetry(c *chk.C) {
	buffer := bytes.Buffer{}
	logf = func(format string, a ...interface{}) {
		fmt.Fprintf(&buffer, format, a...)
	}

	defer func() {
		logf = func(format string, a ...interface{}) {}
	}()

	mockURL, _ := url.Parse(testRetryErrorMockURL)

	retryOption := RetryOptions{
		Policy:        RetryPolicyExponential,
		MaxTries:      4,
		RetryDelay:    time.Duration(1) * time.Second,
		MaxRetryDelay: time.Duration(2) * time.Second,
	}

	fsu := NewServiceURL(*mockURL, newTestRetryPipeline(retryOption))

	_, err := fsu.GetProperties(context.Background())

	c.Assert(err.Error(), chk.Equals, testRetryErrorMessage)

	str := buffer.String()

	c.Assert(strings.Contains(str, "Try=1"), chk.Equals, true)
	c.Assert(strings.Contains(str, "try=1, Delay=0s"), chk.Equals, true)
	c.Assert(strings.Contains(str, "Try=2"), chk.Equals, true)
	c.Assert(strings.Contains(str, "Try=3"), chk.Equals, true) // Min: 0.64 * 3 = 1.92
	c.Assert(strings.Contains(str, "Try=4"), chk.Equals, true)
	c.Assert(strings.Contains(str, "try=4, Delay=2s"), chk.Equals, true) // Min: 0.512 * 7 = 3.584
	// TODO add assertion here about minimum time taken
}
