package azfile

import (
	"context"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

// RetryPolicy tells the pipeline what kind of retry policy to use. See the RetryPolicy* constants.
type RetryPolicy int32

const (
	// RetryPolicyExponential tells the pipeline to use an exponential back-off retry policy
	RetryPolicyExponential RetryPolicy = 0

	// RetryPolicyFixed tells the pipeline to use a fixed back-off retry policy
	RetryPolicyFixed RetryPolicy = 1
)

// RetryOptions configures the retry policy's behavior.
type RetryOptions struct {
	// Policy tells the pipeline what kind of retry policy to use. See the RetryPolicy* constants.\
	// A value of zero means that you accept our default policy.
	Policy RetryPolicy

	// MaxTries specifies the maximum number of attempts an operation will be tried before producing an error (0=default).
	// A value of zero means that you accept our default policy. A value of 1 means 1 try and no retries.
	MaxTries int32

	// TryTimeout indicates the maximum time allowed for any single try of an HTTP request.
	// A value of zero means that you accept our default timeout. NOTE: When transferring large amounts
	// of data, the default TryTimeout will probably not be sufficient. You should override this value
	// based on the bandwidth available to the host machine and proximity to the Storage service. A good
	// starting point may be something like (60 seconds per MB of anticipated-payload-size).
	TryTimeout time.Duration

	// RetryDelay specifies the amount of delay to use before retrying an operation (0=default).
	// When RetryPolicy is specified as RetryPolicyExponential, the delay increases exponentially
	// with each retry up to a maximum specified by MaxRetryDelay.
	// If you specify 0, then you must also specify 0 for MaxRetryDelay.
	// If you specify RetryDelay, then you must also specify MaxRetryDelay, and MaxRetryDelay should be
	// equal to or greater than RetryDelay.
	RetryDelay time.Duration

	// MaxRetryDelay specifies the maximum delay allowed before retrying an operation (0=default).
	// If you specify 0, then you must also specify 0 for RetryDelay.
	MaxRetryDelay time.Duration
}

func (o RetryOptions) defaults() RetryOptions {
	if o.Policy != RetryPolicyExponential && o.Policy != RetryPolicyFixed {
		panic("RetryPolicy must be RetryPolicyExponential or RetryPolicyFixed")
	}
	if o.MaxTries < 0 {
		panic("MaxTries must be >= 0")
	}
	if o.TryTimeout < 0 || o.RetryDelay < 0 || o.MaxRetryDelay < 0 {
		panic("TryTimeout, RetryDelay, and MaxRetryDelay must all be >= 0")
	}
	if o.RetryDelay > o.MaxRetryDelay {
		panic("RetryDelay must be <= MaxRetryDelay")
	}
	if (o.RetryDelay == 0 && o.MaxRetryDelay != 0) || (o.RetryDelay != 0 && o.MaxRetryDelay == 0) {
		panic("Both RetryDelay and MaxRetryDelay must be 0 or neither can be 0")
	}

	IfDefault := func(current *time.Duration, desired time.Duration) {
		if *current == time.Duration(0) {
			*current = desired
		}
	}

	// Set defaults if unspecified
	if o.MaxTries == 0 {
		o.MaxTries = 4
	}
	switch o.Policy {
	case RetryPolicyExponential:
		IfDefault(&o.TryTimeout, 1*time.Minute)
		IfDefault(&o.RetryDelay, 4*time.Second)
		IfDefault(&o.MaxRetryDelay, 120*time.Second)

	case RetryPolicyFixed:
		IfDefault(&o.TryTimeout, 1*time.Minute)
		IfDefault(&o.RetryDelay, 30*time.Second)
		IfDefault(&o.MaxRetryDelay, 120*time.Second)
	}
	return o
}

func (o RetryOptions) calcDelay(try int32) time.Duration { // try is >=1; never 0
	pow := func(number int64, exponent int32) int64 { // pow is nested helper function
		var result int64 = 1
		for n := int32(0); n < exponent; n++ {
			result *= number
		}
		return result
	}

	delay := time.Duration(0)
	switch o.Policy {
	case RetryPolicyExponential:
		delay = time.Duration(pow(2, try-1)-1) * o.RetryDelay

	case RetryPolicyFixed:
		if try > 1 { // Any try after the 1st uses the fixed delay
			delay = o.RetryDelay
		}
	}

	// Introduce some jitter:  [0.0, 1.0) / 2 = [0.0, 0.5) + 0.8 = [0.8, 1.3)
	delay = time.Duration(delay.Seconds() * (rand.Float64()/2 + 0.8) * float64(time.Second)) // NOTE: We want math/rand; not crypto/rand
	if delay > o.MaxRetryDelay {
		delay = o.MaxRetryDelay
	}
	return delay
}

// NewRetryPolicyFactory creates a RetryPolicyFactory object configured using the specified options.
func NewRetryPolicyFactory(o RetryOptions) pipeline.Factory {
	o = o.defaults() // Force defaults to be calculated
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		return func(ctx context.Context, request pipeline.Request) (response pipeline.Response, err error) {
			// Exponential retry algorithm: ((2 ^ attempt) - 1) * delay * random(0.8, 1.2)
			// When to retry: connection failure or an HTTP status code of 500 or greater, except 501 and 505
			for try := int32(1); try <= o.MaxTries; try++ {
				logf("\n=====> Try=%d\n", try)

				// Operate delay
				delay := o.calcDelay(try)
				logf("try=%d, Delay=%v\n", try, delay)
				time.Sleep(delay) // The 1st try returns 0 delay

				// Clone the original request to ensure that each try starts with the original (unmutated) request.
				requestCopy := request.Copy()

				// For every try, seek to the beginning of the Body stream.
				if err = requestCopy.RewindBody(); err != nil {
					panic(err)
				}

				// Set the server-side timeout query parameter "timeout=[seconds]"
				timeout := int32(o.TryTimeout.Seconds()) // Max seconds per try
				if deadline, ok := ctx.Deadline(); ok {  // If user's ctx has a deadline, make the timeout the smaller of the two
					t := int32(deadline.Sub(time.Now()).Seconds()) // Duration from now until user's ctx reaches its deadline
					logf("MaxTryTimeout=%d secs, TimeTilDeadline=%d sec\n", timeout, t)
					if t < timeout {
						timeout = t
					}
					if timeout < 0 {
						timeout = 0 // If timeout ever goes negative, set it to zero; this happen while debugging
					}
					logf("TryTimeout adjusted to=%d sec\n", timeout)
				}
				q := requestCopy.Request.URL.Query()
				q.Set("timeout", strconv.Itoa(int(timeout+1))) // Add 1 to "round up"
				requestCopy.Request.URL.RawQuery = q.Encode()
				logf("Url=%s\n", requestCopy.Request.URL.String())

				// Set the time for this particular retry operation and then Do the operation.
				tryCtx, tryCancel := context.WithTimeout(ctx, time.Second*time.Duration(timeout))
				response, err = next.Do(tryCtx, requestCopy) // Make the request
				logf("Err=%v, response=%v\n", err, response)

				action := "" // This MUST get changed within the switch code below
				switch {
				case ctx.Err() != nil:
					action = "NoRetry: Op timeout"
				case err != nil:
					// NOTE: Protocol Responder returns non-nil if REST API returns invalid status code for the invoked operation
					if nerr, ok := err.(net.Error); ok && (nerr.Temporary() || nerr.Timeout()) {
						action = "Retry: net.Error and Temporary() or Timeout()"
					} else {
						action = "NoRetry: unrecognized error"
					}
				default:
					action = "NoRetry: successful HTTP request" // no error
				}

				logf("Action=%s\n", action)
				// fmt.Println(action + "\n") // This is where we could log the retry operation; action is why we're retrying
				if action[0] != 'R' { // Retry only if action starts with 'R'
					if err != nil {
						tryCancel() // If we're returning an error, cancel this current/last per-retry timeout context
					} else {
						// TODO: Right now, we've decided to leak the per-try Context until the user's Context is canceled.
						// Another option is that we wrap the last per-try context in a body and overwrite the Response's Body field with our wrapper.
						// So, when the user closes the Body, the our per-try context gets closed too.
						// Another option, is that the Last Policy do this wrapping for a per-retry context (not for the user's context)
						_ = tryCancel // So, for now, we don't call cancel: cancel()
					}
					break // Don't retry
				}
				// If retrying, cancel the current per-try timeout context
				tryCancel()
			}
			return response, err // Not retryable or too many retries; return the last response/error
		}
	})
}

// According to https://github.com/golang/go/wiki/CompilerOptimizations, the compiler will inline this method and hopefully optimize all calls to it away
var logf = func(format string, a ...interface{}) {}

// Use this version to see the retry method's code path (import "fmt")
//var logf = fmt.Printf
