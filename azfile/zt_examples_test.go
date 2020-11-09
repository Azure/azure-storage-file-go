package azfile_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-file-go/azfile"
)

// Please set environment variable ACCOUNT_NAME and ACCOUNT_KEY to your storage accout name and account key,
// before run the examples.
func accountInfo() (string, string) {
	return os.Getenv("ACCOUNT_NAME"), os.Getenv("ACCOUNT_KEY")
}

// This example shows how to get started using the Azure Storage File SDK for Go.
func Example() {
	// From the Azure portal, get your Storage account's name and account key.
	accountName, accountKey := accountInfo()

	// Use your Storage account's name and key to create a credential object; this is used to access your account.
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	// Create a request pipeline that is used to process HTTP(S) requests and responses. It requires
	// your account credentials. In more advanced scenarios, you can configure telemetry, retry policies,
	// logging, and other options. Also, you can configure multiple request pipelines for different scenarios.
	p := azfile.NewPipeline(credential, azfile.PipelineOptions{})

	// From the Azure portal, get your Storage account file service URL endpoint.
	// The URL typically looks like this:
	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net", accountName))

	// Create an ServiceURL object that wraps the service URL and a request pipeline.
	serviceURL := azfile.NewServiceURL(*u, p)

	// Now, you can use the serviceURL to perform various share and file operations.

	// All HTTP operations allow you to specify a Go context.Context object to control cancellation/timeout.
	ctx := context.Background() // This example uses a never-expiring context.

	// This example shows several common operations just to get you started.

	// Create a URL that references a to-be-created share in your Azure Storage account.
	// This returns a ShareURL object that wraps the share's URL and a request pipeline (inherited from serviceURL)
	shareURL := serviceURL.NewShareURL("mysharehelloworld") // Share names require lowercase

	// Create the share on the service (with no metadata and default quota size)
	_, err = shareURL.Create(ctx, azfile.Metadata{}, 0)
	if err != nil && err.(azfile.StorageError) != nil && err.(azfile.StorageError).ServiceCode() != azfile.ServiceCodeShareAlreadyExists {
		log.Fatal(err)
	}

	// Create a URL that references to root directory in your Azure Storage account's share.
	// This returns a DirectoryURL object that wraps the directory's URL and a request pipeline (inherited from shareURL)
	directoryURL := shareURL.NewRootDirectoryURL()

	// Create a URL that references a to-be-created file in your Azure Storage account's directory.
	// This returns a FileURL object that wraps the file's URL and a request pipeline (inherited from directoryURL)
	fileURL := directoryURL.NewFileURL("HelloWorld.txt") // File names can be mixed case and is case insensitive

	// Create the file with string (plain text) content.
	data := "Hello World!"
	length := int64(len(data))
	_, err = fileURL.Create(ctx, length, azfile.FileHTTPHeaders{ContentType: "text/plain"}, azfile.Metadata{})
	if err != nil {
		log.Fatal(err)
	}

	_, err = fileURL.UploadRange(ctx, 0, strings.NewReader(data), nil)
	if err != nil {
		log.Fatal(err)
	}

	// Download the file's contents and verify that it worked correctly.
	// User can specify 0 as Offset and azfile.CountToEnd(-1) as Count to indiciate downloading the entire file.
	get, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
	if err != nil {
		log.Fatal(err)
	}

	downloadedData := &bytes.Buffer{}
	retryReader := get.Body(azfile.RetryReaderOptions{})
	defer retryReader.Close() // The client must close the response body when finished with it

	downloadedData.ReadFrom(retryReader)
	fmt.Println("File content: " + downloadedData.String())

	// New a reference to a directory with name DemoDir in share, and create the directory.
	directoryDemoURL := shareURL.NewDirectoryURL("DemoDir")
	_, err = directoryDemoURL.Create(ctx, azfile.Metadata{}, azfile.SMBProperties{})
	if err != nil && err.(azfile.StorageError) != nil && err.(azfile.StorageError).ServiceCode() != azfile.ServiceCodeResourceAlreadyExists {
		log.Fatal(err)
	}

	// List the file(s) and directory(s) in our share's root directory; since a directory may hold millions of files and directories, this is done 1 segment at a time.
	for marker := (azfile.Marker{}); marker.NotDone(); { // The parentheses around azfile.Marker{} are required to avoid compiler error.
		// Get a result segment starting with the file indicated by the current Marker.
		listResponse, err := directoryURL.ListFilesAndDirectoriesSegment(ctx, marker, azfile.ListFilesAndDirectoriesOptions{})
		if err != nil {
			log.Fatal(err)
		}
		// IMPORTANT: ListFilesAndDirectoriesSegment returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listResponse.NextMarker

		// Process the files returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, fileEntry := range listResponse.FileItems {
			fmt.Println("File name: " + fileEntry.Name)
		}

		// Process the directories returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, directoryEntry := range listResponse.DirectoryItems {
			fmt.Println("Directory name: " + directoryEntry.Name)
		}
	}

	// Delete the file we created earlier.
	_, err = fileURL.Delete(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Delete the share we created earlier (with azfile.DeleteSnapshotsOptionNone as no snapshot exists and needs to be deleted).
	_, err = shareURL.Delete(ctx, azfile.DeleteSnapshotsOptionNone)
	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// File content: Hello World!
	// File name: HelloWorld.txt
	// Directory name: DemoDir
}

// This example shows how you can configure a pipeline for making HTTP requests to the Azure Storage File Service.
func ExampleNewPipeline() {
	// This example shows how to wire in your own logging mechanism (this example uses
	// Go's standard logger to write log information to standard error)
	logger := log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds)

	// Create/configure a request pipeline options object.
	// All PipelineOptions' fields are optional; reasonable defaults are set for anything you do not specify
	po := azfile.PipelineOptions{
		// Set RetryOptions to control how HTTP request are retried when retryable failures occur
		Retry: azfile.RetryOptions{
			Policy:        azfile.RetryPolicyExponential, // Use exponential backoff as opposed to linear
			MaxTries:      3,                             // Try at most 3 times to perform the operation (set to 1 to disable retries)
			TryTimeout:    time.Second * 3,               // Maximum time allowed for any single try
			RetryDelay:    time.Second * 1,               // Backoff amount for each retry (exponential or linear)
			MaxRetryDelay: time.Second * 3,               // Max delay between retries
		},

		// Set RequestLogOptions to control how each HTTP request & its response is logged
		RequestLog: azfile.RequestLogOptions{
			LogWarningIfTryOverThreshold: time.Millisecond * 200, // A successful response taking more than this time to arrive is logged as a warning
		},

		// Set LogOptions to control what & where all pipeline log events go
		Log: pipeline.LogOptions{
			Log: func(s pipeline.LogLevel, m string) { // This func is called to log each event
				// This method is not called for filtered-out severities.
				logger.Output(2, m) // This example uses Go's standard logger
			},
			ShouldLog: func(level pipeline.LogLevel) bool {
				return level <= pipeline.LogInfo // Log all events from informational to more severe
			},
		},
	}

	// Create a request pipeline object configured with credentials and with pipeline options. Once created,
	// a pipeline object is goroutine-safe and can be safely used with many XxxURL objects simultaneously.
	p := azfile.NewPipeline(azfile.NewAnonymousCredential(), po) // A pipeline always requires some credential object

	// Once you've created a pipeline object, associate it with an XxxURL object so that you can perform HTTP requests with it.
	u, _ := url.Parse("https://myaccount.file.core.windows.net")
	serviceURL := azfile.NewServiceURL(*u, p)
	// Use the serviceURL as desired...

	// NOTE: When you use an XxxURL object to create another XxxURL object, the new XxxURL object inherits the
	// same pipeline object as its parent. For example, the shareURL and fileURL objects (created below)
	// all share the same pipeline. Any HTTP operations you perform with these objects share the behavior (retry, logging, etc.)
	shareURL := serviceURL.NewShareURL("myshare")
	directoryURL := shareURL.NewDirectoryURL("mydirectory")
	fileURL := directoryURL.NewFileURL("ReadMe.txt")

	// If you'd like to perform some operations with different behavior, create a new pipeline object and
	// associate it with a new XxxURL object by passing the new pipeline to the XxxURL object's WithPipeline method.

	// In this example, I reconfigure the retry policies, create a new pipeline, and then create a new
	// ShareURL object that has the same URL as its parent.
	po.Retry = azfile.RetryOptions{
		Policy:        azfile.RetryPolicyFixed, // Use linear backoff
		MaxTries:      4,                       // Try at most 3 times to perform the operation (set to 1 to disable retries)
		TryTimeout:    time.Minute * 1,         // Maximum time allowed for any single try
		RetryDelay:    time.Second * 5,         // Backoff amount for each retry (exponential or linear)
		MaxRetryDelay: time.Second * 10,        // Max delay between retries
	}
	newShareURL := shareURL.WithPipeline(azfile.NewPipeline(azfile.NewAnonymousCredential(), po))

	// Now, any XxxDirectoryURL object created using newShareURL inherits the pipeline with the new retry policy.
	newDirectoryURL := newShareURL.NewDirectoryURL("mynewdirectory")
	_, _, _ = fileURL, directoryURL, newDirectoryURL // Avoid compiler's "declared and not used" error
}

func ExampleStorageError() {
	// This example shows how to handle errors returned from various XxxURL methods. All these methods return an
	// object implementing the pipeline.Response interface and an object implementing Go's error interface.
	// The error result is nil if the request was successful; your code can safely use the Response interface object.
	// If error is non-nil, the error could be due to:

	// 1. An invalid argument passed to the method. You should not write code to handle these errors;
	//    instead, fix these errors as they appear during development/testing.

	// 2. A network request didn't reach an Azure Storage Service. This usually happens due to a bad URL or
	//    faulty networking infrastructure (like a router issue). In this case, an object implementing the
	//    net.Error interface will be returned. The net.Error interface offers Timeout and Temporary methods
	//    which return true if the network error is determined to be a timeout or temporary condition. If
	//    your pipeline uses the retry policy factory, then this policy looks for Timeout/Temporary and
	//    automatically retries based on the retry options you've configured. Because of the retry policy,
	//    your code will usually not call the Timeout/Temporary methods explicitly other than possibly logging
	//    the network failure.

	// 3. A network request did reach the Azure Storage Service but the service failed to perform the
	//    requested operation. In this case, an object implementing the azfile.StorageError interface is returned.
	//    The azfile.StorageError interface also implements the net.Error interface and, if you use the retry policy,
	//    you would most likely ignore the Timeout/Temporary methods. However, the azfile.StorageError interface exposes
	//    richer information such as a service error code, an error description, details data, and the
	//    service-returned http.Response. And, from the http.Response, you can get the initiating http.Request.

	u, _ := url.Parse("http://myaccount.file.core.windows.net/myshare") // Suppose there is an existing storage account with name myaccount
	shareURL := azfile.NewShareURL(*u, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))
	create, err := shareURL.Create(context.Background(), azfile.Metadata{}, 0)

	if err != nil { // Suppose there is an error occurred
		if serr, ok := err.(azfile.StorageError); ok { // This error is a Service-specific error
			// azfile.StorageError also implements net.Error so you could call its Timeout/Temporary methods if you want.
			switch serr.ServiceCode() { // Compare serviceCode to various ServiceCodeXxx constants
			case azfile.ServiceCodeShareAlreadyExists:
				// You can also look at the http.Response object that failed.
				if failedResponse := serr.Response(); failedResponse != nil {
					// From the response object, you can get the initiating http.Request object
					failedRequest := failedResponse.Request
					_ = failedRequest // Avoid compiler's "declared and not used" error
				}

			case azfile.ServiceCodeShareBeingDeleted:
				// Handle this error ...
			default:
				// Handle other errors ...
			}
			// You can also directly handle error through looking at HTTP's status code.
			if serr.Response().StatusCode == http.StatusForbidden {
				// Handle this error ...
			}
		}
		log.Fatal(err) // Error is not due to Azure Storage service; networking infrastructure failure
	}

	// If err is nil, then the method was successful; use the response to access the result
	_ = create // Avoid compiler's "declared and not used" error
}

// This example shows how to break a URL into its parts so you can
// examine and/or change some of its values and then construct a new URL.
func ExampleFileURLParts() {
	// Let's start with a URL that identifies a snapshot of a file in a share.
	// The URL also contains a Shared Access Signature (SAS):
	u, _ := url.Parse("https://myaccount.file.core.windows.net/myshare/mydirectory/ReadMe.txt?" +
		"sharesnapshot=2018-03-08T02:29:11.0000000Z&" +
		"sv=2015-02-21&sr=b&st=2111-01-09T01:42:34.936Z&se=2222-03-09T01:42:34.936Z&sp=rw&sip=168.1.5.60-168.1.5.70&" +
		"spr=https,http&si=myIdentifier&ss=bf&srt=s&sig=92836758923659283652983562==")

	// You can parse this URL into its constituent parts:
	parts := azfile.NewFileURLParts(*u)

	// Now, we access the parts (this example prints them).
	fmt.Println(parts.Host, parts.ShareName, parts.DirectoryOrFilePath, parts.ShareSnapshot)
	sas := parts.SAS
	fmt.Println(sas.Version(), sas.Resource(), sas.StartTime(), sas.ExpiryTime(), sas.Permissions(),
		sas.IPRange(), sas.Protocol(), sas.Identifier(), sas.Services(), sas.ResourceTypes(), sas.Signature())

	// You can then change some of the fields and construct a new URL:
	parts.SAS = azfile.SASQueryParameters{} // Remove the SAS query parameters
	parts.ShareSnapshot = ""                // Remove the share snapshot timestamp
	parts.ShareName = "othershare"          // Change the share name
	// In this example, we'll keep the path of file or directory as is.

	// Construct a new URL from the parts:
	newURL := parts.URL()
	fmt.Print(newURL.String())
	// NOTE: You can pass the new URL to azfile.NewFileURLParts (or similar methods) to manipulate the file.

	// Output:
	// myaccount.file.core.windows.net myshare mydirectory/ReadMe.txt 2018-03-08T02:29:11.0000000Z
	// 2015-02-21 b 2111-01-09 01:42:34.936 +0000 UTC 2222-03-09 01:42:34.936 +0000 UTC rw {168.1.5.60 168.1.5.70} https,http myIdentifier bf s 92836758923659283652983562==
	// https://myaccount.file.core.windows.net/othershare/mydirectory/ReadMe.txt
}

// This example shows how to create and use an Azure Storage account Shared Access Signature (SAS).
func ExampleAccountSASSignatureValues() {
	// From the Azure portal, get your Storage account's name and account key.
	accountName, accountKey := accountInfo()

	// Use your Storage account's name and key to create a credential object; this is required to sign a SAS.
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	// Set the desired SAS signature values and sign them with the shared key credentials to get the SAS query parameters.
	sasQueryParams, err := azfile.AccountSASSignatureValues{
		Protocol:      azfile.SASProtocolHTTPS,              // Users MUST use HTTPS (not HTTP)
		ExpiryTime:    time.Now().UTC().Add(48 * time.Hour), // 48-hours before expiration
		Permissions:   azfile.AccountSASPermissions{Read: true, List: true}.String(),
		Services:      azfile.AccountSASServices{File: true}.String(),
		ResourceTypes: azfile.AccountSASResourceTypes{Container: true, Object: true}.String(),
	}.NewSASQueryParameters(credential)
	if err != nil {
		log.Fatal(err)
	}

	qp := sasQueryParams.Encode()
	urlToSendToSomeone := fmt.Sprintf("https://%s.file.core.windows.net?%s", accountName, qp)
	// At this point, you can send the urlToSendToSomeone to someone via email or any other mechanism you choose.

	// ************************************************************************************************

	// When someone receives the URL, they access the SAS-protected resource with code like this:
	u, _ := url.Parse(urlToSendToSomeone)

	// Create an ServiceURL object that wraps the service URL (and its SAS) and a pipeline.
	// When using a SAS URLs, anonymous credentials are required.
	serviceURL := azfile.NewServiceURL(*u, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))
	// Now, you can use this serviceURL just like any other to make requests of the resource.

	// You can parse a URL into its constituent parts:
	fileURLParts := azfile.NewFileURLParts(serviceURL.URL())
	fmt.Printf("SAS Protocol=%v\n", fileURLParts.SAS.Protocol())
	fmt.Printf("SAS Permissions=%v\n", fileURLParts.SAS.Permissions())
	fmt.Printf("SAS Services=%v\n", fileURLParts.SAS.Services())
	fmt.Printf("SAS ResourceTypes=%v\n", fileURLParts.SAS.ResourceTypes())

	_ = serviceURL // Avoid compiler's "declared and not used" error

	// Output:
	// SAS Protocol=https
	// SAS Permissions=rl
	// SAS Services=f
	// SAS ResourceTypes=co
}

// This example shows how to create and use a File Service Shared Access Signature (SAS).
func ExampleFileSASSignatureValues() {
	// From the Azure portal, get your Storage account's name and account key.
	accountName, accountKey := accountInfo()

	// Use your Storage account's name and key to create a credential object; this is required to sign a SAS.
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	// This is the name of the share and path of the file that we're creating a SAS to.
	shareName := "myshare"                   // Share names require lowercase
	filePath := "mydirectory/HelloWorld.txt" // Directory and file path can be mixed case and is case insensitive

	// Set the desired SAS signature values and sign them with the shared key credentials to get the SAS query parameters.
	sasQueryParams, err := azfile.FileSASSignatureValues{
		Protocol:   azfile.SASProtocolHTTPS,              // Users MUST use HTTPS (not HTTP)
		ExpiryTime: time.Now().UTC().Add(48 * time.Hour), // 48-hours before expiration
		ShareName:  shareName,
		FilePath:   filePath,

		// To produce a share SAS (as opposed to a file SAS in this example), assign to Permissions using
		// ShareSASPermissions and make sure the FilePath field is "" (the default).
		Permissions: azfile.FileSASPermissions{Read: true, Write: true}.String(),
	}.NewSASQueryParameters(credential)
	if err != nil {
		log.Fatal(err)
	}

	// Create the URL of the resource you wish to access and append the SAS query parameters.
	// Since this is a file SAS, the URL is to the Azure storage file.
	qp := sasQueryParams.Encode()
	urlToSendToSomeone := fmt.Sprintf("https://%s.file.core.windows.net/%s/%s?%s",
		accountName, shareName, filePath, qp)
	// At this point, you can send the urlToSendToSomeone to someone via email or any other mechanism you choose.

	// ************************************************************************************************

	// When someone receives the URL, they access the SAS-protected resource with code like this:
	u, _ := url.Parse(urlToSendToSomeone)

	// Create an FileURL object that wraps the file URL (and its SAS) and a pipeline.
	// When using a SAS URLs, anonymous credentials are required.
	fileURL := azfile.NewFileURL(*u, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))
	// Now, you can use this fileURL just like any other to make requests of the resource.

	// If you have a SAS query parameter string, you can parse it into its parts:
	fileURLParts := azfile.NewFileURLParts(fileURL.URL())
	fmt.Printf("SAS expiry time=%v", fileURLParts.SAS.ExpiryTime())
	fmt.Printf(urlToSendToSomeone)

	_ = fileURL // Avoid compiler's "declared and not used" error
}

// This examples shows how to create a share with metadata, how to read properties & update the metadata, and then delete the share.
func ExampleShareURL() {
	// From the Azure portal, get your Storage account file service URL endpoint.
	accountName, accountKey := accountInfo()

	// Create a ShareURL object that wraps a soon-to-be-created share's URL and a default pipeline.
	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/mysharegeneral", accountName))
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}
	shareURL := azfile.NewShareURL(*u, azfile.NewPipeline(credential, azfile.PipelineOptions{}))

	ctx := context.Background() // This example uses a never-expiring context

	// Create a share with some metadata (string key/value pairs) and default quota.
	// NOTE: Metadata key names are always converted to lowercase before being sent to the Storage Service.
	// Therefore, you should always use lowercase letters; especially when querying a map for a metadata key.
	_, err = shareURL.Create(ctx, azfile.Metadata{"createdby": "Jeffrey&Jiachen"}, 0)
	if err != nil {
		log.Fatal(err)
	}

	// Query the share's metadata
	get, err := shareURL.GetProperties(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Show the share's metadata
	metadata := get.NewMetadata()
	for k, v := range metadata {
		fmt.Print(k + "=" + v + "\n")
	}

	// Update the metadata and write it back to the share
	metadata["updateby"] = "Jiachen" // NOTE: The keyname is in all lowercase letters
	_, err = shareURL.SetMetadata(ctx, metadata)
	if err != nil {
		log.Fatal(err)
	}

	// NOTE: The SetMetadata & SetQuota methods update the share's ETag & LastModified properties

	// Delete the share
	_, err = shareURL.Delete(ctx, azfile.DeleteSnapshotsOptionNone)
	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// createdby=Jeffrey&Jiachen
}

// This example shows how to set maximum size for a file share.
func ExampleShareURL_SetQuota() {
	// Create a request pipeline using your Storage account's name and account key.
	accountName, accountKey := accountInfo()
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}
	p := azfile.NewPipeline(credential, azfile.PipelineOptions{})

	// From the Azure portal, get your Storage account file service URL endpoint.
	sURL, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/newshareforquotademo", accountName))

	// Create an ShareURL object that wraps the share URL and a request pipeline to making requests.
	shareURL := azfile.NewShareURL(*sURL, p)

	ctx := context.Background() // This example uses a never-expiring context

	_, err = shareURL.Create(ctx, azfile.Metadata{}, 0)
	if err != nil {
		log.Fatal(err)
	}

	// Check current usage stats for the share.
	// Note that the ShareStats object is part of the protocol layer for the File service.
	if statistics, err := shareURL.GetStatistics(ctx); err == nil {
		shareUsageGB := statistics.ShareUsageBytes / 1024 / 1024 / 1024
		fmt.Printf("Current share usage: %d GB\n", shareUsageGB)

		shareURL.SetProperties(ctx, 10+shareUsageGB)

		properties, err := shareURL.GetProperties(ctx)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Updated share usage: %d GB\n", properties.Quota())
	}

	_, err = shareURL.Delete(ctx, azfile.DeleteSnapshotsOptionNone)
	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// Current share usage: 0 GB
	// Updated share usage: 10 GB
}

// This example shows how to create, delete, list, and restore share snapshots.
func ExampleShareURL_CreateSnapshot() {
	// From the Azure portal, get your Storage account file service URL endpoint.
	accountName, accountKey := accountInfo()
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background() // This example uses a never-expiring context

	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net", accountName))
	serviceURL := azfile.NewServiceURL(*u, azfile.NewPipeline(credential, azfile.PipelineOptions{}))

	shareName := "baseshare"
	shareURL := serviceURL.NewShareURL(shareName)

	_, err = shareURL.Create(ctx, azfile.Metadata{}, 0)
	if err != nil {
		log.Fatal(err)
	}

	defer shareURL.Delete(ctx, azfile.DeleteSnapshotsOptionInclude)

	// Let's create a file in the base share.
	fileURL := shareURL.NewRootDirectoryURL().NewFileURL("myfile")
	_, err = fileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, azfile.Metadata{})
	if err != nil {
		log.Fatal(err)
	}

	// Create share snapshot, the snapshot contains the created file.
	snapshotShare, err := shareURL.CreateSnapshot(ctx, azfile.Metadata{})
	fmt.Printf("Created share snapshot: %s", snapshotShare.Snapshot())

	// List share snapshots.
	listSnapshot, err := serviceURL.ListSharesSegment(ctx, azfile.Marker{}, azfile.ListSharesOptions{Detail: azfile.ListSharesDetail{Snapshots: true}})
	for _, share := range listSnapshot.ShareItems {
		if share.Snapshot != nil {
			fmt.Printf("Listed share snapshot: %s\n", *share.Snapshot)
		}
	}

	// Delete file in base share.
	_, err = fileURL.Delete(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Restore file from share snapshot.
	// Create a SAS.
	sasQueryParams, err := azfile.FileSASSignatureValues{
		Protocol:   azfile.SASProtocolHTTPS,              // Users MUST use HTTPS (not HTTP)
		ExpiryTime: time.Now().UTC().Add(48 * time.Hour), // 48-hours before expiration
		ShareName:  shareName,

		// To produce a share SAS (as opposed to a file SAS), assign to Permissions using
		// ShareSASPermissions and make sure the DirectoryAndFilePath field is "" (the default).
		Permissions: azfile.ShareSASPermissions{Read: true, Write: true}.String(),
	}.NewSASQueryParameters(credential)
	if err != nil {
		log.Fatal(err)
	}

	// Build a file snapshot URL.
	fileParts := azfile.NewFileURLParts(fileURL.URL())
	fileParts.ShareSnapshot = snapshotShare.Snapshot()
	fileParts.SAS = sasQueryParams
	sourceURL := fileParts.URL()

	// Do restore.
	fileURL.StartCopy(ctx, sourceURL, azfile.Metadata{})
	if err != nil {
		log.Fatal(err)
	}

	// Delete share snapshot. To delete individual share snapshot, please use azfile.DeleteSnapshotsOptionNone
	_, err = shareURL.WithSnapshot(snapshotShare.Snapshot()).Delete(ctx, azfile.DeleteSnapshotsOptionNone)
	if err != nil {
		log.Fatal(err)
	}
}

// ExampleFileURL shows how to create & resize a file, and then update & get data in the file.
func ExampleFileURL() {
	// From the Azure portal, get your Storage account file service URL endpoint.
	accountName, accountKey := accountInfo()

	ctx := context.Background() // This example uses a never-expiring context
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	// Prepare a share for the file example.
	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/myshare", accountName))
	shareURL := azfile.NewShareURL(*u, azfile.NewPipeline(credential, azfile.PipelineOptions{}))

	_, err = shareURL.Create(ctx, azfile.Metadata{}, 0)
	if err != nil && err.(azfile.StorageError) != nil && err.(azfile.StorageError).ServiceCode() != azfile.ServiceCodeShareAlreadyExists {
		log.Fatal(err)
	}

	// Create a FileURL object with a default pipeline.
	u, _ = url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/myshare/MyFile.txt", accountName))
	fileURL := azfile.NewFileURL(*u, azfile.NewPipeline(credential, azfile.PipelineOptions{}))

	// Create the file with string (plain text) content.
	d1 := "Hello "
	d1Length := int64(len(d1))
	_, err = fileURL.Create(ctx, d1Length, azfile.FileHTTPHeaders{ContentType: "text/plain"}, azfile.Metadata{})
	if err != nil {
		log.Fatal(err)
	}

	// UploadRange updates data in the file with the range for d1.
	// In this stage, file created has one range: [0, d1Length-1]
	_, err = fileURL.UploadRange(ctx, 0, strings.NewReader(d1), nil)
	if err != nil {
		log.Fatal(err)
	}

	// We have more data to save in the file.
	d2 := "World!"
	d2Offset := d1Length
	d2Length := int64(len(d2))
	totalLength := d1Length + d2Length

	// Resize the file, as we want to save more data in this file.
	_, err = fileURL.Resize(ctx, totalLength)
	if err != nil {
		log.Fatal(err)
	}

	// UploadRange updates data in the file with the range for d2.
	// In this stage, file created has two ranges: [0, length-1] for data and [d2Offset, totalLength-1] for d2.
	_, err = fileURL.UploadRange(ctx, d2Offset, strings.NewReader(d2), nil)
	if err != nil {
		log.Fatal(err)
	}

	// Let's get all the data saved in the file, and verify if data is correct.
	// User can specify 0 as Offset and azfile.CountToEnd(-1) as Count to indiciate downloading the entire file.
	get, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
	if err != nil {
		log.Fatal(err)
	}

	fileData := &bytes.Buffer{}
	// The resilient reader can help to read stream in a resilient way, by default it returns a raw stream,
	// which will not provide additional retry mechanism.
	retryReader := get.Body(azfile.RetryReaderOptions{})
	defer retryReader.Close() // The client must close the response body when finished with it

	_, err = fileData.ReadFrom(retryReader)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(fileData)
	// The output would be:
	// Hello World!
}

// This examples shows how to create a file with metadata and then how to get properties & update
// the file's metadata.
func ExampleFileURL_GetProperties() {
	// From the Azure portal, get your Storage account file service URL endpoint.
	accountName, accountKey := accountInfo()
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	// Create a FileURL with default pipeline based on an existing share with name myshare.
	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/myshare/ReadMe.txt", accountName))
	fileURL := azfile.NewFileURL(*u, azfile.NewPipeline(credential, azfile.PipelineOptions{}))

	ctx := context.Background() // This example uses a never-expiring context

	// Create a file with metadata (string key/value pairs)
	// NOTE: Metadata key names are always converted to lowercase before being sent to the Storage Service.
	// Therefore, you should always use lowercase letters; especially when querying a map for a metadata key.
	_, err = fileURL.Create(ctx, 0, azfile.FileHTTPHeaders{}, azfile.Metadata{"createdby": "Jeffrey&Jiachen"}) // With size 0
	if err != nil {
		log.Fatal(err)
	}

	// Query the file's properties and metadata
	get, err := fileURL.GetProperties(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Show some of the file's read-only properties
	fmt.Println(get.FileType(), get.ETag(), get.LastModified())

	// Show the file's metadata
	metadata := get.NewMetadata()
	for k, v := range metadata {
		fmt.Print(k + "=" + v + "\n")
	}

	// Update the file's metadata and write it back to the file
	metadata["updatedby"] = "Jiachen" // Add a new key/value; NOTE: The keyname is in all lowercase letters
	_, err = fileURL.SetMetadata(ctx, metadata)
	if err != nil {
		log.Fatal(err)
	}

	// NOTE: The SetMetadata method updates the file's ETag & LastModified properties

	_, err = fileURL.Delete(ctx)
	if err != nil {
		log.Fatal(err)
	}
}

// This examples shows how to create a file with HTTP Headers and then how to read & update
// the file's HTTP headers.
func ExampleFileURL_SetHTTPHeaders() {
	// From the Azure portal, get your Storage account file service URL endpoint.
	accountName, accountKey := accountInfo()
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	// Create a FileURL with default pipeline based on an existing share with name myshare.
	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/myshare/HelpForHTTPHeader.txt", accountName))
	fileURL := azfile.NewFileURL(*u, azfile.NewPipeline(credential, azfile.PipelineOptions{}))

	ctx := context.Background() // This example uses a never-expiring context

	// Create a file with HTTP headers
	_, err = fileURL.Create(ctx, 0,
		azfile.FileHTTPHeaders{
			ContentType:        "text/html; charset=utf-8",
			ContentDisposition: "attachment",
		},
		azfile.Metadata{}) // With size 0
	if err != nil {
		log.Fatal(err)
	}

	// GetProperties returns the file's properties, HTTP headers, and metadata
	get, err := fileURL.GetProperties(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Read the file's HTTP Headers
	httpHeaders := get.NewHTTPHeaders()
	fmt.Println(httpHeaders.ContentType, httpHeaders.ContentDisposition)

	// Update the file's HTTP Headers and write them back to the file
	httpHeaders.ContentType = "text/plain"
	_, err = fileURL.SetHTTPHeaders(ctx, httpHeaders)
	if err != nil {
		log.Fatal(err)
	}

	// NOTE: The SetHTTPHeaders method updates the file's ETag & LastModified properties

	_, err = fileURL.Delete(ctx)
	if err != nil {
		log.Fatal(err)
	}
}

// This example shows how to upload and download with progress updates.
func ExampleFileURL_progressUploadDownload() {
	// Create a request pipeline using your Storage account's name and account key.
	accountName, accountKey := accountInfo()
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}
	p := azfile.NewPipeline(credential, azfile.PipelineOptions{})

	// From the Azure portal, get your Storage account file service URL endpoint.
	sURL, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/myshare", accountName))

	// Create a ShareURL object that wraps the share URL and a request pipeline to making requests.
	shareURL := azfile.NewShareURL(*sURL, p)

	ctx := context.Background() // This example uses a never-expiring context
	fileURL := shareURL.NewRootDirectoryURL().NewFileURL("Data.bin")

	// requestBody is the stream of data to write
	requestBody := strings.NewReader("Some text to write")
	size := requestBody.Len()

	// Wrap the request body in a RequestBodyProgress and pass a callback function for progress reporting.
	_, err = fileURL.Create(ctx, int64(size),
		azfile.FileHTTPHeaders{
			ContentType:        "text/html; charset=utf-8",
			ContentDisposition: "attachment",
		},
		azfile.Metadata{})
	if err != nil {
		log.Fatal(err)
	}

	_, err = fileURL.UploadRange(ctx, 0,
		pipeline.NewRequestBodyProgress(requestBody, func(bytesTransferred int64) {
			fmt.Printf("Wrote %d of %d bytes.\n", bytesTransferred, size)
		}), nil)
	if err != nil {
		log.Fatal(err)
	}

	// Here's how to read the file's data with progress reporting:
	get, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
	if err != nil {
		log.Fatal(err)
	}
	// Wrap the response body in a ResponseBodyProgress and pass a callback function for progress reporting.
	responseBody := pipeline.NewResponseBodyProgress(get.Body(azfile.RetryReaderOptions{}), func(bytesTransferred int64) {
		fmt.Printf("Read %d of %d bytes.\n", bytesTransferred, get.ContentLength())
	})

	downloadedData := &bytes.Buffer{}
	downloadedData.ReadFrom(responseBody)
	responseBody.Close() // The client must close the response body when finished with it
	// The downloaded file data is in downloadData's buffer
}

// This example shows how to copy a source document on the Internet to a file.
func ExampleFileURL_StartCopy() {
	// From the Azure portal, get your Storage account file service URL endpoint.
	accountName, accountKey := accountInfo()
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	// Create a ShareURL object to a share where we'll create a file and its snapshot.
	// Create a BlockFileURL object to a file in the share.
	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/myshare/CopiedFile.bin", accountName))
	fileURL := azfile.NewFileURL(*u, azfile.NewPipeline(credential, azfile.PipelineOptions{}))

	ctx := context.Background() // This example uses a never-expiring context

	src, _ := url.Parse("https://cdn2.auth0.com/docs/media/addons/azure_file.svg") // Suppose this is an accessible source resource
	startCopy, err := fileURL.StartCopy(ctx, *src, nil)
	if err != nil {
		log.Fatal(err)
	}

	copyID := startCopy.CopyID()
	copyStatus := startCopy.CopyStatus()
	for copyStatus == azfile.CopyStatusPending {
		time.Sleep(time.Second * 2)
		properties, err := fileURL.GetProperties(ctx)
		if err != nil {
			log.Fatal(err)
		}
		copyStatus = properties.CopyStatus()
	}
	fmt.Printf("StartCopy from %s to %s: ID=%s, Status=%s\n", src.String(), fileURL, copyID, copyStatus)
}

// This example shows how to download a large file using a RetryReader. Specifically, if
// the connection fails while reading, continuing to read from this RetryReader initiates a new
// Download call passing a range that starts from the last byte successfully read before the failure.
func ExampleFileURL_Download() {
	// From the Azure portal, get your Storage account file service URL endpoint.
	accountName, accountKey := accountInfo()
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	// Create a FileURL object to a file in the share (we assume the share & file already exist).
	// Note: You can call GetProperties first to ensure the Azure file exists before downloading.
	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/myshare/BigFile.bin", accountName))
	fileURL := azfile.NewFileURL(*u, azfile.NewPipeline(credential, azfile.PipelineOptions{}))

	// Trigger download.
	downloadResponse, err := fileURL.Download(context.Background(), 0, azfile.CountToEnd, false) // 0 Offset and azfile.CountToEnd(-1) Count means download entire file.
	if err != nil {
		log.Fatal(err)
	}

	contentLength := downloadResponse.ContentLength() // Used for progress reporting to report the total number of bytes being downloaded.

	// Setup RetryReader options for stream reading retry.
	retryReader := downloadResponse.Body(azfile.RetryReaderOptions{MaxRetryRequests: 3})

	// NewResponseBodyStream wraps the RetryReader with progress reporting; it returns an io.ReadCloser.
	progressReader := pipeline.NewResponseBodyProgress(retryReader,
		func(bytesTransferred int64) {
			fmt.Printf("Downloaded %d of %d bytes.\n", bytesTransferred, contentLength)
		})
	defer progressReader.Close() // The client must close the response body when finished with it

	file, err := os.Create("BigFile.bin") // Create the file to hold the downloaded file contents.
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	written, err := io.Copy(file, progressReader) // Write to the file by reading from the file (with intelligent retries).
	if err != nil {
		log.Fatal(err)
	}
	_ = written // Avoid compiler's "declared and not used" error
}

// This example shows how to upload a large local file to Azure file with parallel support.
func ExampleUploadFileToAzureFile() {
	file, err := os.Open("BigFile.bin") // Open the file we want to upload (we assume the file already exists).
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	fileSize, err := file.Stat() // Get the size of the file (stream)
	if err != nil {
		log.Fatal(err)
	}

	// From the Azure portal, get your Storage account file service URL endpoint.
	accountName, accountKey := accountInfo()
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	// Create a FileURL object to a file in the share (we assume the share already exists).
	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/myshare/BigFile.bin", accountName))

	fileURL := azfile.NewFileURL(*u, azfile.NewPipeline(credential, azfile.PipelineOptions{}))

	ctx := context.Background() // This example uses a never-expiring context

	// Trigger parallel upload with Parallelism set to 3. Note if there is an Azure file
	// with same name exists, UploadFileToAzureFile will overwrite the existing Azure file with new content,
	// and set specified azfile.FileHTTPHeaders and Metadata.
	err = azfile.UploadFileToAzureFile(ctx, file, fileURL,
		azfile.UploadToAzureFileOptions{
			Parallelism: 3,
			FileHTTPHeaders: azfile.FileHTTPHeaders{
				CacheControl: "no-transform",
			},
			Metadata: azfile.Metadata{
				"createdby": "Jeffrey&Jiachen",
			},
			// If Progress is non-nil, this function is called periodically as bytes are uploaded.
			Progress: func(bytesTransferred int64) {
				fmt.Printf("Uploaded %d of %d bytes.\n", bytesTransferred, fileSize.Size())
			},
		})
	if err != nil {
		log.Fatal(err)
	}
}

// This example shows how to download a large Azure file to local with parallel support.
func ExampleDownloadAzureFileToFile() {
	// From the Azure portal, get your Storage account file service URL endpoint.
	accountName, accountKey := accountInfo()
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	// Create a FileURL object to a file in the share (we assume the share & file already exist).
	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/myshare/BigFile.bin", accountName))
	fileURL := azfile.NewFileURL(*u, azfile.NewPipeline(credential, azfile.PipelineOptions{}))

	file, err := os.Create("BigFile.bin") // Create the file to hold the downloaded file contents.
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Trigger parallel download with Parallelism set to 3, MaxRetryRequestsPerRange means the Count of retry requests
	// could be sent if there is error during reading stream.
	downloadResponse, err := azfile.DownloadAzureFileToFile(context.Background(), fileURL, file,
		azfile.DownloadFromAzureFileOptions{
			Parallelism:              3,
			MaxRetryRequestsPerRange: 2,
			Progress: func(bytesTransferred int64) {
				fmt.Printf("Downloaded %d bytes.\n", bytesTransferred)
			},
		})
	if err != nil {
		log.Fatal(err)
	}

	lastModified := downloadResponse.LastModified() // You can check the property of download file as well.

	_ = lastModified // Avoid compiler's "declared and not used" error
}
