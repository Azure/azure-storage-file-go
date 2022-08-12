package azfile

// Code generated by Microsoft (R) AutoRest Code Generator.
// Changes may cause incorrect behavior and will be lost if the code is regenerated.

import (
	"context"
	"encoding/xml"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
)

// directoryClient is the client for the Directory methods of the Azfile service.
type directoryClient struct {
	managementClient
}

// newDirectoryClient creates an instance of the directoryClient client.
func newDirectoryClient(url url.URL, p pipeline.Pipeline) directoryClient {
	return directoryClient{newManagementClient(url, p)}
}

// Create creates a new directory under the specified share or parent directory.
//
// fileAttributes is if specified, the provided file attributes shall be set. Default value: ‘Archive’ for file and
// ‘Directory’ for directory. ‘None’ can also be specified as default. fileCreationTime is creation time for the
// file/directory. Default value: Now. fileLastWriteTime is last write time for the file/directory. Default value: Now.
// shareName is the name of the target share. directory is the path of the target directory. timeout is the timeout
// parameter is expressed in seconds. For more information, see <a
// href="https://docs.microsoft.com/en-us/rest/api/storageservices/Setting-Timeouts-for-File-Service-Operations?redirectedfrom=MSDN">Setting
// Timeouts for File Service Operations.</a> metadata is a name-value pair to associate with a file storage object.
// filePermission is if specified the permission (security descriptor) shall be set for the directory/file. This header
// can be used if Permission size is <= 8KB, else x-ms-file-permission-key header shall be used. Default value:
// Inherit. If SDDL is specified as input, it must have owner, group and dacl. Note: Only one of the
// x-ms-file-permission or x-ms-file-permission-key should be specified. filePermissionKey is key of the permission to
// be set for the directory/file. Note: Only one of the x-ms-file-permission or x-ms-file-permission-key should be
// specified.
func (client directoryClient) Create(ctx context.Context, fileAttributes string, fileCreationTime string, fileLastWriteTime string, shareName string, directory string, timeout *int32, metadata map[string]string, filePermission *string, filePermissionKey *string) (*DirectoryCreateResponse, error) {
	if err := validate([]validation{
		{targetValue: timeout,
			constraints: []constraint{{target: "timeout", name: null, rule: false,
				chain: []constraint{{target: "timeout", name: inclusiveMinimum, rule: 0, chain: nil}}}}}}); err != nil {
		return nil, err
	}
	req, err := client.createPreparer(fileAttributes, fileCreationTime, fileLastWriteTime, shareName, directory, timeout, metadata, filePermission, filePermissionKey)
	if err != nil {
		return nil, err
	}
	resp, err := client.Pipeline().Do(ctx, responderPolicyFactory{responder: client.createResponder}, req)
	if err != nil {
		return nil, err
	}
	return resp.(*DirectoryCreateResponse), err
}

// createPreparer prepares the Create request.
func (client directoryClient) createPreparer(fileAttributes string, fileCreationTime string, fileLastWriteTime string, shareName string, directory string, timeout *int32, metadata map[string]string, filePermission *string, filePermissionKey *string) (pipeline.Request, error) {
	req, err := pipeline.NewRequest("PUT", client.url, nil)
	if err != nil {
		return req, pipeline.NewError(err, "failed to create request")
	}
	params := req.URL.Query()
	if timeout != nil {
		params.Set("timeout", strconv.FormatInt(int64(*timeout), 10))
	}
	params.Set("restype", "directory")
	req.URL.RawQuery = params.Encode()
	if metadata != nil {
		for k, v := range metadata {
			req.Header.Set("x-ms-meta-"+k, v)
		}
	}
	req.Header.Set("x-ms-version", ServiceVersion)
	if filePermission != nil {
		req.Header.Set("x-ms-file-permission", *filePermission)
	}
	if filePermissionKey != nil {
		req.Header.Set("x-ms-file-permission-key", *filePermissionKey)
	}
	req.Header.Set("x-ms-file-attributes", fileAttributes)
	req.Header.Set("x-ms-file-creation-time", fileCreationTime)
	req.Header.Set("x-ms-file-last-write-time", fileLastWriteTime)
	return req, nil
}

// createResponder handles the response to the Create request.
func (client directoryClient) createResponder(resp pipeline.Response) (pipeline.Response, error) {
	err := validateResponse(resp, http.StatusOK, http.StatusCreated)
	if resp == nil {
		return nil, err
	}
	io.Copy(ioutil.Discard, resp.Response().Body)
	resp.Response().Body.Close()
	return &DirectoryCreateResponse{rawResponse: resp.Response()}, err
}

// Delete removes the specified empty directory. Note that the directory must be empty before it can be deleted.
//
// shareName is the name of the target share. directory is the path of the target directory. timeout is the timeout
// parameter is expressed in seconds. For more information, see <a
// href="https://docs.microsoft.com/en-us/rest/api/storageservices/Setting-Timeouts-for-File-Service-Operations?redirectedfrom=MSDN">Setting
// Timeouts for File Service Operations.</a>
func (client directoryClient) Delete(ctx context.Context, shareName string, directory string, timeout *int32) (*DirectoryDeleteResponse, error) {
	if err := validate([]validation{
		{targetValue: timeout,
			constraints: []constraint{{target: "timeout", name: null, rule: false,
				chain: []constraint{{target: "timeout", name: inclusiveMinimum, rule: 0, chain: nil}}}}}}); err != nil {
		return nil, err
	}
	req, err := client.deletePreparer(shareName, directory, timeout)
	if err != nil {
		return nil, err
	}
	resp, err := client.Pipeline().Do(ctx, responderPolicyFactory{responder: client.deleteResponder}, req)
	if err != nil {
		return nil, err
	}
	return resp.(*DirectoryDeleteResponse), err
}

// deletePreparer prepares the Delete request.
func (client directoryClient) deletePreparer(shareName string, directory string, timeout *int32) (pipeline.Request, error) {
	req, err := pipeline.NewRequest("DELETE", client.url, nil)
	if err != nil {
		return req, pipeline.NewError(err, "failed to create request")
	}
	params := req.URL.Query()
	if timeout != nil {
		params.Set("timeout", strconv.FormatInt(int64(*timeout), 10))
	}
	params.Set("restype", "directory")
	req.URL.RawQuery = params.Encode()
	req.Header.Set("x-ms-version", ServiceVersion)
	return req, nil
}

// deleteResponder handles the response to the Delete request.
func (client directoryClient) deleteResponder(resp pipeline.Response) (pipeline.Response, error) {
	err := validateResponse(resp, http.StatusOK, http.StatusAccepted)
	if resp == nil {
		return nil, err
	}
	io.Copy(ioutil.Discard, resp.Response().Body)
	resp.Response().Body.Close()
	return &DirectoryDeleteResponse{rawResponse: resp.Response()}, err
}

// ForceCloseHandles closes all handles open for given directory.
//
// handleID is specifies handle ID opened on the file or directory to be closed. Asterisk (‘*’) is a wildcard that
// specifies all handles. shareName is the name of the target share. directory is the path of the target directory.
// timeout is the timeout parameter is expressed in seconds. For more information, see <a
// href="https://docs.microsoft.com/en-us/rest/api/storageservices/Setting-Timeouts-for-File-Service-Operations?redirectedfrom=MSDN">Setting
// Timeouts for File Service Operations.</a> marker is a string value that identifies the portion of the list to be
// returned with the next list operation. The operation returns a marker value within the response body if the list
// returned was not complete. The marker value may then be used in a subsequent call to request the next set of list
// items. The marker value is opaque to the client. sharesnapshot is the snapshot parameter is an opaque DateTime value
// that, when present, specifies the share snapshot to query. recursive is specifies operation should apply to the
// directory specified in the URI, its files, its subdirectories and their files.
func (client directoryClient) ForceCloseHandles(ctx context.Context, handleID string, shareName string, directory string, timeout *int32, marker *string, sharesnapshot *string, recursive *bool) (*DirectoryForceCloseHandlesResponse, error) {
	if err := validate([]validation{
		{targetValue: timeout,
			constraints: []constraint{{target: "timeout", name: null, rule: false,
				chain: []constraint{{target: "timeout", name: inclusiveMinimum, rule: 0, chain: nil}}}}}}); err != nil {
		return nil, err
	}
	req, err := client.forceCloseHandlesPreparer(handleID, shareName, directory, timeout, marker, sharesnapshot, recursive)
	if err != nil {
		return nil, err
	}
	resp, err := client.Pipeline().Do(ctx, responderPolicyFactory{responder: client.forceCloseHandlesResponder}, req)
	if err != nil {
		return nil, err
	}
	return resp.(*DirectoryForceCloseHandlesResponse), err
}

// forceCloseHandlesPreparer prepares the ForceCloseHandles request.
func (client directoryClient) forceCloseHandlesPreparer(handleID string, shareName string, directory string, timeout *int32, marker *string, sharesnapshot *string, recursive *bool) (pipeline.Request, error) {
	req, err := pipeline.NewRequest("PUT", client.url, nil)
	if err != nil {
		return req, pipeline.NewError(err, "failed to create request")
	}
	params := req.URL.Query()
	if timeout != nil {
		params.Set("timeout", strconv.FormatInt(int64(*timeout), 10))
	}
	if marker != nil && len(*marker) > 0 {
		params.Set("marker", *marker)
	}
	if sharesnapshot != nil && len(*sharesnapshot) > 0 {
		params.Set("sharesnapshot", *sharesnapshot)
	}
	params.Set("comp", "forceclosehandles")
	req.URL.RawQuery = params.Encode()
	req.Header.Set("x-ms-handle-id", handleID)
	if recursive != nil {
		req.Header.Set("x-ms-recursive", strconv.FormatBool(*recursive))
	}
	req.Header.Set("x-ms-version", ServiceVersion)
	return req, nil
}

// forceCloseHandlesResponder handles the response to the ForceCloseHandles request.
func (client directoryClient) forceCloseHandlesResponder(resp pipeline.Response) (pipeline.Response, error) {
	err := validateResponse(resp, http.StatusOK)
	if resp == nil {
		return nil, err
	}
	io.Copy(ioutil.Discard, resp.Response().Body)
	resp.Response().Body.Close()
	return &DirectoryForceCloseHandlesResponse{rawResponse: resp.Response()}, err
}

// GetProperties returns all system properties for the specified directory, and can also be used to check the existence
// of a directory. The data returned does not include the files in the directory or any subdirectories.
//
// shareName is the name of the target share. directory is the path of the target directory. sharesnapshot is the
// snapshot parameter is an opaque DateTime value that, when present, specifies the share snapshot to query. timeout is
// the timeout parameter is expressed in seconds. For more information, see <a
// href="https://docs.microsoft.com/en-us/rest/api/storageservices/Setting-Timeouts-for-File-Service-Operations?redirectedfrom=MSDN">Setting
// Timeouts for File Service Operations.</a>
func (client directoryClient) GetProperties(ctx context.Context, shareName string, directory string, sharesnapshot *string, timeout *int32) (*DirectoryGetPropertiesResponse, error) {
	if err := validate([]validation{
		{targetValue: timeout,
			constraints: []constraint{{target: "timeout", name: null, rule: false,
				chain: []constraint{{target: "timeout", name: inclusiveMinimum, rule: 0, chain: nil}}}}}}); err != nil {
		return nil, err
	}
	req, err := client.getPropertiesPreparer(shareName, directory, sharesnapshot, timeout)
	if err != nil {
		return nil, err
	}
	resp, err := client.Pipeline().Do(ctx, responderPolicyFactory{responder: client.getPropertiesResponder}, req)
	if err != nil {
		return nil, err
	}
	return resp.(*DirectoryGetPropertiesResponse), err
}

// getPropertiesPreparer prepares the GetProperties request.
func (client directoryClient) getPropertiesPreparer(shareName string, directory string, sharesnapshot *string, timeout *int32) (pipeline.Request, error) {
	req, err := pipeline.NewRequest("GET", client.url, nil)
	if err != nil {
		return req, pipeline.NewError(err, "failed to create request")
	}
	params := req.URL.Query()
	if sharesnapshot != nil && len(*sharesnapshot) > 0 {
		params.Set("sharesnapshot", *sharesnapshot)
	}
	if timeout != nil {
		params.Set("timeout", strconv.FormatInt(int64(*timeout), 10))
	}
	params.Set("restype", "directory")
	req.URL.RawQuery = params.Encode()
	req.Header.Set("x-ms-version", ServiceVersion)
	return req, nil
}

// getPropertiesResponder handles the response to the GetProperties request.
func (client directoryClient) getPropertiesResponder(resp pipeline.Response) (pipeline.Response, error) {
	err := validateResponse(resp, http.StatusOK)
	if resp == nil {
		return nil, err
	}
	io.Copy(ioutil.Discard, resp.Response().Body)
	resp.Response().Body.Close()
	return &DirectoryGetPropertiesResponse{rawResponse: resp.Response()}, err
}

// ListFilesAndDirectoriesSegment returns a list of files or directories under the specified share or directory. It
// lists the contents only for a single level of the directory hierarchy.
//
// shareName is the name of the target share. directory is the path of the target directory. prefix is filters the
// results to return only entries whose name begins with the specified prefix. sharesnapshot is the snapshot parameter
// is an opaque DateTime value that, when present, specifies the share snapshot to query. marker is a string value that
// identifies the portion of the list to be returned with the next list operation. The operation returns a marker value
// within the response body if the list returned was not complete. The marker value may then be used in a subsequent
// call to request the next set of list items. The marker value is opaque to the client. maxresults is specifies the
// maximum number of entries to return. If the request does not specify maxresults, or specifies a value greater than
// 5,000, the server will return up to 5,000 items. timeout is the timeout parameter is expressed in seconds. For more
// information, see <a
// href="https://docs.microsoft.com/en-us/rest/api/storageservices/Setting-Timeouts-for-File-Service-Operations?redirectedfrom=MSDN">Setting
// Timeouts for File Service Operations.</a> include is include this parameter to specify one or more datasets to
// include in the response. includeExtendedInfo is include extended information.
func (client directoryClient) ListFilesAndDirectoriesSegment(ctx context.Context, shareName string, directory string, prefix *string, sharesnapshot *string, marker *string, maxresults *int32, timeout *int32, include []ListFilesIncludeType, includeExtendedInfo *bool) (*ListFilesAndDirectoriesSegmentResponse, error) {
	if err := validate([]validation{
		{targetValue: maxresults,
			constraints: []constraint{{target: "maxresults", name: null, rule: false,
				chain: []constraint{{target: "maxresults", name: inclusiveMinimum, rule: 1, chain: nil}}}}},
		{targetValue: timeout,
			constraints: []constraint{{target: "timeout", name: null, rule: false,
				chain: []constraint{{target: "timeout", name: inclusiveMinimum, rule: 0, chain: nil}}}}}}); err != nil {
		return nil, err
	}
	req, err := client.listFilesAndDirectoriesSegmentPreparer(shareName, directory, prefix, sharesnapshot, marker, maxresults, timeout, include, includeExtendedInfo)
	if err != nil {
		return nil, err
	}
	resp, err := client.Pipeline().Do(ctx, responderPolicyFactory{responder: client.listFilesAndDirectoriesSegmentResponder}, req)
	if err != nil {
		return nil, err
	}
	return resp.(*ListFilesAndDirectoriesSegmentResponse), err
}

// listFilesAndDirectoriesSegmentPreparer prepares the ListFilesAndDirectoriesSegment request.
func (client directoryClient) listFilesAndDirectoriesSegmentPreparer(shareName string, directory string, prefix *string, sharesnapshot *string, marker *string, maxresults *int32, timeout *int32, include []ListFilesIncludeType, includeExtendedInfo *bool) (pipeline.Request, error) {
	req, err := pipeline.NewRequest("GET", client.url, nil)
	if err != nil {
		return req, pipeline.NewError(err, "failed to create request")
	}
	params := req.URL.Query()
	if prefix != nil && len(*prefix) > 0 {
		params.Set("prefix", *prefix)
	}
	if sharesnapshot != nil && len(*sharesnapshot) > 0 {
		params.Set("sharesnapshot", *sharesnapshot)
	}
	if marker != nil && len(*marker) > 0 {
		params.Set("marker", *marker)
	}
	if maxresults != nil {
		params.Set("maxresults", strconv.FormatInt(int64(*maxresults), 10))
	}
	if timeout != nil {
		params.Set("timeout", strconv.FormatInt(int64(*timeout), 10))
	}
	if include != nil && len(include) > 0 {
		params.Set("include", joinConst(include, ","))
	}
	params.Set("restype", "directory")
	params.Set("comp", "list")
	req.URL.RawQuery = params.Encode()
	req.Header.Set("x-ms-version", ServiceVersion)
	if includeExtendedInfo != nil {
		req.Header.Set("x-ms-file-extended-info", strconv.FormatBool(*includeExtendedInfo))
	}
	return req, nil
}

// listFilesAndDirectoriesSegmentResponder handles the response to the ListFilesAndDirectoriesSegment request.
func (client directoryClient) listFilesAndDirectoriesSegmentResponder(resp pipeline.Response) (pipeline.Response, error) {
	err := validateResponse(resp, http.StatusOK)
	if resp == nil {
		return nil, err
	}
	result := &ListFilesAndDirectoriesSegmentResponse{rawResponse: resp.Response()}
	if err != nil {
		return result, err
	}
	defer resp.Response().Body.Close()
	b, err := ioutil.ReadAll(resp.Response().Body)
	if err != nil {
		return result, err
	}
	if len(b) > 0 {
		b = removeBOM(b)
		err = xml.Unmarshal(b, result)
		if err != nil {
			return result, NewResponseError(err, resp.Response(), "failed to unmarshal response body")
		}
	}
	return result, nil
}

// ListHandles lists handles for directory.
//
// shareName is the name of the target share. directory is the path of the target directory. marker is a string value
// that identifies the portion of the list to be returned with the next list operation. The operation returns a marker
// value within the response body if the list returned was not complete. The marker value may then be used in a
// subsequent call to request the next set of list items. The marker value is opaque to the client. maxresults is
// specifies the maximum number of entries to return. If the request does not specify maxresults, or specifies a value
// greater than 5,000, the server will return up to 5,000 items. timeout is the timeout parameter is expressed in
// seconds. For more information, see <a
// href="https://docs.microsoft.com/en-us/rest/api/storageservices/Setting-Timeouts-for-File-Service-Operations?redirectedfrom=MSDN">Setting
// Timeouts for File Service Operations.</a> sharesnapshot is the snapshot parameter is an opaque DateTime value that,
// when present, specifies the share snapshot to query. recursive is specifies operation should apply to the directory
// specified in the URI, its files, its subdirectories and their files.
func (client directoryClient) ListHandles(ctx context.Context, shareName string, directory string, marker *string, maxresults *int32, timeout *int32, sharesnapshot *string, recursive *bool) (*ListHandlesResponse, error) {
	if err := validate([]validation{
		{targetValue: maxresults,
			constraints: []constraint{{target: "maxresults", name: null, rule: false,
				chain: []constraint{{target: "maxresults", name: inclusiveMinimum, rule: 1, chain: nil}}}}},
		{targetValue: timeout,
			constraints: []constraint{{target: "timeout", name: null, rule: false,
				chain: []constraint{{target: "timeout", name: inclusiveMinimum, rule: 0, chain: nil}}}}}}); err != nil {
		return nil, err
	}
	req, err := client.listHandlesPreparer(shareName, directory, marker, maxresults, timeout, sharesnapshot, recursive)
	if err != nil {
		return nil, err
	}
	resp, err := client.Pipeline().Do(ctx, responderPolicyFactory{responder: client.listHandlesResponder}, req)
	if err != nil {
		return nil, err
	}
	return resp.(*ListHandlesResponse), err
}

// listHandlesPreparer prepares the ListHandles request.
func (client directoryClient) listHandlesPreparer(shareName string, directory string, marker *string, maxresults *int32, timeout *int32, sharesnapshot *string, recursive *bool) (pipeline.Request, error) {
	req, err := pipeline.NewRequest("GET", client.url, nil)
	if err != nil {
		return req, pipeline.NewError(err, "failed to create request")
	}
	params := req.URL.Query()
	if marker != nil && len(*marker) > 0 {
		params.Set("marker", *marker)
	}
	if maxresults != nil {
		params.Set("maxresults", strconv.FormatInt(int64(*maxresults), 10))
	}
	if timeout != nil {
		params.Set("timeout", strconv.FormatInt(int64(*timeout), 10))
	}
	if sharesnapshot != nil && len(*sharesnapshot) > 0 {
		params.Set("sharesnapshot", *sharesnapshot)
	}
	params.Set("comp", "listhandles")
	req.URL.RawQuery = params.Encode()
	if recursive != nil {
		req.Header.Set("x-ms-recursive", strconv.FormatBool(*recursive))
	}
	req.Header.Set("x-ms-version", ServiceVersion)
	return req, nil
}

// listHandlesResponder handles the response to the ListHandles request.
func (client directoryClient) listHandlesResponder(resp pipeline.Response) (pipeline.Response, error) {
	err := validateResponse(resp, http.StatusOK)
	if resp == nil {
		return nil, err
	}
	result := &ListHandlesResponse{rawResponse: resp.Response()}
	if err != nil {
		return result, err
	}
	defer resp.Response().Body.Close()
	b, err := ioutil.ReadAll(resp.Response().Body)
	if err != nil {
		return result, err
	}
	if len(b) > 0 {
		b = removeBOM(b)
		err = xml.Unmarshal(b, result)
		if err != nil {
			return result, NewResponseError(err, resp.Response(), "failed to unmarshal response body")
		}
	}
	return result, nil
}

// Rename renames a directory
//
// renameSource is required. Specifies the URI-style path of the source file, up to 2 KB in length. shareName is the
// name of the target share. directory is the path of the target directory. timeout is the timeout parameter is
// expressed in seconds. For more information, see <a
// href="https://docs.microsoft.com/en-us/rest/api/storageservices/Setting-Timeouts-for-File-Service-Operations?redirectedfrom=MSDN">Setting
// Timeouts for File Service Operations.</a> replaceIfExists is optional. A boolean value for if the destination file
// already exists, whether this request will overwrite the file or not. If true, the rename will succeed and will
// overwrite the destination file. If not provided or if false and the destination file does exist, the request will
// not overwrite the destination file. If provided and the destination file doesn’t exist, the rename will succeed.
// Note: This value does not override the x-ms-file-copy-ignore-read-only header value. ignoreReadOnly is optional. A
// boolean value that specifies whether the ReadOnly attribute on a preexisting destination file should be respected.
// If true, the rename will succeed, otherwise, a previous file at the destination with the ReadOnly attribute set will
// cause the rename to fail. sourceLeaseID is required if the source file has an active infinite lease.
// destinationLeaseID is required if the destination file has an active infinite lease. The lease ID specified for this
// header must match the lease ID of the destination file. If the request does not include the lease ID or it is not
// valid, the operation fails with status code 412 (Precondition Failed). If this header is specified and the
// destination file does not currently have an active lease, the operation will also fail with status code 412
// (Precondition Failed). fileAttributes is specifies either the option to copy file attributes from a source
// file(source) to a target file or a list of attributes to set on a target file. fileCreationTime is specifies either
// the option to copy file creation time from a source file(source) to a target file or a time value in ISO 8601 format
// to set as creation time on a target file. fileLastWriteTime is specifies either the option to copy file last write
// time from a source file(source) to a target file or a time value in ISO 8601 format to set as last write time on a
// target file. filePermission is if specified the permission (security descriptor) shall be set for the
// directory/file. This header can be used if Permission size is <= 8KB, else x-ms-file-permission-key header shall be
// used. Default value: Inherit. If SDDL is specified as input, it must have owner, group and dacl. Note: Only one of
// the x-ms-file-permission or x-ms-file-permission-key should be specified. filePermissionKey is key of the permission
// to be set for the directory/file. Note: Only one of the x-ms-file-permission or x-ms-file-permission-key should be
// specified. metadata is a name-value pair to associate with a file storage object.
func (client directoryClient) Rename(ctx context.Context, renameSource string, shareName string, directory string, timeout *int32, replaceIfExists *bool, ignoreReadOnly *bool, sourceLeaseID *string, destinationLeaseID *string, fileAttributes *string, fileCreationTime *string, fileLastWriteTime *string, filePermission *string, filePermissionKey *string, metadata map[string]string) (*DirectoryRenameResponse, error) {
	if err := validate([]validation{
		{targetValue: timeout,
			constraints: []constraint{{target: "timeout", name: null, rule: false,
				chain: []constraint{{target: "timeout", name: inclusiveMinimum, rule: 0, chain: nil}}}}}}); err != nil {
		return nil, err
	}
	req, err := client.renamePreparer(renameSource, shareName, directory, timeout, replaceIfExists, ignoreReadOnly, sourceLeaseID, destinationLeaseID, fileAttributes, fileCreationTime, fileLastWriteTime, filePermission, filePermissionKey, metadata)
	if err != nil {
		return nil, err
	}
	resp, err := client.Pipeline().Do(ctx, responderPolicyFactory{responder: client.renameResponder}, req)
	if err != nil {
		return nil, err
	}
	return resp.(*DirectoryRenameResponse), err
}

// renamePreparer prepares the Rename request.
func (client directoryClient) renamePreparer(renameSource string, shareName string, directory string, timeout *int32, replaceIfExists *bool, ignoreReadOnly *bool, sourceLeaseID *string, destinationLeaseID *string, fileAttributes *string, fileCreationTime *string, fileLastWriteTime *string, filePermission *string, filePermissionKey *string, metadata map[string]string) (pipeline.Request, error) {
	req, err := pipeline.NewRequest("PUT", client.url, nil)
	if err != nil {
		return req, pipeline.NewError(err, "failed to create request")
	}
	params := req.URL.Query()
	if timeout != nil {
		params.Set("timeout", strconv.FormatInt(int64(*timeout), 10))
	}
	params.Set("restype", "directory")
	params.Set("comp", "rename")
	req.URL.RawQuery = params.Encode()
	req.Header.Set("x-ms-version", ServiceVersion)
	req.Header.Set("x-ms-file-rename-source", renameSource)
	if replaceIfExists != nil {
		req.Header.Set("x-ms-file-rename-replace-if-exists", strconv.FormatBool(*replaceIfExists))
	}
	if ignoreReadOnly != nil {
		req.Header.Set("x-ms-file-rename-ignore-readonly", strconv.FormatBool(*ignoreReadOnly))
	}
	if sourceLeaseID != nil {
		req.Header.Set("x-ms-source-lease-id", *sourceLeaseID)
	}
	if destinationLeaseID != nil {
		req.Header.Set("x-ms-destination-lease-id", *destinationLeaseID)
	}
	if fileAttributes != nil {
		req.Header.Set("x-ms-file-attributes", *fileAttributes)
	}
	if fileCreationTime != nil {
		req.Header.Set("x-ms-file-creation-time", *fileCreationTime)
	}
	if fileLastWriteTime != nil {
		req.Header.Set("x-ms-file-last-write-time", *fileLastWriteTime)
	}
	if filePermission != nil {
		req.Header.Set("x-ms-file-permission", *filePermission)
	}
	if filePermissionKey != nil {
		req.Header.Set("x-ms-file-permission-key", *filePermissionKey)
	}
	if metadata != nil {
		for k, v := range metadata {
			req.Header.Set("x-ms-meta-"+k, v)
		}
	}
	return req, nil
}

// renameResponder handles the response to the Rename request.
func (client directoryClient) renameResponder(resp pipeline.Response) (pipeline.Response, error) {
	err := validateResponse(resp, http.StatusOK)
	if resp == nil {
		return nil, err
	}
	io.Copy(ioutil.Discard, resp.Response().Body)
	resp.Response().Body.Close()
	return &DirectoryRenameResponse{rawResponse: resp.Response()}, err
}

// SetMetadata updates user defined metadata for the specified directory.
//
// shareName is the name of the target share. directory is the path of the target directory. timeout is the timeout
// parameter is expressed in seconds. For more information, see <a
// href="https://docs.microsoft.com/en-us/rest/api/storageservices/Setting-Timeouts-for-File-Service-Operations?redirectedfrom=MSDN">Setting
// Timeouts for File Service Operations.</a> metadata is a name-value pair to associate with a file storage object.
func (client directoryClient) SetMetadata(ctx context.Context, shareName string, directory string, timeout *int32, metadata map[string]string) (*DirectorySetMetadataResponse, error) {
	if err := validate([]validation{
		{targetValue: timeout,
			constraints: []constraint{{target: "timeout", name: null, rule: false,
				chain: []constraint{{target: "timeout", name: inclusiveMinimum, rule: 0, chain: nil}}}}}}); err != nil {
		return nil, err
	}
	req, err := client.setMetadataPreparer(shareName, directory, timeout, metadata)
	if err != nil {
		return nil, err
	}
	resp, err := client.Pipeline().Do(ctx, responderPolicyFactory{responder: client.setMetadataResponder}, req)
	if err != nil {
		return nil, err
	}
	return resp.(*DirectorySetMetadataResponse), err
}

// setMetadataPreparer prepares the SetMetadata request.
func (client directoryClient) setMetadataPreparer(shareName string, directory string, timeout *int32, metadata map[string]string) (pipeline.Request, error) {
	req, err := pipeline.NewRequest("PUT", client.url, nil)
	if err != nil {
		return req, pipeline.NewError(err, "failed to create request")
	}
	params := req.URL.Query()
	if timeout != nil {
		params.Set("timeout", strconv.FormatInt(int64(*timeout), 10))
	}
	params.Set("restype", "directory")
	params.Set("comp", "metadata")
	req.URL.RawQuery = params.Encode()
	if metadata != nil {
		for k, v := range metadata {
			req.Header.Set("x-ms-meta-"+k, v)
		}
	}
	req.Header.Set("x-ms-version", ServiceVersion)
	return req, nil
}

// setMetadataResponder handles the response to the SetMetadata request.
func (client directoryClient) setMetadataResponder(resp pipeline.Response) (pipeline.Response, error) {
	err := validateResponse(resp, http.StatusOK)
	if resp == nil {
		return nil, err
	}
	io.Copy(ioutil.Discard, resp.Response().Body)
	resp.Response().Body.Close()
	return &DirectorySetMetadataResponse{rawResponse: resp.Response()}, err
}

// SetProperties sets properties on the directory.
//
// fileAttributes is if specified, the provided file attributes shall be set. Default value: ‘Archive’ for file and
// ‘Directory’ for directory. ‘None’ can also be specified as default. fileCreationTime is creation time for the
// file/directory. Default value: Now. fileLastWriteTime is last write time for the file/directory. Default value: Now.
// shareName is the name of the target share. directory is the path of the target directory. timeout is the timeout
// parameter is expressed in seconds. For more information, see <a
// href="https://docs.microsoft.com/en-us/rest/api/storageservices/Setting-Timeouts-for-File-Service-Operations?redirectedfrom=MSDN">Setting
// Timeouts for File Service Operations.</a> filePermission is if specified the permission (security descriptor) shall
// be set for the directory/file. This header can be used if Permission size is <= 8KB, else x-ms-file-permission-key
// header shall be used. Default value: Inherit. If SDDL is specified as input, it must have owner, group and dacl.
// Note: Only one of the x-ms-file-permission or x-ms-file-permission-key should be specified. filePermissionKey is key
// of the permission to be set for the directory/file. Note: Only one of the x-ms-file-permission or
// x-ms-file-permission-key should be specified.
func (client directoryClient) SetProperties(ctx context.Context, fileAttributes string, fileCreationTime string, fileLastWriteTime string, shareName string, directory string, timeout *int32, filePermission *string, filePermissionKey *string) (*DirectorySetPropertiesResponse, error) {
	if err := validate([]validation{
		{targetValue: timeout,
			constraints: []constraint{{target: "timeout", name: null, rule: false,
				chain: []constraint{{target: "timeout", name: inclusiveMinimum, rule: 0, chain: nil}}}}}}); err != nil {
		return nil, err
	}
	req, err := client.setPropertiesPreparer(fileAttributes, fileCreationTime, fileLastWriteTime, shareName, directory, timeout, filePermission, filePermissionKey)
	if err != nil {
		return nil, err
	}
	resp, err := client.Pipeline().Do(ctx, responderPolicyFactory{responder: client.setPropertiesResponder}, req)
	if err != nil {
		return nil, err
	}
	return resp.(*DirectorySetPropertiesResponse), err
}

// setPropertiesPreparer prepares the SetProperties request.
func (client directoryClient) setPropertiesPreparer(fileAttributes string, fileCreationTime string, fileLastWriteTime string, shareName string, directory string, timeout *int32, filePermission *string, filePermissionKey *string) (pipeline.Request, error) {
	req, err := pipeline.NewRequest("PUT", client.url, nil)
	if err != nil {
		return req, pipeline.NewError(err, "failed to create request")
	}
	params := req.URL.Query()
	if timeout != nil {
		params.Set("timeout", strconv.FormatInt(int64(*timeout), 10))
	}
	params.Set("restype", "directory")
	params.Set("comp", "properties")
	req.URL.RawQuery = params.Encode()
	req.Header.Set("x-ms-version", ServiceVersion)
	if filePermission != nil {
		req.Header.Set("x-ms-file-permission", *filePermission)
	}
	if filePermissionKey != nil {
		req.Header.Set("x-ms-file-permission-key", *filePermissionKey)
	}
	req.Header.Set("x-ms-file-attributes", fileAttributes)
	req.Header.Set("x-ms-file-creation-time", fileCreationTime)
	req.Header.Set("x-ms-file-last-write-time", fileLastWriteTime)
	return req, nil
}

// setPropertiesResponder handles the response to the SetProperties request.
func (client directoryClient) setPropertiesResponder(resp pipeline.Response) (pipeline.Response, error) {
	err := validateResponse(resp, http.StatusOK)
	if resp == nil {
		return nil, err
	}
	io.Copy(ioutil.Discard, resp.Response().Body)
	resp.Response().Body.Close()
	return &DirectorySetPropertiesResponse{rawResponse: resp.Response()}, err
}
