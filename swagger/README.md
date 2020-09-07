# Azure File Storage for Golang

> see https://aka.ms/autorest

### Generation
```bash
cd swagger
autorest README.md --use=@microsoft.azure/autorest.go@v3.0.63
gofmt -w Go_FileStorage/*
```

More modifications have to be made after generation in order to fix issues that the Go generator can't work around right now. Namely:
- Under shareClient.getPermissionResponder and shareClient.createPermissionPreparer, change all xml.Marshal and xml.Unmarshal lines to json.Marshal and json.Unmarshal respectively
    - (Issue opened: https://github.com/Azure/go-autorest/issues/495)

### Settings
``` yaml
input-file: https://raw.githubusercontent.com/Azure/azure-rest-api-specs/storage-dataplane-preview/specification/storage/data-plane/Microsoft.FileStorage/preview/2019-02-02/file.json
go: true
output-folder: Go_FileStorage
namespace: azfile
go-export-clients: false
enable-xml: true
file-prefix: zz_generated_
```

### ShareUsageBytes should be uint64
``` yaml
directive:
- from: swagger-document
  where: $.definitions.ShareStats.properties.ShareUsageBytes
  transform: >
    $.format = "uint64"
```

### Note: the following directives were copied over from Python
### The dates should be string instead
``` yaml
directive:
- from: swagger-document
  where: $["x-ms-paths"]..responses..headers["x-ms-file-last-write-time"]
  transform: >
    $.format = "str";
- from: swagger-document
  where: $["x-ms-paths"]..responses..headers["x-ms-file-change-time"]
  transform: >
    $.format = "str";
- from: swagger-document
  where: $["x-ms-paths"]..responses..headers["x-ms-file-creation-time"]
  transform: >
    $.format = "str";
```

### Change new SMB file parameters to use default values
``` yaml
directive:
- from: swagger-document
  where: $.parameters.FileCreationTime
  transform: >
    $.format = "str";
    $.default = "now";
- from: swagger-document
  where: $.parameters.FileLastWriteTime
  transform: >
    $.format = "str";
    $.default = "now";
- from: swagger-document
  where: $.parameters.FileAttributes
  transform: >
    $.default = "none";
- from: swagger-document
  where: $.parameters.FilePermission
  transform: >
    $.default = "inherit";
```

### FileRangeWriteFromUrl Constant
This value is supposed to be the constant value update and these changes turn it from a parameter into a constant.
``` yaml
directive:
- from: swagger-document
  where: $.parameters.FileRangeWriteFromUrl
  transform: >
    delete $.default;
    delete $["x-ms-enum"];
    $["x-ms-parameter-location"] = "method";
```

### TODO: Get rid of StorageError since we define it
### attempt didn't work

### TODO: Sort out the duplicated definitions related to listing
### clarify the purpose first