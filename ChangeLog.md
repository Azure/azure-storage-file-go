# Change Log

> See [BreakingChanges](BreakingChanges.md) for a detailed list of API breaks.

## Version 0.9.0:
- Added support of service version `2020-02-10`. 
    - Please visit [File Service Rest API public documentation](https://docs.microsoft.com/en-us/rest/api/storageservices/file-service-rest-api) to read more about supported operations and updated file/share limits
- Updated the GO version to `1.15` and other dependencies (`azure-pipeline-go`, `sys`, `check.v1`, `pretty`)
- Resolved issue [#27](https://github.com/Azure/azure-storage-file-go/issues/27)
 
## Version 0.8.0:
- Allow more time formats for SAS
- Enable recovering from an unexpectedEOF error

## Version 0.7.0:
- Add support for permissions
- Add support for SMB properties

## Version 0.6.0:
- Added support for UploadRangeFromURL

## Version 0.5.0:
- Align jitter calculations exactly to blob SDK
- General secondary host improvements
- Log error body

## Version 0.4.1:
- Updated module settings.

## Version 0.4.0:
- [Breaking] Upgraded service version to 2018-03-28. Upgraded to latest protocol layer's models.
- [Breaking] Optimized error reporting and minimized panics. Removed most panics from the library. Several functions now return an error.
- [Breaking] Removed 2017 service version.
- Added support for module.
- Added forced retries and optional retry logging to retry reader.
- Fixed the service SAS to support specifying query params to override response headers.
- Fixed an issue that specify type of constant FileMaxSizeInBytes to support 32-bit build.
- Optimized `SASQueryParameters` to support time format YYYY-MM-DDThh:mmZ and YYYY-MM-DD for signedstart and signedexpiry.
- Optimized the mmf related code which switched to x/sys due to deprecation of syscall.