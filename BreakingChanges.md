# Breaking Changes

> See the [Change Log](ChangeLog.md) for a summary of storage library changes.

## Version 0.9.0:
- Upgraded service version to `2020-02-10`. 
- Added `leaseId` parameter in the function signatures.
- `SetQuota` function has been renamed to `SetProperties` 

## Version 0.4.0:
- Upgraded service version to 2018-03-28. Upgraded to latest protocol layer's models.
- Optimized error reporting and minimized panics. Removed most panics from the library. Several functions now return an error.
- Removed 2017 service version.