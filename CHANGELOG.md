##
- **UPDATED** Updated longtail to 0.3.4

## v0.3.4
- **ADDED** new `--s3-endpoint-resolver-uri` for use with s3 storage/targets to set endpoint resolver URI
- **UPDATED** Update all Go dependencies to latest version

## v0.3.3
- **FIX** Simplified release workflow
- **CHANGED** Read CHANGELOG.md when creating a release
- **CHANGED** Automatically detect pre-release base on tag name (-preX suffix)

## v0.3.3
- **CHANGED** set block extension to ".lsb" for fsblockstore in remote stores
- **UPDATED** Updated longtail to 0.3.3

## v0.3.2
- **ADDED** `put` commmand to complement `get` commmand
- **CHANGED** `--get-config-path` option has been removed from `upsync`; use `put` instead.
- **CHANGED** `--get-config-path` option for `get` renamed to `--source-path`
- **FIXED** Automatic resolving of `--target-path` for `downsync` and `get` now resolves to a folder in the local directory
- **UPDATED** Updated longtail to 0.3.2
