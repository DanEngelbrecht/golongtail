##
- **UPDATED** Updated longtail to 0.3.7-pre0

## v0.3.6
- **CHANGED** commands now logs input details at `info` level
- **CHANGED** Improved logging in remotestore with `info` level
- **CHANGED** Stats output is now printed to StdOut and with formatted logging to log file if `--log-file-path` is enabled
- **CHANGED** All logging now goes through logrus with the default logrus text formatting
- **CHANGED** Progress output in console now goes to StdOut instead of StdErr
- **CHANGED** Add NativeBuffer to avoid copying of bytes to Golang array and remove signed 32-bit integer length of arrays (`WriteStoredBlockToBuffer`, `WriteBlockIndexToBuffer`, `WriteVersionIndexToBuffer`, `WriteStoreIndexToBuffer`)
- **CHANGED** `--min-block-usage-percent` now defaults to 80 to balance download size vs patch size
- **ADDED** `--log-to-console` option, default is on, disable all logging output in console with `--no-log-to-console`
- **ADDED** `--log-file-path` option, default is no log file output, add path to json formatted log file
- **ADDED** `--log-coloring` option, enables colored logging output in console, default is non-colored
- **ADDED** `--log-console-timestamp` option, enabled timestamps in the console log, default is not to include time stamp
- **FIXED** Improved retrylogic when writing stored block with better logging details
- **FIXED** Full support for windows extended length paths (fixes: UNC path may not contain forward slashes (#214))
- **FIXED** Corrected some function names logging
- **FIXED** splitURI handles mixed forward and backward slash better
- **FIXED** Reduced memory consumption when doing downsync/get of version
- **FIXED** `put` with `--target-path` without folder in path (local or absolute) now writes to current folder instead of root
- **UPDATED** Updated longtail to 0.3.6

## v0.3.5
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
