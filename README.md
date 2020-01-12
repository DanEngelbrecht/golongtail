|Branch      | OSX / Linux / Windows |
|------------|-----------------------|
|master      | [![Build Status](https://travis-ci.org/DanEngelbrecht/go_longtail.svg?branch=master)](https://travis-ci.org/DanEngelbrecht/go_longtail?branch=master) |

# go_longtail

A Go wrapper for [longtail][https://github.com/DanEngelbrecht/longtail], both module that hides the C interface and a module that provides a simple command line interface for up/down loading content.

The command line tool can upload and download to a GCS bucket but requires that you are already logged in to gcloud, no authentication code is in place yet.

## Cloning
git clone https://github.com/DanEngelbrecht/go_longtail.git

## Building
You need Go and gcc installed.

### Windows
Navigate to `longtail\import` and run the `build_lib.bat` script to create a library of the C code.
Navigate to `com` and run `go build .` and you should get an `cmd.exe` executable.

### Linux
Navigate to `longtail/import` and run the `build_lib.sh` script to create a library of the C code.
Navigate to `com` and run `go build .` and you should get an `cmd.exe` executable.

## Usage
`cmd.exe upsync --source-path "my_folder" --target-path "index/my_folder.lvi" --storage-uri "gs://test_block_storage"`

`cmd.exe downsync --source-path "index/my_folder.lvi" --target-path "my_folder_copy" --storage-uri "gs://test_block_storage"`
