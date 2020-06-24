|Branch      | OSX / Linux / Windows |
|------------|-----------------------|
|master      | [![Build Status](https://github.com/DanEngelbrecht/golongtail/workflows/Build%20Master/badge.svg)](https://github.com/DanEngelbrecht/golongtail/workflows/Build%20Master/badge.svg) |

# go_longtail

A Go wrapper for [longtail](https://github.com/DanEngelbrecht/longtail), both module that hides the C interface and a module that provides a simple command line interface for up/down loading content.

The command line tool can upload and download to a GCS bucket but requires that you are already logged in to gcloud, no authentication code is in place yet. It can also upload/download to a regular folder path.

## Cloning
git clone https://github.com/DanEngelbrecht/golongtail.git

## Building
You need Go and gcc installed.

### Windows
Navigate to `cmd\longtail` and run the `build.bat` script.
You should get an `longtail.exe` executable in the cmd\longtail folder.

### Linux
Navigate to `cmd/longtail` and run the `build.sh` script.
You should get an `longtail` executable in the cmd/longtail folder.

## Usage
Build the command line and run it for a breif description of commands/options.

### Upload to GCS
`longtail.exe upsync --source-path "my_folder" --target-path "gs://test_block_storage/store/index/my_folder.lvi" --storage-uri "gs://test_block_storage/store"`

### Upload to a local folder
`longtail.exe upsync --source-path "my_folder" --target-path "local_store/index/my_folder.lvi" --storage-uri "local_store"`

### Download from GCS
`longtail.exe downsync --source-path "gs://test_block_storage/store/index/my_folder.lvi" --target-path "my_folder_copy" --storage-uri "gs://test_block_storage/store" --cache-path "cache"`

### Download from a local folder
`longtail.exe downsync --source-path "local_store/index/my_folder.lvi" --target-path "my_folder_copy" --storage-uri "local_store"`
