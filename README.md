|Branch      | OSX / Linux / Windows |
|------------|-----------------------|
|master      | [![Build Status](https://github.com/DanEngelbrecht/golongtail/workflows/.github/workflows/master-build.yml/badge.svg)](https://github.com/DanEngelbrecht/golongtail/workflows/.github/workflows/master-build.yml/badge.svg) |

# go_longtail

A Go wrapper for [longtail](https://github.com/DanEngelbrecht/longtail), both module that hides the C interface and a module that provides a simple command line interface for up/down loading content.

The command line tool can upload and download to a GCS bucket but requires that you are already logged in to gcloud, no authentication code is in place yet. It can also upload/download to a regular folder path.

## Cloning
git clone https://github.com/DanEngelbrecht/go_longtail.git

## Building
You need Go and gcc installed.

### Windows
Navigate to `cmd` and run the `build.bat` script.
You should get an `longtail.exe` executable in the cmd folder.

### Linux
Navigate to `cmd` and run the `build.sh` script.
You should get an `longtail` executable in the cmd folder.

## Usage
Build the command line and run it for a breif description of commands/options.

### Upload to GCS
`longtail.exe upsync --source-path "my_folder" --target-path "index/my_folder.lvi" --storage-uri "gs://test_block_storage"`

### Upload to a local folder
`longtail.exe upsync --source-path "my_folder" --target-path "index/my_folder.lvi" --storage-uri "c:\test_block_storage"`

### Download from GCS
`longtail.exe downsync --source-path "index/my_folder.lvi" --target-path "my_folder_copy" --storage-uri "gs://test_block_storage"`

### Download from a local folder
`longtail.exe downsync --source-path "index/my_folder.lvi" --target-path "my_folder_copy" --storage-uri "c:\test_block_storage"`
