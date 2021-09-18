|Branch      | OSX / Linux / Windows |
|------------|-----------------------|
|master      | [![Build Status](https://github.com/DanEngelbrecht/golongtail/workflows/Build%20Master/badge.svg)](https://github.com/DanEngelbrecht/golongtail/workflows/Build%20Master/badge.svg) |

# golongtail

A Go wrapper for [longtail](https://github.com/DanEngelbrecht/longtail), both a module that hides the C interface and a module that provides a simple command line interface for up/down loading content.

The command line tool can upload and download to a GCS or S3 bucket but requires that you are already logged in to gcloud, no authentication code is in place yet. It can also upload/download to a regular folder path.

## Performance numbers
Using a well known Unreal based game comparing longtail with the preferred distribution application for the game. The final installation of the game is 80.6 Gb on disk.

### Hardware specs
Intel Xeon W-2155 CPU @ 3.3 GHz, 10 Cores w hyper threading enabled, 64 Gb RAM, Toshiba XG5 KXG50ZNV1T02 1TB NVMe SSD disk, running Windows 10 with a 1Gb internet connection.

### Install comparision
**Download Size** is the amount of data that needs to be transfered over the network.

**Time** is the time to download and write the final data to disk, including validation of written data (applicable for *Game Store* and *Longtail*).

The Longtail examples accesses data directly from a GCS storage bucket.

|                           |Download Size     |Time     |
|---------------------------|------------------|---------|
|Ideal* Uncompressed        | 80.6 Gb          | 11m 32s |
|Ideal* Game Store          | 39.6 Gb          | 05m 40s |
|Ideal* Longtail            | 33.6 Gb          | 04m 48s |
|Game Store                 | 39.6 Gb          | 15m 45s |
|Longtail w/o cache**       | 33.6 Gb          | 07m 05s |
|Longtail w/ cache**        | 33.6 Gb          | 05m 57s |
|Longtail w/ primed cache** | 0 Gb             | 03m 00s |
|Local copy***              | 0 Gb             | 02m 59s |

\* Ideal time is how fast the data can be downloaded on a 1Gb connection in theory. It does not take into account the time to write data to disk.

\** Longtail uses an (optional) cache to primarily speed up incremental downloads but it also helps with full downloads. The cache size contains the downloaded data so it will in this case end up at 32.1 Gb in size.
- *Longtail w/o cache* = no local cache of downloaded blocks
- *Longtail w/ cache* = local cache of downloaded blocks, starting from empty cache
- *Longtail w/ primed cache* = local cache of downloaded blocks with all block already cached

\*** *Local copy* is the time it takes to copy the installed 80.6 Gb from one location on local disk to a new location.

### Other numbers
Indexing and chunking (finding out chunks and blocks) of the 80.6 Gb takes 35s - this is the time it would take to validate an installation against source data.

Compressing the 80.6 Gb of original data into 32.1 Gb and save to local storage using longtail on listed hardware specifications takes 2m 39s.

Compressing the 80.6 Gb of original data into 32.1 Gb and upload to a GCS storage bucket using longtail on listed hardware specifications takes 6m 50s.

## Cloning
git clone https://github.com/DanEngelbrecht/golongtail.git

## Building
You need Go and gcc installed.

### Windows
Navigate to `cmd\longtail` and run `go build .`.
You should get an `longtail.exe` executable in the cmd\longtail folder.

### Linux
Navigate to `cmd/longtail` and run `go build .` script.
You should get an `longtail` executable in the cmd/longtail folder.

## Usage
Build the command line and run it for a breif description of commands/options.

### Upload to GCS
`longtail.exe upsync --source-path "my_folder" --target-path "gs://test_block_storage/store/index/my_folder.lvi" --storage-uri "gs://test_block_storage/store"`

### Upload to a local folder
`longtail.exe upsync --source-path "my_folder" --target-path "local_store/index/my_folder.lvi" --storage-uri "local_store"`

### Download from S3
`longtail.exe downsync --source-path "s3://test_block_storage/store/index/my_folder.lvi" --target-path "my_folder_copy" --storage-uri "s3://test_block_storage/store" --cache-path "cache"`

### Download from a local folder
`longtail.exe downsync --source-path "local_store/index/my_folder.lvi" --target-path "my_folder_copy" --storage-uri "local_store"`
