|Branch      | OSX / Linux / Windows |
|------------|-----------------------|
|master      | [![Build Status](https://github.com/DanEngelbrecht/golongtail/workflows/Build%20Master/badge.svg)](https://github.com/DanEngelbrecht/golongtail/workflows/Build%20Master/badge.svg) |

# golongtail
Incremental asset delivery tool - using techniques related to the casync project by Lennart Poettering (https://github.com/systemd/casync). It is designed to make efficient diffs between version and allows arbitrary forward and backward patching while keeping the patching size and fast.

This repo is a Go wrapper for [longtail](https://github.com/DanEngelbrecht/longtail), both a module that hides the C interface and a module that provides a simple command line interface for up/down loading content as well as managing a "store".

The command line tool can upload to and download from a GCS or S3 bucket but requires that you are already logged in to gcloud, authentication is handled by the GCS and S3 libraries. It can also upload and download to a regular folder path. It provides a well defined interface so adding new storage options is fairly straightforward - Azure block store is one thing on the to-do list.

The tool provides many options when it comes to both compression and hashing, defaulting to Blake3 for hashing and ZStd for compression, but others can be selected which will trade speed and or size depending on options.

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

## Technical overview
When uploading a folder to GCS, longtail will first do an "indexing" pass on the folder, listing all the files, sizes, and permissions inside. Next step is the chunking part, this is where longtail will try to identify segments of data that are repeated. Think of it as finding redundant files in a folder but on a more granular level - finding duplicates *inside* each file. For each section a hash is calculated which makes it quick to find duplicates.

Then we create the Version Index - a file that contains the "structure" of the input folder - file/folder names, size, etc but also how each file can be put together with a series of chunks that we found in the indexing part. In the version index we only list the chunk hashes to represent the content so the version index is small.

Loaded with this information we can group the chunk data into blocks which we upload to the "store" - a place which can be a GCS, S3 or local disk target. Each block contains one to many chunks and is (optionally) compressed as one piece of data. What we also need to store is information on how you can find *where* a chunk is stored - in which block did we put it? This is stored in the store index - a lookup table where you can go from chunk hash to block hash, and via the block hash you can find the stored block which contains the data needed for a specific chunk.

Next time you upload a new version and the indexing is done we will find chunks that are already present in the store and we will only upload the missing chunks to the store, grouping the chunks in to blocks.

In the downloading scenario, we start by reading a version index, this tells us which chunks we need to rebuild the folder structure. The target folder is also scanned for its content so we can tell excatly which files/folder we need to change. As the version index does not contain the actual data we need the path to a store which we can query which blocks we need that contains the chunks we need.

The folder is then recreated by looking up the data each chunk hash represents and write that to the each file.

The design of putting data into blocks instead of saving each chunk in a separate file (which casync does) gives a couple of benefits. Downloading ten of thousands of individual files from for example GCS is very inefficient and compressing each chunk individually also yields poor compression ratios. By bundling multiple chunks into blocks we reduce the traffic and increase the compression ration. The downside of this is that we need the store index to go from chunk hash to block hash to chunk data.

The store index will grow in size and if you have a lot of data in a single store it can get so big as to start to negate the benefits of the incremental patching. Longtail provides an option where you can create a subset of the store index and save that in a file. When uploading you *always* want to have access to the entire index to make optimal descisions of what to add to the store, but when you download you only care about the lookup information for the content you need. This is where the `version-local-store-index` is useful. You can ask longtail to create it at upload and you can provide it as input when you download to minimize download size.

To minimize downloads even more, longtail has support for a download cache where all downloaded blocks is stored as they are downloaded. As a block can be used by many different assets it is useful to cache them both for an initial download as well as when patching an existing version.

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

### MacOS
Navigate to `cmd/longtail` and run `go build .` script.
You should get an `longtail` executable in the cmd/longtail folder.

## Usage
Build the command line and run it for a breif description of commands/options.

### Upload to GCS
`longtail.exe upsync --source-path "my_folder" --target-path "gs://test_block_storage/store/index/my_folder.lvi" --storage-uri "gs://test_block_storage/store"`

### Download from GCS (Using default target folder name, derived from the --source-path, in this case "my_folder")
`longtail.exe downsync --source-path "gs://test_block_storage/store/index/my_folder.lvi" --storage-uri "gs://test_block_storage/store"`

### Upload to a local folder
`longtail.exe upsync --source-path "my_folder" --target-path "local_store/index/my_folder.lvi" --storage-uri "local_store"`

### Download from S3
`longtail.exe downsync --source-path "s3://test_block_storage/store/index/my_folder.lvi" --target-path "my_folder_copy" --storage-uri "s3://test_block_storage/store" --cache-path "cache"`

### Download from a local folder
`longtail.exe downsync --source-path "local_store/index/my_folder.lvi" --target-path "my_folder_copy" --storage-uri "local_store"`

### Upload to GCS with version local store index and a `get-info` file
`longtail.exe upsync --source-path "my_folder" --target-path "gs://test_block_storage/store/index/my_folder.lvi" --storage-uri "gs://test_block_storage/store" --version-local-store-index-path "gs://test_block_storage/store/index/my_folder.lsi" --get-config-path "gs://test_block_storage/store/index/my_folder.json"`

### Download from GCS using version local store index and a `get-info` file
`longtail.exe get --get-config-path "gs://test_block_storage/store/index/my_folder.json"`

### Download from GCS using version local store index and a `get-info` file with a cache
`longtail.exe get --get-config-path "gs://test_block_storage/store/index/my_folder.json" --cache-path "./download-cache"`
