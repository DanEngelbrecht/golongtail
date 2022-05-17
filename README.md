|Branch      | OSX / Linux / Windows |
|------------|-----------------------|
|master      | [![Build Status](https://github.com/DanEngelbrecht/golongtail/workflows/Build%20Master/badge.svg)](https://github.com/DanEngelbrecht/golongtail/workflows/Build%20Master/badge.svg) |

# golongtail
Incremental asset delivery tool - using techniques related to the casync project by Lennart Poettering (https://github.com/systemd/casync). It is designed to make efficient diffs between version and allows arbitrary forward and backward patching while keeping the patching size small and fast.

This repo is a Go wrapper for [longtail](https://github.com/DanEngelbrecht/longtail), both a module that hides the C interface and a module that provides a simple command line interface for up/down loading content as well as managing a "store".

The command line tool can upload to and download from a GCS or S3 bucket but requires that you are already logged in, authentication is handled by the GCS and S3 libraries. It can also upload and download to a regular folder path. It provides a well defined interface so adding new storage options is fairly straightforward - Azure block store is one thing on the to-do list.

The tool provides many options when it comes to both compression and hashing, defaulting to Blake3 for hashing and ZStd for compression, but others can be selected which will trade speed and or size depending on options.

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

## Performance numbers
Using a well known Unreal based game comparing longtail with the preferred distribution application for the game. The final installation of the game is 80.6 Gb on disk.

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

### Hardware specs
Intel Xeon W-2155 CPU @ 3.3 GHz, 10 Cores w hyper threading enabled, 64 Gb RAM, Toshiba XG5 KXG50ZNV1T02 1TB NVMe SSD disk, running Windows 10 with a 1Gb internet connection.

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

## Self contained archives
Longtail can operate in the standard store way which is focused on creating minimal diffs between two versions, it does a good job of keeping track of earlier content and just let you download/upload the new parts of a version. Some data is not suited to try and do a delta as it would make the store grow very fast and the gains would disappear.

Longtail can also create self-contained archive which contains the version index, store index and the blocks all in one file. It provides and alternative to a zip file with the added benefit of being able to apply changes to an existing folder doing minimal work. Pack and unpack speed is also good as is the compression rates.

Example for an archive containing Unreal Editor 5.0 for Windows.

|                           | Mode                | Size       |Pack Time  |Unpack Time  |
|---------------------------|---------------------|------------|-----------|-------------|
|Raw                        |Copy-Object -Recurse | 55,1 Gb    |  2m 57s   | 2m 57s      |
|robocopy                   |/MIR /MT12           | 55,1 Gb    |  1m 04s   | 1m 04s      |
|7-Zip                      |LZMA2 ultra          | 3.26 Gb    | 12m 52s   | 5m 20s      |
|7-Zip 21.07                |LZMA2 level 5        | 13,4 Gb    | 25m 25s   | 2m 42s      |
|7-Zip 21.07                |LZMA2 level 2        | 15,5 Gb    |  8m 55s   | 2m 43s      |
|7-Zip 21.07                |Deflate level 5      | 19,3 Gb    | 18m 24s   | 0m 35s      |
|Longtail                   |zstd level 3         | 15.0 Gb    |  0m 51s   | 0m 58s      |

### Hardware specs
AMD Ryzen 9 5900X CPU @ 4.6 GHz, 6 Core SMT (Precision boost mode), 64 Gb RAM, Samsung 980 PRO M.2 SSD disk.

## Technical overview
When uploading a folder to a store (GCS, S3 or file system folder), longtail will first do an "indexing" pass on the folder, listing all the files, sizes, and permissions inside. Next step is the chunking part, this is where longtail will try to identify segments of data that are repeated. Think of it as finding redundant files in a folder but on a more granular level - finding duplicates *inside* each file. For each section a hash is calculated which makes it quick to find duplicates.

Then we create the Version Index - a file that contains the "structure" of the input folder - file/folder names, size, etc but also how each file can be put together with a series of chunks that we found in the indexing part. In the version index we only list the chunk hashes to represent the content so the version index is small.

Loaded with this information we can group the chunk data into blocks which we upload to the "store" - a place which can be a GCS, S3 or local disk target. Each block contains one to many chunks and is (optionally) compressed as one piece of data. What we also need to store is information on how you can find *where* a chunk is stored - in which block did we put it? This is stored in the store index - a lookup table where you can go from chunk hash to block hash, and via the block hash you can find the stored block which contains the data needed for a specific chunk.

Next time you upload a new version and the indexing is done we will find chunks that are already present in the store and we will only upload the missing chunks to the store, grouping the chunks in to blocks.

In the downloading scenario, we start by reading a version index, this tells us which chunks we need to rebuild the folder structure. The target folder is also scanned for its content so we can tell excatly which files/folder we need to change. As the version index does not contain the actual data we need the path to a store which we can query which blocks we need that contains the chunks we need.

The folder is then recreated by looking up the data each chunk hash represents and write that to the each file.

The design of putting data into blocks instead of saving each chunk in a separate file (which casync does) gives a couple of benefits. Downloading ten of thousands of individual files from for example GCS is very inefficient and compressing each chunk individually also yields poor compression ratios. By bundling multiple chunks into blocks we reduce the traffic and increase the compression ration. The downside of this is that we need the store index to go from chunk hash to block hash to chunk data.

The store index will grow in size and if you have a lot of data in a single store it can get so big as to start to negate the benefits of the incremental patching. Longtail provides an option where you can create a subset of the store index and save that in a file. When uploading you *always* want to have access to the entire index to make optimal descisions of what to add to the store, but when you download you only care about the lookup information for the content you need. This is where the `version-local-store-index` is useful. You can ask longtail to create it at upload and you can provide it as input when you download to minimize download size.

To minimize downloads even more, longtail has support for a download cache where all downloaded blocks is stored as they are downloaded. As a block can be used by many different assets it is useful to cache them both for an initial download as well as when patching an existing version.

## Usage
Build the command line and run it using `longtail --help` for a breif description of commands/options.

## Guidelines for uploading and downloading from a delta store

### Structure

To `upsync` you should provide a `--storage-uri` - this is where the data is stored for all versions including an index over all the data, an example for GCS would be `gs://my_bucket/store` and for S3 `s3://my_bucket/store` which creates a folder in `my_bucket`.
The bucket must already be created and have proper access rights - for upload you need to be able to create objects, read and write to them, list objects and read/write meta-data.

You also need to provide a `--target-path` which is where the version index is stored. I recommend that you put that in a separate folder in your bucket.

Optionally (and recommended) you can add a `--version-local-store-index-path` to store a version local store index which is a subset to the full store index required to fullfill this version only. By using that you do not need to download the full store index when you later `downsync` a version.

To reduce the number of parameters to give and keep track on you can use the `get` and `put` commands which let you specify the path to a json file which includes the path to the store, version index and optional version local store index. This command also lets you skip the path to the version index and version local store index and will automatically set them up based on the target name (the json config file).

An example of the default structure using the `put` command:
```
[bucket]
    store
        store.lsi
        chunks
            0000
                0000a9c8s8a771242.lsb
                ...
            ...
    version-store-index
        v1.lsi
        v2.lsi
        ...
    v1.lvi
    v2.lvi
    ...
```

To achive this structure in GCS the syntax would be:
`longtail upsync --source-path v1 --target-path gs://bucket/v1.lvi --version-local-store-index-path gs://bucket/version-store-index/v1.lsi --storage-uri gs://bucket/store`

To `downsync` this you would issue:
`longtail downsync --target-path v1 --source-path gs://bucket/v1.lvi --version-local-store-index-path gs://bucket/version-store-index/v1.lsi --storage-uri gs://bucket/store`

Quite the long command lines, but you will likely automate this rather than do it manually.

You can simplify the syntax by using the `put` and `get` commands:

The default structure for `put`:
```
[bucket]
    store
        store.lsi
        chunks
            0000
                0000a9c8s8a771242.lsb
                ...
            ...
    version-data
        version-index
            v1.lvi
            v2.lvi
            ...
        version-store-index
            v1.lsi
            v2.lsi
            ...
    v1.json
    v2.json
    ...
```

`longtail put --source-path v1 --target-path gs://bucket/v1.json`
`longtail get --source-path gs://bucket/v1.json`

The `--target-path` is optional for `get` and longtail will deduce the target folder name from the `source-path` path, in this case it will be a folder called `v1` in the current directory.

You can still use the `put` command and override the `--storage-uri`, `target-version-index-path` and `version-local-store-index-path` if you want a different structure.

### Caching data between versions
By default longtail `downsync` or `get` does not cache any downloaded blocks from the store. This works fairly well since it will only download data for the files that needs to be modified/added, but if you care about download size you do want to create a cache of blocks to use between versions.

You do this using the `--cache-path` option, like this:
`longtail get --source-path gs://bucket/versions/v1.json --cache-path /tmp/longtail-cache`

The cache will save every block that you download from the store and will use blocks from the cache as a primary source of data.

When updating a version it first figures out which files that needs to be re-written and which needs to be added. From that it has the data it needs and it first queries the local cache for that data and only for the remaining data it goes to the remote store. That data set is usually very small and reduces the amount of data to download. And reading blocks from local disk is also much faster.

The cache is not managed by longtail and it will only get bigger and new blocks added over time so if you want to keep it in check for size you need to do that yourself. The good news is that just wiping the cache at times works pretty well. There are tools for purging data from a store (the cache is a store itself) but they are a bit complex to work with at this time.

### Sharing cache between machines
If you are using cloud instances that download using longtail it is possible to set up a file share for the cache so all instances benefits from the caching. Longtail manages the locking and sharing of the cache making this safe. Make sure the path to the cache is a fast location and not for example an SMB mapping as that will likely cause bad performance. The cache should behave like a local disk in terms of performance.

### Reducing redundant download data
By default longtail will try and do a minimal delta between what is present in the store and the version you are doing upsync for. This is great if you download once and then follow every single incremental version without skipping any. However since we only do the minimal delta if you skip versions you will get blocks that contain less relevant data and the first download will be unnecessarily big and negate the benefits of longtail over downloading a complete version.

To reduce that you can use the `--min-block-usage-percent` option. This is used with `upsync` and determines which blocks to reuse from the store. If it turns out you versions finds a single chunk in a block it will by default try to use it and getting that chunk would result in downloading a lot of redundant data.

Setting `--min-block-usage-percent` to a higher value, say 80, would mean that at most you would get 20% of redundant data while fetching a block. Setting it to 100 would mean that you do not want any redundant data in a block. Putting a limit to which blocks to use will mean that new blocks will be created even though the chunk you need already exists in the store. This is not a problem for the store as a chunk can exists in multiple blocks.

You might think that setting this to 100% would be optimal, but then you will get no reuse of older blocks that you might already have cached so you need to make a reasonable compromise here.

From my experience when longtail have been used to download Unreal editors and Unreal-based games 80 (as in 80% relevant data in each block) is a good compromise which leads to small deltas and not to much redundant data.

When longtail downloads data from a store it optimizes which blocks to use if it find a chunk in multiple blocks to pick the blocks with the least amount of redundant data.

## GCS

### Authenticating
Either install the gcloud SDK and use `gcloud auth login` or follow the instructions here to set up an authentication key and environment variable to use: https://cloud.google.com/storage/docs/reference/libraries#windows.

Longtail uses the versioning metadata to syncronize writes to the store index to allow multiple clients to upload and download to the same store in parallell.

## S3

### Authenticating
The s3 storage uses the default aws config file to get authentication keys - it reads the file `~/.aws/config` for details, check the AWS documentation on how to download such a key file.

Longtail uses a mechanism of writing partial store indexes and merging them on the fly to allow multiple clients to upload and download to the same store in parallell.

## Usage Examples

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

### Upload to GCS with version local store index and a `get-config` file
`longtail.exe put --source-path "my_folder" --target-path "gs://test_block_storage/store/index/my_folder.json"`

### Download from GCS using version local store index and a `get-config` file into default folder `my_folder`
`longtail.exe get --source-path "gs://test_block_storage/store/index/my_folder.json"`

### Download from GCS using version local store index and a `get-config` file with a cache into custom folder `download/target_folder`
`longtail.exe get --source-path "gs://test_block_storage/store/index/my_folder.json" --cache-path "./download-cache" --target-path "download/target_folder"`

### Create a self-containing archive from a folder naming the archive using the source name
`longtail.exe pack --source-path "stuff/my_folder"`

### Create a self-containing archive from a folder with an explicit target file
`longtail.exe pack --source-path "stuff/my_folder" --target-path "my_folder.la"`

### Unpacking a self-contained archive to a folder naming the target folder from the source archive name
`longtail.exe unpack --source-path "my_folder.la"`

### Unpacking a self-contained archive to a folder without deleting files not in the archive
`longtail.exe unpack --source-path "my_folder.la" --target-path "merged_folder" --no-scan-target`
