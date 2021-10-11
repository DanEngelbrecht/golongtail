#!/bin/bash

# create version 1

if [ -d "./test" ]; then rm -rf ./test; fi

mkdir -p test/version/v1
touch test/version/v1/empty-file
echo "this is a test file" > test/version/v1/abitoftext.txt
mkdir -p test/version/v1/folder
echo "this is a test file in a subfolder" > test/version/v1/folder/abitoftextinasubfolder.txt
echo "this is a second test file in a subfolder" > test/version/v1/folder/anotherabitoftextinasubfolder.txt

cp -r test/version/v1 test/version/v2
echo "we have some stuff" > test/version/v2/stuff.txt
mkdir -p test/version/v2/folder2
echo "and some more text that we need" > test/version/v2/folder2/anotherabitoftextinasubfolder2.txt
rm test/version/v2/folder/anotherabitoftextinasubfolder.txt

cp -r test/version/v1 test/version/v3
echo "we have some stuff" > test/version/v2/stuff.txt
echo "we have some more stuff" > test/version/v2/morestuff.txt
mkdir -p test/version/v2/folder2
echo "and some more text that we need" > test/version/v2/folder2/anotherabitoftextinasubfolder2.txt
mv test/version/v2/folder/abitoftextinasubfolder.txt test/version/v2/folder/abitoftextmvinasubfolder.txt

./longtail.exe upsync --source-path ./test/version/v1 --target-path ./test/index/v1.lvi --storage-uri fsblob://test/storage
./longtail.exe upsync --source-path ./test/version/v2 --target-path ./test/index/v2.lvi --storage-uri fsblob://test/storage
./longtail.exe upsync --source-path ./test/version/v3 --target-path ./test/index/v3.lvi --storage-uri fsblob://test/storage

./longtail.exe downsync --source-path ./test/index/v1.lvi --target-path ./test/current --storage-uri fsblob://test/storage
./longtail.exe downsync --source-path ./test/index/v2.lvi --target-path ./test/current --storage-uri fsblob://test/storage
./longtail.exe downsync --source-path ./test/index/v3.lvi --target-path ./test/current --storage-uri fsblob://test/storage
./longtail.exe downsync --source-path ./test/index/v2.lvi --target-path ./test/current --storage-uri fsblob://test/storage
./longtail.exe downsync --source-path ./test/index/v1.lvi --target-path ./test/current --storage-uri fsblob://test/storage

./longtail.exe ls --version-index-path ./test/index/v1.lvi .

mkdir -p ./test/cp

./longtail.exe cp --version-index-path ./test/index/v1.lvi folder/anotherabitoftextinasubfolder.txt --storage-uri fsblob://test/storage ./test/cp/anotherabitoftextinasubfolder.txt
./longtail.exe cp --version-index-path ./test/index/v2.lvi folder2/anotherabitoftextinasubfolder2.txt --storage-uri fsblob://test/storage ./test/cp/anotherabitoftextinasubfolder2.txt

rm -rf ./test/index
rm -rf fsblob://test/storage
rm -rf ./test/current
rm -rf ./test/cp

./longtail.exe upsync --source-path ./test/version/v1 --target-path ./test/index/v1.lvi --storage-uri fsblob://test/storage --get-config-path ./test/index/v1.json
./longtail.exe upsync --source-path ./test/version/v2 --target-path ./test/index/v2.lvi --storage-uri fsblob://test/storage --get-config-path ./test/index/v2.json
./longtail.exe upsync --source-path ./test/version/v3 --target-path ./test/index/v3.lvi --storage-uri fsblob://test/storage --get-config-path ./test/index/v3.json

./longtail.exe validate-version --version-index-path ./test/index/v1.lvi --storage-uri fsblob://test/storage
./longtail.exe validate-version --version-index-path ./test/index/v2.lvi --storage-uri fsblob://test/storage
./longtail.exe validate-version --version-index-path ./test/index/v3.lvi --storage-uri fsblob://test/storage

./longtail.exe get --get-config-path ./test/index/v1.json --target-path ./test/current
./longtail.exe get --get-config-path ./test/index/v2.json --target-path ./test/current
./longtail.exe get --get-config-path ./test/index/v3.json --target-path ./test/current
./longtail.exe get --get-config-path ./test/index/v2.json --target-path ./test/current
./longtail.exe get --get-config-path ./test/index/v1.json --target-path ./test/current

./longtail.exe create-version-store-index --source-path ./test/index/v1.lvi --storage-uri fsblob://test/storage --version-local-store-index-path ./test/index/v1.lsi
./longtail.exe create-version-store-index --source-path ./test/index/v2.lvi --storage-uri fsblob://test/storage --version-local-store-index-path ./test/index/v2.lsi
./longtail.exe create-version-store-index --source-path ./test/index/v3.lvi --storage-uri fsblob://test/storage --version-local-store-index-path ./test/index/v3.lsi

./longtail.exe downsync --source-path ./test/index/v3.lvi --target-path ./test/current --storage-uri fsblob://test/storage --version-local-store-index-path ./test/index/v3.lsi
./longtail.exe downsync --source-path ./test/index/v2.lvi --target-path ./test/current --storage-uri fsblob://test/storage --version-local-store-index-path ./test/index/v2.lsi
./longtail.exe downsync --source-path ./test/index/v1.lvi --target-path ./test/current --storage-uri fsblob://test/storage --version-local-store-index-path ./test/index/v1.lsi

./longtail.exe dump-version-assets --version-index-path ./test/index/v1.lvi
./longtail.exe dump-version-assets --version-index-path ./test/index/v2.lvi
./longtail.exe dump-version-assets --version-index-path ./test/index/v3.lvi

./longtail.exe dump-version-assets --version-index-path ./test/index/v1.lvi --details
./longtail.exe dump-version-assets --version-index-path ./test/index/v2.lvi --details
./longtail.exe dump-version-assets --version-index-path ./test/index/v3.lvi --details

rm ./test/storage/store.*

./longtail.exe init-remote-store --storage-uri fsblob://test/storage

./longtail.exe validate-version --version-index-path ./test/index/v1.lvi --storage-uri fsblob://test/storage
./longtail.exe validate-version --version-index-path ./test/index/v2.lvi --storage-uri fsblob://test/storage
./longtail.exe validate-version --version-index-path ./test/index/v3.lvi --storage-uri fsblob://test/storage

./longtail.exe print-store --store-index-path fsblob://test/storage/store.lsi
./longtail.exe print-store --store-index-path fsblob://test/storage/store.lsi --details
./longtail.exe print-store --store-index-path fsblob://test/storage/store.lsi --compact
./longtail.exe print-store --store-index-path fsblob://test/storage/store.lsi --compact --details

./longtail.exe print-version --version-index-path ./test/index/v1.lvi
./longtail.exe print-version --version-index-path ./test/index/v1.lvi --compact

./longtail.exe print-version-usage --version-index-path ./test/index/v1.lvi --storage-uri fsblob://test/storage

echo ./test/index/v1.lvi >sources.txt
./longtail.exe prune-store --source-paths sources.txt --storage-uri fsblob://test/storage
rm -rf ./test/current
./longtail.exe downsync --source-path ./test/index/v1.lvi --target-path ./test/current --storage-uri fsblob://test/storage

./longtail.exe upsync --source-path ./test/version/v2 --target-path ./test/index/v2.lvi --storage-uri fsblob://test/storage
./longtail.exe upsync --source-path ./test/version/v3 --target-path ./test/index/v3.lvi --storage-uri fsblob://test/storage

echo ./test/index/v2.lvi >sources.txt
echo ./test/index-clone/v2.lvi >targets.txt
./longtail.exe clone-store --target-path ./test/current --source-paths sources.txt --target-paths targets.txt --source-storage-uri fsblob://test/storage --target-storage-uri fsblob://test/storage-clone

rm -rf ./test/current
./longtail.exe downsync --source-path ./test/index-clone/v2.lvi --target-path ./test/current --storage-uri fsblob://test/storage-clone --version-local-store-index-path ./test/index/v3.lsi

rm -rf ./test/index-clone
rm -rf fsblob://test/storage-clone

exit 0

# BROKEN! Fails due to missing zip file?
echo $'./test/index/v2.lvi\n./test/index/v1.lvi' >sources.txt
echo $'./test/index-clone/v2.lvi\n./test/index-clone/v1.lvi' >targets.txt
./longtail.exe clone-store --target-path ./test/current --source-paths sources.txt --target-paths targets.txt --source-storage-uri fsblob://test/storage --target-storage-uri fsblob://test/storage-clone --create-version-local-store-index
./longtail.exe downsync --source-path ./test/index-clone/v2.lvi --target-path ./test/current --storage-uri fsblob://test/storage-clone --version-local-store-index-path ./test/index-clone/v3.lsi
