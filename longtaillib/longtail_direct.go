package longtaillib

import (
	"encoding/binary"
	"sort"
)

const (
	// CurrentStoreIndexVersion
	CurrentStoreIndexVersion = (uint32(1) << 24) | (uint32(0) << 16) | (uint32(0) << 8) | uint32(0)
)

type StoreIndex struct {
	Version            uint32
	HashIdentifier     uint32
	BlockHashes        []uint64
	ChunkHashes        []uint64
	BlockChunksOffsets []uint32
	BlockChunkCounts   []uint32
	BlockTags          []uint32
	ChunkSizes         []uint32
}

// BytesTo64BitArray ...
func BytesTo64BitArray(bytes []byte, offset int, count int) []uint64 {
	result := make([]uint64, count)
	for b := 0; b < count; b++ {
		result[b] = uint64(binary.LittleEndian.Uint64(bytes[offset+b*8 : offset+(b+1)*8]))
	}
	return result
}

// BytesTo32BitArray ...
func BytesTo32BitArray(bytes []byte, offset int, count int) []uint32 {
	result := make([]uint32, count)
	for b := 0; b < count; b++ {
		result[b] = uint32(binary.LittleEndian.Uint32(bytes[offset+b*4 : offset+(b+1)*4]))
	}
	return result
}

const (
	versionOffset        = 0
	hashIdentifierOffset = versionOffset + 4
	blockCountOffset     = hashIdentifierOffset + 4
	chunkCountOffset     = blockCountOffset + 4
	headerSize           = chunkCountOffset + 4
)

// DirectGetStoreIndexSize ...
func DirectGetStoreIndexSize(blockCount int, chunkCount int) int {
	return int(4 + 4 + 4 + 4 + blockCount*8 + chunkCount*8 + blockCount*4 + blockCount*4 + blockCount*4 + chunkCount*4)
}

// ReadStoreIndexHeader ...
func DirectReadStoreIndex(rawBytes []byte) *StoreIndex {
	index := &StoreIndex{}
	index.Version = uint32(binary.LittleEndian.Uint32(rawBytes[versionOffset : versionOffset+4]))
	index.HashIdentifier = uint32(binary.LittleEndian.Uint32(rawBytes[hashIdentifierOffset : hashIdentifierOffset+4]))
	blockCount := int(binary.LittleEndian.Uint32(rawBytes[blockCountOffset : blockCountOffset+4]))
	chunkCount := int(binary.LittleEndian.Uint32(rawBytes[chunkCountOffset : chunkCountOffset+4]))
	if len(rawBytes) < DirectGetStoreIndexSize(blockCount, chunkCount) {
		return nil
	}

	offset := headerSize
	index.BlockHashes = BytesTo64BitArray(rawBytes, offset, blockCount)
	offset += blockCount * 8
	index.ChunkHashes = BytesTo64BitArray(rawBytes, offset, chunkCount)
	offset += chunkCount * 8
	index.BlockChunksOffsets = BytesTo32BitArray(rawBytes, offset, blockCount)
	offset += blockCount * 4
	index.BlockChunkCounts = BytesTo32BitArray(rawBytes, offset, blockCount)
	offset += blockCount * 4
	index.BlockTags = BytesTo32BitArray(rawBytes, offset, blockCount)
	offset += blockCount * 4
	index.ChunkSizes = BytesTo32BitArray(rawBytes, offset, chunkCount)
	return index
}

func bytesFrom64BitArray(values []uint64, offset int, output []byte) {
	for b := 0; b < len(values); b++ {
		binary.LittleEndian.PutUint64(output[offset+b*8:offset+(b+1)*8], values[b])
	}
}

func bytesFrom32BitArray(values []uint32, offset int, output []byte) {
	for b := 0; b < len(values); b++ {
		binary.LittleEndian.PutUint32(output[offset+b*4:offset+(b+1)*4], values[b])
	}
}

// DirectWriteStoreIndex ...
func DirectWriteStoreIndex(storeIndex *StoreIndex) []byte {
	storeIndexSize := DirectGetStoreIndexSize(len(storeIndex.BlockHashes), len(storeIndex.ChunkHashes))
	result := make([]byte, storeIndexSize)
	binary.LittleEndian.PutUint32(result[versionOffset:versionOffset+4], storeIndex.Version)
	binary.LittleEndian.PutUint32(result[hashIdentifierOffset:hashIdentifierOffset+4], storeIndex.HashIdentifier)
	binary.LittleEndian.PutUint32(result[blockCountOffset:blockCountOffset+4], uint32(len(storeIndex.BlockHashes)))
	binary.LittleEndian.PutUint32(result[chunkCountOffset:chunkCountOffset+4], uint32(len(storeIndex.ChunkHashes)))
	blockCount := len(storeIndex.BlockHashes)
	chunkCount := len(storeIndex.ChunkHashes)

	offset := headerSize
	bytesFrom64BitArray(storeIndex.BlockHashes, offset, result)
	offset += blockCount * 8
	bytesFrom64BitArray(storeIndex.ChunkHashes, offset, result)
	offset += chunkCount * 8
	bytesFrom32BitArray(storeIndex.BlockChunksOffsets, offset, result)
	offset += blockCount * 4
	bytesFrom32BitArray(storeIndex.BlockChunkCounts, offset, result)
	offset += blockCount * 4
	bytesFrom32BitArray(storeIndex.BlockTags, offset, result)
	offset += blockCount * 4
	bytesFrom32BitArray(storeIndex.ChunkSizes, offset, result)
	offset += chunkCount * 4

	return result
}

// DirectGetExistingStoreIndex ...
func DirectGetExistingStoreIndex(storeIndex *StoreIndex, chunkHashes []uint64, minBlockUsagePercent uint32) *StoreIndex {
	chunkCount := len(chunkHashes)
	storeBlockCount := len(storeIndex.BlockHashes)

	uniqueChunkCount := 0

	chunkToIndexLookup := make(map[uint64]int, chunkCount)
	for i, chunkHash := range chunkHashes {
		if _, exists := chunkToIndexLookup[chunkHash]; exists {
			continue
		}
		chunkToIndexLookup[chunkHash] = i
		uniqueChunkCount++
	}

	foundStoreBlockHashes := make([]uint64, storeBlockCount)
	blockUses := make([]uint32, storeBlockCount)
	blockOrder := make([]int, storeBlockCount)
	blockSizes := make([]uint32, storeBlockCount)

	blockToIndexLookup := make(map[uint64]int, storeBlockCount)
	chunkToStoreIndexLookup := make(map[uint64]int, chunkCount)

	foundBlockCount := 0
	foundBlockChunkCount := 0
	foundChunkCount := 0
	if minBlockUsagePercent <= 100 {
		for b, _ := range storeIndex.BlockHashes {
			blockOrder[b] = b
			blockUses[b] = 0
			blockSizes[b] = 0
			blockChunkCount := int(storeIndex.BlockChunkCounts[b])
			chunkOffset := storeIndex.BlockChunksOffsets[b]
			for c := 0; c < blockChunkCount; c++ {
				chunkSize := storeIndex.ChunkSizes[chunkOffset]
				chunkHash := storeIndex.ChunkHashes[chunkOffset]
				chunkOffset++
				blockSizes[b] += chunkSize
				if _, exists := chunkToIndexLookup[chunkHash]; exists {
					blockUses[b] += chunkSize
				}
			}
		}
		// Favour blocks we use more data out of - if a chunk is in mutliple blocks we want to pick
		// the blocks that has the most requested chunk data
		// This does not guarantee a perfect block match as one block can be a 100% match which
		// could lead to skipping part or whole of another 100% match block resulting in us
		// picking a block that we will not use 100% of
		sort.SliceStable(blockOrder, func(aIndex, bIndex int) bool {
			aUsage := blockUses[aIndex]
			bUsage := blockUses[bIndex]
			if aUsage > bUsage {
				return true
			}
			return false
		})

		for bo := 0; bo < storeBlockCount && foundChunkCount < uniqueChunkCount; bo++ {
			b := blockOrder[bo]
			blockUse := blockUses[b]
			blockSize := blockSizes[b]
			if minBlockUsagePercent > 0 {
				if blockUse == 0 {
					// No more blocks that has any use, exit search
					break
				}
				blockUsagePercent := uint32((uint64(blockUse) * 100) / uint64(blockSize))
				if blockUsagePercent < minBlockUsagePercent {
					continue
				}
			}
			blockHash := storeIndex.BlockHashes[b]
			blockChunkCount := int(storeIndex.BlockChunkCounts[b])
			storeChunkIndexOffset := storeIndex.BlockChunksOffsets[b]
			currentFoundBlockIndex := foundBlockCount
			for c := 0; c < blockChunkCount; c++ {
				chunkHash := storeIndex.ChunkHashes[storeChunkIndexOffset]
				if blockUse != blockSize {
					if _, exists := chunkToIndexLookup[chunkHash]; !exists {
						storeChunkIndexOffset++
						continue
					}
				}
				if _, exists := chunkToStoreIndexLookup[chunkHash]; exists {
					storeChunkIndexOffset++
					continue
				}
				foundChunkCount++
				if currentFoundBlockIndex == foundBlockCount {
					if _, exists := blockToIndexLookup[blockHash]; !exists {
						blockToIndexLookup[blockHash] = currentFoundBlockIndex
						foundStoreBlockHashes[foundBlockCount] = blockHash
						foundBlockCount++
						foundBlockChunkCount += blockChunkCount
					}
					storeChunkIndexOffset++
					continue
				}
				storeChunkIndexOffset++
			}
		}
	}
	if foundBlockCount == 0 {
		return &StoreIndex{
			Version:            CurrentStoreIndexVersion,
			HashIdentifier:     storeIndex.HashIdentifier,
			BlockHashes:        []uint64{},
			ChunkHashes:        []uint64{},
			BlockChunksOffsets: []uint32{},
			BlockChunkCounts:   []uint32{},
			BlockTags:          []uint32{},
			ChunkSizes:         []uint32{},
		}
	}

	blockHashLookup := make(map[uint64]int, foundBlockCount)
	for i, blockHash := range storeIndex.BlockHashes {
		if _, exists := blockToIndexLookup[blockHash]; !exists {
			continue
		}
		blockHashLookup[blockHash] = i
	}

	outStoreIndex := &StoreIndex{
		Version:            CurrentStoreIndexVersion,
		HashIdentifier:     storeIndex.HashIdentifier,
		BlockHashes:        make([]uint64, foundBlockCount),
		ChunkHashes:        make([]uint64, foundBlockChunkCount),
		BlockChunksOffsets: make([]uint32, foundBlockCount),
		BlockChunkCounts:   make([]uint32, foundBlockCount),
		BlockTags:          make([]uint32, foundBlockCount),
		ChunkSizes:         make([]uint32, foundBlockChunkCount),
	}

	blockChunkOffset := 0

	for b := 0; b < foundBlockCount; b++ {
		blockHash := foundStoreBlockHashes[b]
		storeBlockIndex := blockHashLookup[blockHash]
		blockChunkCount := int(storeIndex.BlockChunkCounts[storeBlockIndex])
		storeBlockChunkOffset := int(storeIndex.BlockChunksOffsets[storeBlockIndex])
		blockTag := storeIndex.BlockTags[storeBlockIndex]

		outStoreIndex.BlockHashes[b] = blockHash
		outStoreIndex.BlockChunkCounts[b] = uint32(blockChunkCount)
		outStoreIndex.BlockChunksOffsets[b] = uint32(blockChunkOffset)
		outStoreIndex.BlockTags[b] = blockTag
		for c := 0; c < blockChunkCount; c++ {
			chunkHash := storeIndex.ChunkHashes[storeBlockChunkOffset+c]
			chunkSize := storeIndex.ChunkSizes[storeBlockChunkOffset+c]
			outStoreIndex.ChunkHashes[blockChunkOffset+c] = chunkHash
			outStoreIndex.ChunkSizes[blockChunkOffset+c] = chunkSize
		}
		blockChunkOffset += blockChunkCount
	}

	return outStoreIndex
}
