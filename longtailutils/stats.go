package longtailutils

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
)

type TimeStat struct {
	Name string
	Dur  time.Duration
}

type StoreStat struct {
	Name  string
	Stats longtaillib.BlockStoreStats
}

func ByteCountDecimal(b uint64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %c", float64(b)/float64(div), "kMGTPE"[exp])
}

func ByteCountBinary(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func GetDetailsString(path string, size uint64, permissions uint16, isDir bool, sizePadding int) string {
	sizeString := fmt.Sprintf("%d", size)
	sizeString = strings.Repeat(" ", sizePadding-len(sizeString)) + sizeString
	bits := ""
	if isDir {
		bits += "d"
		path = strings.TrimRight(path, "/")
	} else {
		bits += "-"
	}
	if (permissions & 0400) == 0 {
		bits += "-"
	} else {
		bits += "r"
	}
	if (permissions & 0200) == 0 {
		bits += "-"
	} else {
		bits += "w"
	}
	if (permissions & 0100) == 0 {
		bits += "-"
	} else {
		bits += "x"
	}

	if (permissions & 0040) == 0 {
		bits += "-"
	} else {
		bits += "r"
	}
	if (permissions & 0020) == 0 {
		bits += "-"
	} else {
		bits += "w"
	}
	if (permissions & 0010) == 0 {
		bits += "-"
	} else {
		bits += "x"
	}

	if (permissions & 0004) == 0 {
		bits += "-"
	} else {
		bits += "r"
	}
	if (permissions & 0002) == 0 {
		bits += "-"
	} else {
		bits += "w"
	}
	if (permissions & 0001) == 0 {
		bits += "-"
	} else {
		bits += "x"
	}

	return fmt.Sprintf("%s %s %s", bits, sizeString, path)
}

func PrintStats(name string, stats longtaillib.BlockStoreStats) {
	log.Printf("%s:\n", name)
	log.Printf("------------------\n")
	log.Printf("GetStoredBlock_Count:          %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]))
	log.Printf("GetStoredBlock_RetryCount:     %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_RetryCount]))
	log.Printf("GetStoredBlock_FailCount:      %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount]))
	log.Printf("GetStoredBlock_Chunk_Count:    %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count]))
	log.Printf("GetStoredBlock_Byte_Count:     %s\n", ByteCountBinary(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count]))
	log.Printf("PutStoredBlock_Count:          %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count]))
	log.Printf("PutStoredBlock_RetryCount:     %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount]))
	log.Printf("PutStoredBlock_FailCount:      %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount]))
	log.Printf("PutStoredBlock_Chunk_Count:    %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count]))
	log.Printf("PutStoredBlock_Byte_Count:     %s\n", ByteCountBinary(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count]))
	log.Printf("GetExistingContent_Count:      %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_Count]))
	log.Printf("GetExistingContent_RetryCount: %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_RetryCount]))
	log.Printf("GetExistingContent_FailCount:  %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_FailCount]))
	log.Printf("PreflightGet_Count:            %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_Count]))
	log.Printf("PreflightGet_RetryCount:       %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_RetryCount]))
	log.Printf("PreflightGet_FailCount:        %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount]))
	log.Printf("Flush_Count:                   %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_Flush_Count]))
	log.Printf("Flush_FailCount:               %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_Flush_FailCount]))
	log.Printf("GetStats_Count:                %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStats_Count]))
	log.Printf("------------------\n")
}
