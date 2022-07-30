package longtailutils

import (
	"fmt"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/sirupsen/logrus"
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

func PrintStats(name string, stats longtaillib.BlockStoreStats, showStats bool) {
	if showStats {
		fmt.Printf("%s:\n", name)
		fmt.Printf("------------------\n")
		fmt.Printf("GetStoredBlock_Count:          %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]))
		fmt.Printf("GetStoredBlock_RetryCount:     %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_RetryCount]))
		fmt.Printf("GetStoredBlock_FailCount:      %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount]))
		fmt.Printf("GetStoredBlock_Chunk_Count:    %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count]))
		fmt.Printf("GetStoredBlock_Byte_Count:     %s\n", ByteCountBinary(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count]))
		fmt.Printf("PutStoredBlock_Count:          %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count]))
		fmt.Printf("PutStoredBlock_RetryCount:     %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount]))
		fmt.Printf("PutStoredBlock_FailCount:      %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount]))
		fmt.Printf("PutStoredBlock_Chunk_Count:    %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count]))
		fmt.Printf("PutStoredBlock_Byte_Count:     %s\n", ByteCountBinary(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count]))
		fmt.Printf("GetExistingContent_Count:      %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_Count]))
		fmt.Printf("GetExistingContent_RetryCount: %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_RetryCount]))
		fmt.Printf("GetExistingContent_FailCount:  %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_FailCount]))
		fmt.Printf("PreflightGet_Count:            %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_Count]))
		fmt.Printf("PreflightGet_RetryCount:       %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_RetryCount]))
		fmt.Printf("PreflightGet_FailCount:        %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount]))
		fmt.Printf("Flush_Count:                   %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_Flush_Count]))
		fmt.Printf("Flush_FailCount:               %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_Flush_FailCount]))
		fmt.Printf("GetStats_Count:                %s\n", ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStats_Count]))
		fmt.Printf("------------------\n")
	}
	logrus.WithFields(logrus.Fields{
		"Store":                         name,
		"GetStoredBlock_Count":          ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]),
		"GetStoredBlock_RetryCount":     ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_RetryCount]),
		"GetStoredBlock_FailCount":      ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount]),
		"GetStoredBlock_Chunk_Count":    ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count]),
		"GetStoredBlock_Byte_Count":     ByteCountBinary(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count]),
		"PutStoredBlock_Count":          ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count]),
		"PutStoredBlock_RetryCount":     ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount]),
		"PutStoredBlock_FailCount":      ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount]),
		"PutStoredBlock_Chunk_Count":    ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count]),
		"PutStoredBlock_Byte_Count":     ByteCountBinary(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count]),
		"GetExistingContent_Count":      ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_Count]),
		"GetExistingContent_RetryCount": ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_RetryCount]),
		"GetExistingContent_FailCount":  ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_FailCount]),
		"PreflightGet_Count":            ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_Count]),
		"PreflightGet_RetryCount":       ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_RetryCount]),
		"PreflightGet_FailCount":        ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount]),
		"Flush_Count":                   ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_Flush_Count]),
		"Flush_FailCount":               ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_Flush_FailCount]),
		"GetStats_Count":                ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStats_Count]),
	}).Printf("store stats")
}
