package main

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/commands"
	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/alecthomas/kong"
	"github.com/sirupsen/logrus"
)

func main() {
	executionStartTime := time.Now()
	initStartTime := executionStartTime

	context := &commands.Context{}

	defer func() {
		executionTime := time.Since(executionStartTime)
		context.TimeStats = append(context.TimeStats, longtailutils.TimeStat{"Execution", executionTime})

		if commands.Cli.ShowStoreStats {
			for _, s := range context.StoreStats {
				longtailutils.PrintStats(s.Name, s.Stats)
			}
		}

		if commands.Cli.ShowStats {
			maxLen := 0
			for _, s := range context.TimeStats {
				if len(s.Name) > maxLen {
					maxLen = len(s.Name)
				}
			}
			for _, s := range context.TimeStats {
				name := fmt.Sprintf("%s:", s.Name)
				log.Printf("%-*s %s", maxLen+1, name, s.Dur)
			}
		}
	}()

	ctx := kong.Parse(&commands.Cli)

	longtailLogLevel, err := longtailutils.ParseLevel(commands.Cli.LogLevel)
	if err != nil {
		log.Fatal(err)
	}

	switch commands.Cli.LogLevel {
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	}

	longtaillib.SetLogger(&longtailutils.LoggerData{})
	defer longtaillib.SetLogger(nil)
	longtaillib.SetLogLevel(longtailLogLevel)

	longtaillib.SetAssert(&longtailutils.AssertData{})
	defer longtaillib.SetAssert(nil)

	if commands.Cli.WorkerCount == 0 {
		context.NumWorkerCount = runtime.NumCPU()
	} else {
		context.NumWorkerCount = commands.Cli.WorkerCount
	}

	if commands.Cli.MemTrace || commands.Cli.MemTraceDetailed || commands.Cli.MemTraceCSV != "" {
		longtaillib.EnableMemtrace()
		defer func() {
			memTraceLogLevel := longtaillib.MemTraceSummary
			if commands.Cli.MemTraceDetailed {
				memTraceLogLevel = longtaillib.MemTraceDetailed
			}
			if commands.Cli.MemTraceCSV != "" {
				longtaillib.MemTraceDumpStats(commands.Cli.MemTraceCSV)
			}
			memTraceLog := longtaillib.GetMemTraceStats(memTraceLogLevel)
			memTraceLines := strings.Split(memTraceLog, "\n")
			for _, l := range memTraceLines {
				if l == "" {
					continue
				}
				log.Printf("[MEM] %s", l)
			}
			longtaillib.DisableMemtrace()
		}()
	}

	initTime := time.Since(initStartTime)

	err = ctx.Run(context)

	context.TimeStats = append([]longtailutils.TimeStat{{"Init", initTime}}, context.TimeStats...)

	if err != nil {
		log.Fatal(err)
	}
}
