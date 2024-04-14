package longtailutils

import (
	"context"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// ProgressData ...
type ProgressData struct {
	inited    bool
	startTime time.Time
	lastTime  time.Time
	last      uint32
	task      string
}

// ProgressData ...
func (p *ProgressData) OnProgress(totalCount uint32, doneCount uint32) {
	endChar := "\r"
	if doneCount == totalCount {
		if !p.inited {
			return
		}
		endChar = "\n"
	}

	etaString := ""
	if p.inited {
		if doneCount != totalCount {
			fractionDoneThisRound := float64(doneCount-p.last) / float64(totalCount)
			timeThisRound := time.Since(p.lastTime)
			fractionLeft := float64(totalCount-doneCount) / float64(totalCount)
			fractionEta := float64(timeThisRound.Seconds()) / fractionDoneThisRound * fractionLeft
			averageRate := float64(doneCount) / time.Since(p.startTime).Seconds()
			averageEta := (float64(totalCount) - float64(doneCount)) / averageRate
			weightedEta := ((fractionEta + (averageEta * 3)) / 4)
			eta := (time.Duration(weightedEta) * time.Second).String()
			etaString = fmt.Sprintf(":%s", eta)
		}
	}
	p.lastTime = time.Now()

	p.inited = true
	percentDone := int((100 * doneCount) / totalCount)

	progressBarFullLength := 50
	progressBarCount := int(progressBarFullLength * percentDone / 100)
	elapsed := (time.Duration(time.Since(p.startTime).Seconds()) * time.Second).String()

	timeString := fmt.Sprintf("%s%s", elapsed, etaString)

	fmt.Printf("\r%s %3d%%: |%s%s|: [%s]        %s",
		p.task,
		percentDone,
		strings.Repeat("█", progressBarCount), strings.Repeat(" ", progressBarFullLength-progressBarCount),
		timeString,
		endChar)

	p.last = doneCount
}

// CreateProgress ...
func CreateProgress(task string, percentRateLimit uint32) longtaillib.Longtail_ProgressAPI {
	const fname = "CreateProgress"
	log := log.WithContext(context.Background()).WithFields(log.Fields{
		"fname": fname,
		"task":  task,
	})
	log.Debug(fname)
	progress := &ProgressData{task: task, startTime: time.Now()}
	baseProgress := longtaillib.CreateProgressAPI(progress)
	if percentRateLimit == 0 {
		return baseProgress
	}
	return longtaillib.CreateRateLimitedProgressAPI(baseProgress, percentRateLimit)
}
