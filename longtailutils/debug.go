package longtailutils

import (
	"fmt"
	"log"
	"strings"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/pkg/errors"
)

// LoggerData ...
type LoggerData struct {
}

var logLevelNames = [...]string{"DEBUG", "INFO", "WARNING", "ERROR", "OFF"}

// OnLog ...
func (l *LoggerData) OnLog(file string, function string, line int, level int, logFields []longtaillib.LogField, message string) {
	var b strings.Builder
	b.Grow(32 + len(message))
	fmt.Fprintf(&b, "{")
	fmt.Fprintf(&b, "\"file\": \"%s\"", file)
	fmt.Fprintf(&b, ", \"func\": \"%s\"", function)
	fmt.Fprintf(&b, ", \"line\": \"%d\"", line)
	fmt.Fprintf(&b, ", \"level\": \"%s\"", logLevelNames[level])
	for _, field := range logFields {
		fmt.Fprintf(&b, ", \"%s\": \"%s\"", field.Name, field.Value)
	}
	fmt.Fprintf(&b, ", \"msg\": \"%s\"", message)
	fmt.Fprintf(&b, "}")
	log.Printf("%s", b.String())
}

func ParseLevel(lvl string) (int, error) {
	switch strings.ToLower(lvl) {
	case "debug":
		return 0, nil
	case "info":
		return 1, nil
	case "warn":
		return 2, nil
	case "error":
		return 3, nil
	case "off":
		return 4, nil
	}

	return -1, errors.Wrapf(longtaillib.ErrnoToError(longtaillib.EIO), "not a valid log Level: %s", lvl)
}

// AssertData ...
type AssertData struct {
}

// OnAssert ...
func (a *AssertData) OnAssert(expression string, file string, line int) {
	log.Fatalf("ASSERT: %s %s:%d", expression, file, line)
}
