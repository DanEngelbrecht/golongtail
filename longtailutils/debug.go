package longtailutils

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

// LoggerData ...
type LoggerData struct {
}

// OnLog ...
func (l *LoggerData) OnLog(file string, function string, line int, level int, logFields []longtaillib.LogField, message string) {

	fields := make(map[string]interface{})
	for _, field := range logFields {
		fields[field.Name] = field.Value
	}
	fields["file"] = file
	fields["func"] = function
	fields["line"] = line

	log := logrus.WithFields(fields)

	switch level {
	case 0:
		log.WithFields(fields).Debug(message)
	case 1:
		log.WithFields(fields).Info(message)
	case 2:
		log.WithFields(fields).Warning(message)
	case 3:
		log.WithFields(fields).Error(message)
	case 4:
		break
	}

	//log.Printf("%s", b.String())
}

func ParseLevel(lvl string) (int, error) {
	const fname = "ParseLevel"
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

	return -1, errors.Wrap(fmt.Errorf("invalid log Level: %s", lvl), fname)
}

// AssertData ...
type AssertData struct {
}

// OnAssert ...
func (a *AssertData) OnAssert(expression string, file string, line int) {
	logrus.Fatalf("ASSERT: %s %s:%d", expression, file, line)
}

type FileHook struct {
	path      string
	formatter logrus.Formatter
	fd        *os.File
}

func NewFileLog(path string, formatter logrus.Formatter) (FileHook, error) {
	dir := filepath.Dir(path)
	os.MkdirAll(dir, os.ModePerm)
	fd, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		logrus.Println("failed to open logfile:", path, err)
		return FileHook{}, err
	}
	return FileHook{path: path, formatter: formatter, fd: fd}, nil
}

func (hook *FileHook) Fire(entry *logrus.Entry) error {
	msg, err := hook.formatter.Format(entry)

	if err != nil {
		log.Println("failed to generate string for entry:", err)
		return err
	}
	_, err = hook.fd.Write(msg)
	if err != nil {
		log.Println("failed to write string to log file:", err)
		return err
	}
	return hook.fd.Sync()
}

func (hook *FileHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.TraceLevel,
		logrus.DebugLevel,
		logrus.InfoLevel,
		logrus.WarnLevel,
		logrus.ErrorLevel,
		logrus.FatalLevel,
		logrus.PanicLevel,
	}
}
