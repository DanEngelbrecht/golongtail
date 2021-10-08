package longtailutils

import (
	"context"
	"regexp"
	"strings"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
)

func NormalizePath(path string) string {
	doubleForwardRemoved := strings.Replace(path, "//", "/", -1)
	doubleBackwardRemoved := strings.Replace(doubleForwardRemoved, "\\\\", "/", -1)
	backwardRemoved := strings.Replace(doubleBackwardRemoved, "\\", "/", -1)
	return backwardRemoved
}

type regexPathFilter struct {
	compiledIncludeRegexes []*regexp.Regexp
	compiledExcludeRegexes []*regexp.Regexp
}

// MakeRegexPathFilter ...
func MakeRegexPathFilter(includeFilterRegEx string, excludeFilterRegEx string) (longtaillib.Longtail_PathFilterAPI, error) {
	log := log.WithContext(context.Background()).WithFields(log.Fields{
		"includeFilterRegEx": includeFilterRegEx,
		"excludeFilterRegEx": excludeFilterRegEx,
	})
	log.Debug("MakeRegexPathFilter")

	regexPathFilter := &regexPathFilter{}
	if includeFilterRegEx != "" {
		compiledIncludeRegexes, err := splitRegexes(includeFilterRegEx)
		if err != nil {
			log.WithError(err).Error("MakeRegexPathFilter")
			return longtaillib.Longtail_PathFilterAPI{}, errors.Wrap(err, "MakeRegexPathFilter")
		}
		regexPathFilter.compiledIncludeRegexes = compiledIncludeRegexes
	}
	if excludeFilterRegEx != "" {
		compiledExcludeRegexes, err := splitRegexes(excludeFilterRegEx)
		if err != nil {
			log.WithError(err).Error("MakeRegexPathFilter")
			return longtaillib.Longtail_PathFilterAPI{}, errors.Wrap(err, "MakeRegexPathFilter")
		}
		regexPathFilter.compiledExcludeRegexes = compiledExcludeRegexes
	}
	if len(regexPathFilter.compiledIncludeRegexes) > 0 || len(regexPathFilter.compiledExcludeRegexes) > 0 {
		return longtaillib.CreatePathFilterAPI(regexPathFilter), nil
	}
	return longtaillib.Longtail_PathFilterAPI{}, nil
}

func (f *regexPathFilter) Include(rootPath string, assetPath string, assetName string, isDir bool, size uint64, permissions uint16) bool {
	log := log.WithContext(context.Background()).WithFields(log.Fields{
		"rootPath":    rootPath,
		"assetPath":   assetPath,
		"assetName":   assetName,
		"isDir":       isDir,
		"size":        size,
		"permissions": permissions,
	})
	log.Debug("regexPathFilter.Include")

	for _, r := range f.compiledExcludeRegexes {
		if r.MatchString(assetPath) {
			log.Debugf("Skipping `%s`", assetPath)
			return false
		}
	}
	if len(f.compiledIncludeRegexes) == 0 {
		return true
	}
	for _, r := range f.compiledIncludeRegexes {
		if r.MatchString(assetPath) {
			return true
		}
	}
	log.Debugf("Skipping `%s`", assetPath)
	return false
}

func splitRegexes(regexes string) ([]*regexp.Regexp, error) {
	log := log.WithContext(context.Background()).WithFields(log.Fields{
		"regexes": regexes,
	})
	log.Debug("splitRegexes")

	var compiledRegexes []*regexp.Regexp
	m := 0
	s := 0
	for i := 0; i < len(regexes); i++ {
		if (regexes)[i] == '\\' {
			m = -1
		} else if m == 0 && (regexes)[i] == '*' {
			m++
		} else if m == 1 && (regexes)[i] == '*' {
			r := (regexes)[s:(i - 1)]
			regex, err := regexp.Compile(r)
			if err != nil {
				log.WithError(err).Error("splitRegexes")
				return nil, errors.Wrap(err, "splitRegexes")
			}
			compiledRegexes = append(compiledRegexes, regex)
			s = i + 1
			m = 0
		} else {
			m = 0
		}
	}
	if s < len(regexes) {
		r := (regexes)[s:]
		regex, err := regexp.Compile(r)
		if err != nil {
			log.WithError(err).Error("splitRegexes")
			return nil, errors.Wrap(err, "splitRegexes")
		}
		compiledRegexes = append(compiledRegexes, regex)
	}
	return compiledRegexes, nil
}
