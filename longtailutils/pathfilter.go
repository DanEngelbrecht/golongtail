package longtailutils

import (
	"context"
	"regexp"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
)

type regexPathFilter struct {
	compiledIncludeRegexes []*regexp.Regexp
	compiledExcludeRegexes []*regexp.Regexp
}

// MakeRegexPathFilter ...
func MakeRegexPathFilter(includeFilterRegEx string, excludeFilterRegEx string) (longtaillib.Longtail_PathFilterAPI, error) {
	const fname = "MakeRegexPathFilter"
	log := log.WithContext(context.Background()).WithFields(log.Fields{
		"fname":              fname,
		"includeFilterRegEx": includeFilterRegEx,
		"excludeFilterRegEx": excludeFilterRegEx,
	})
	log.Debug(fname)

	regexPathFilter := &regexPathFilter{}
	if includeFilterRegEx != "" {
		compiledIncludeRegexes, err := splitRegexes(includeFilterRegEx)
		if err != nil {
			return longtaillib.Longtail_PathFilterAPI{}, errors.Wrap(err, fname)
		}
		regexPathFilter.compiledIncludeRegexes = compiledIncludeRegexes
	}
	if excludeFilterRegEx != "" {
		compiledExcludeRegexes, err := splitRegexes(excludeFilterRegEx)
		if err != nil {
			return longtaillib.Longtail_PathFilterAPI{}, errors.Wrap(err, fname)
		}
		regexPathFilter.compiledExcludeRegexes = compiledExcludeRegexes
	}
	if len(regexPathFilter.compiledIncludeRegexes) > 0 || len(regexPathFilter.compiledExcludeRegexes) > 0 {
		return longtaillib.CreatePathFilterAPI(regexPathFilter), nil
	}
	return longtaillib.Longtail_PathFilterAPI{}, nil
}

func (f *regexPathFilter) Include(rootPath string, assetPath string, assetName string, isDir bool, size uint64, permissions uint16) bool {
	const fname = "regexPathFilter.Include"
	log := log.WithContext(context.Background()).WithFields(log.Fields{
		"fname":       fname,
		"rootPath":    rootPath,
		"assetPath":   assetPath,
		"assetName":   assetName,
		"isDir":       isDir,
		"size":        size,
		"permissions": permissions,
	})
	log.Debug(fname)

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
	const fname = "splitRegexes"
	log := log.WithContext(context.Background()).WithFields(log.Fields{
		"fname":   fname,
		"regexes": regexes,
	})
	log.Debug(fname)

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
				return nil, errors.Wrap(err, fname)
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
			return nil, errors.Wrap(err, fname)
		}
		compiledRegexes = append(compiledRegexes, regex)
	}
	return compiledRegexes, nil
}
