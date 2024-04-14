package longtailutils

import (
	"context"
	"regexp"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type regexPathFilter struct {
	includeRegexes         []string
	compiledIncludeRegexes []*regexp.Regexp
	excludeRegexes         []string
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
		includeRegexes, compiledIncludeRegexes, err := splitRegexes(includeFilterRegEx)
		if err != nil {
			return longtaillib.Longtail_PathFilterAPI{}, errors.Wrap(err, fname)
		}
		regexPathFilter.includeRegexes = includeRegexes
		regexPathFilter.compiledIncludeRegexes = compiledIncludeRegexes
	}
	if excludeFilterRegEx != "" {
		excludeRegexes, compiledExcludeRegexes, err := splitRegexes(excludeFilterRegEx)
		if err != nil {
			return longtaillib.Longtail_PathFilterAPI{}, errors.Wrap(err, fname)
		}
		regexPathFilter.excludeRegexes = excludeRegexes
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

	for i, r := range f.compiledExcludeRegexes {
		if r.MatchString(assetPath) {
			log.Debugf("Excluded file `%s` (match for `%s`)", assetPath, f.excludeRegexes[i])
			return false
		}
		if isDir {
			if r.MatchString(assetPath + "/") {
				log.Debugf("Excluded dir `%s/` (match for `%s`)", assetPath, f.excludeRegexes[i])
				return false
			}
		}
	}
	if len(f.compiledIncludeRegexes) == 0 {
		return true
	}
	for i, r := range f.compiledIncludeRegexes {
		if r.MatchString(assetPath) {
			log.Debugf("Included file `%s` (match for `%s`)", assetPath, f.includeRegexes[i])
			return true
		}
		if isDir {
			if r.MatchString(assetPath + "/") {
				log.Debugf("Included dir `%s/` (match for `%s`)", assetPath, f.includeRegexes[i])
				return true
			}
		}
	}
	log.Debugf("Skipping `%s`", assetPath)
	return false
}

func splitRegexes(regexes string) ([]string, []*regexp.Regexp, error) {
	const fname = "splitRegexes"
	log := log.WithContext(context.Background()).WithFields(log.Fields{
		"fname":   fname,
		"regexes": regexes,
	})
	log.Debug(fname)

	var regexStrings []string
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
				return nil, nil, errors.Wrap(err, fname)
			}
			regexStrings = append(regexStrings, r)
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
			return nil, nil, errors.Wrap(err, fname)
		}
		regexStrings = append(regexStrings, r)
		compiledRegexes = append(compiledRegexes, regex)
	}
	return regexStrings, compiledRegexes, nil
}
