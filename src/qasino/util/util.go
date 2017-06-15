package util

import (
	"errors"
	"fmt"
	"strings"

	"github.com/golang/glog"
)

// Based on stdlib Join, RepeatJoin concatenates a string 'a',
// 'repeat' times to create a new string.  The separator string 'sep'
// is placed between elements in the resulting string.
func RepeatJoin(a string, sep string, repeat int) string {
	if a == "" || repeat < 1 {
		return ""
	}
	if repeat == 1 {
		return a
	}
	n := len(sep)*(repeat-1) + len(a)*repeat

	b := make([]byte, n)
	bp := copy(b, a)

	for i := 1; i < repeat; i++ {
		bp += copy(b[bp:], sep)
		bp += copy(b[bp:], a)
	}
	return string(b)
}

func Wrap_err(format string, a ...interface{}) error {
	return errors.New(fmt.Sprintf(format, a...))
}

func Log_wrap_err(format string, a ...interface{}) error {
	errorstr := fmt.Sprintf(format, a...)
	glog.ErrorDepth(1, errorstr)
	return errors.New(errorstr)
}

// Converts anything to bool using what you would expect with numbers and
// using for string: 't', 'true' !'0' == true and 'f', 'false', '0' == false
func ToBool(thing interface{}) bool {
	switch thing.(type) {
	case bool:
		return thing.(bool)
	case int:
		return thing != 0
	case int64:
		return thing != 0
	case int32:
		return thing != 0
	case float64:
		return thing != 0.0
	case float32:
		return thing != 0.0
	case string:
		switch strings.ToLower(thing.(string)) {
		case "t":
			return true
		case "true":
			return true
		case "":
			return false
		case "f":
			return false
		case "false":
			return false
		case "0":
			return false
		case "00":
			return false
		case "000":
			return false
		case "0000":
			return false
		default:
			return true
		}
	}
	// Some other type??
	return false
}

func ToBoolDefault(thing interface{}, _default bool) bool {
	if thing == nil {
		return _default
	}
	return ToBool(thing)
}
