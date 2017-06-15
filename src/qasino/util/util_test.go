package util

import (
	"qasino/util"
	"testing"
)

func check_repeatjoin(t *testing.T, testname, a, sep string, repeat int, expected string) {
	if util.RepeatJoin(a, sep, repeat) != expected {
		t.Error("TestRepeatJoin test failed: testname='" + testname + "'")
	}
}

func TestRepeatJoin(t *testing.T) {
	check_repeatjoin(t, "zero", "?", ",", 0, "")
	check_repeatjoin(t, "one", "?", ",", 1, "?")
	check_repeatjoin(t, "empty", "", ",", 1, "")
	check_repeatjoin(t, "onesep", "?", ",", 2, "?,?")
	check_repeatjoin(t, "onenosep", "?", "", 2, "??")
	check_repeatjoin(t, "multemptysep", "abc", "", 2, "abcabc")
	check_repeatjoin(t, "longersep", "abc", "--", 2, "abc--abc")
	check_repeatjoin(t, "mult", "abc", "-", 3, "abc-abc-abc")
	check_repeatjoin(t, "multlongersep", "abc", "--------", 4, "abc--------abc--------abc--------abc")
	check_repeatjoin(t, "longmult", "abcdefghijklmnopqrstuvwxyz123456789", "---------------", 10, "abcdefghijklmnopqrstuvwxyz123456789---------------abcdefghijklmnopqrstuvwxyz123456789---------------abcdefghijklmnopqrstuvwxyz123456789---------------abcdefghijklmnopqrstuvwxyz123456789---------------abcdefghijklmnopqrstuvwxyz123456789---------------abcdefghijklmnopqrstuvwxyz123456789---------------abcdefghijklmnopqrstuvwxyz123456789---------------abcdefghijklmnopqrstuvwxyz123456789---------------abcdefghijklmnopqrstuvwxyz123456789---------------abcdefghijklmnopqrstuvwxyz123456789")
}
