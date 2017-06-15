package util

// From Kubernetes.

import (
	"flag"
	"reflect"
	"strings"

	"github.com/spf13/pflag"
)

// flagValueWrapper implements pflag.Value around a flag.Value.  The main
// difference here is the addition of the Type method that returns a string
// name of the type.  As this is generally unknown, we approximate that with
// reflection.
type flagValueWrapper struct {
	inner    flag.Value
	flagType string
}

func wrapFlagValue(v flag.Value) pflag.Value {
	// If the flag.Value happens to also be a pflag.Value, just use it directly.
	if pv, ok := v.(pflag.Value); ok {
		return pv
	}

	pv := &flagValueWrapper{
		inner: v,
	}
	pv.flagType = reflect.TypeOf(v).Elem().Name()
	pv.flagType = strings.TrimSuffix(pv.flagType, "Value")
	return pv
}

func (v *flagValueWrapper) String() string {
	return v.inner.String()
}

func (v *flagValueWrapper) Set(s string) error {
	return v.inner.Set(s)
}

func (v *flagValueWrapper) Type() string {
	return v.flagType
}

// Imports a 'flag.Flag' into a 'pflag.FlagSet'.  The "short" option is unset
// and the type is inferred using reflection.
func AddFlagToPFlagSet(f *flag.Flag, fs *pflag.FlagSet) {
	fs.Var(wrapFlagValue(f.Value), f.Name, f.Usage)
}

// Adds all of the flags in a 'flag.FlagSet' package flags to a 'pflag.FlagSet'.
func AddFlagSetToPFlagSet(fsIn *flag.FlagSet, fsOut *pflag.FlagSet) {
	fsIn.VisitAll(func(f *flag.Flag) {
		AddFlagToPFlagSet(f, fsOut)
	})
}

// Adds all of the top level 'flag' package flags to a 'pflag.FlagSet'.
func AddAllFlagsToPFlagSet(fs *pflag.FlagSet) {
	AddFlagSetToPFlagSet(flag.CommandLine, fs)
}