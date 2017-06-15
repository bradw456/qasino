package util

import (
	"github.com/spf13/viper"
)

func ConfGetStringDefault(key, default_value string) string {
	if viper.IsSet(key) {
		return viper.GetString(key)
	} else {
		return default_value
	}
}

func ConfGetIntDefault(key string, default_value int) int {
	if viper.IsSet(key) {
		return viper.GetInt(key)
	} else {
		return default_value
	}
}
