package daylimit

import (
	"io/ioutil"
	"launchpad.net/goyaml"
	"os"
	"syscall"
)

var fileName string
var config *ConfigInfo = nil
var loaded bool = false

type ConfigInfo struct {
	LimitWindow   int64
	LimitSize     int64
	LogFile       string
	FetchInterval int64
	BlockRate     int64
	UnblockRate   int64
	WardenBin     string
}

func Config() *ConfigInfo {
	return config
}

func Exist(filename string) bool {
	if _, err := os.Stat(filename); err != nil {
		if e, ok := err.(*os.PathError); !ok || (e.Err != syscall.ENOENT && e.Err != syscall.ENOTDIR) {
			Logger().Warnf("Stat file error:[%s]", e)
		}
		return false
	}
	return true
}

func LoadConfig(filename string) (err error) {
	if ok := Exist(filename); !ok {
		panic("Config file not exist")
	}
	var data []byte
	if data, err = ioutil.ReadFile(filename); err != nil {
		return
	}
	config = new(ConfigInfo)
	return goyaml.Unmarshal(data, config)
}
