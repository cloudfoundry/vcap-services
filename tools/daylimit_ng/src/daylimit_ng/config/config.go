package config

import (
	"daylimit_ng/logger"
	"io/ioutil"
	"launchpad.net/goyaml"
	"os"
	"path"
	"runtime"
	"syscall"
)

var fileName string
var config *Config = nil
var loaded bool = false

const (
	DEFAULTCONFIG = "config.yml"
)

type Config struct {
	LimitWindow  int64
	LimitSize    int64
	LogFile      string
	FetchInteval int64
	BlockRate    int64
	UnblockRate  int64
	WardenBin    string
}

func Get() *Config {
	if config == nil {
		_, file, _, _ := runtime.Caller(0)
		filename := path.Join(path.Dir(file), DEFAULTCONFIG)
		Load(filename)
	}
	return config
}

func Exist(filename string) bool {
	if _, err := os.Stat(filename); err != nil {
		if e, ok := err.(*os.PathError); !ok || (e.Err != syscall.ENOENT && e.Err != syscall.ENOTDIR) {
			logger.Logger().Warnf("Stat file error:[%s]", e)
		}
		return false
	}
	return true
}

func Load(filename string) (err error) {
	if ok := Exist(filename); !ok {
		panic("Config file not exist")
	}
	var data []byte
	if data, err = ioutil.ReadFile(filename); err != nil {
		return
	}
	config = new(Config)
	return goyaml.Unmarshal(data, config)
}
