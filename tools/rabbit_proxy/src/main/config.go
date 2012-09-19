package main

import (
	"encoding/json"
	"io/ioutil"
	"logger"
	"server"
)

func LoadConfig(file string) (items []*server.Path, err error) {
	buf, err := ioutil.ReadFile(file)
	if err != nil {
		logger.Log(logger.ERR, "Read Config File Error [%s]", err)
		return
	}
	items = make([]*server.Path, 0, 500)
	err = json.Unmarshal(buf, &items)
	return
}
