package main

import (
	"github.com/xushiwei/goyaml"
	"go-mongo-proxy/proxy"
	"io/ioutil"
)

func load_config(path string) (config proxy.ProxyConfig) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	conf := proxy.ProxyConfig{}
	err = goyaml.Unmarshal([]byte(data), &conf)
	if err != nil {
		panic(err)
	}
	return conf
}
