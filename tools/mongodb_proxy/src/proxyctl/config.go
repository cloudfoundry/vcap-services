package main

import (
	"io/ioutil"
	"go-mongo-proxy/proxy"
    "github.com/xushiwei/goyaml"
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
