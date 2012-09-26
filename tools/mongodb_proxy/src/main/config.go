package main

import (
    "launchpad.net/goyaml"
    "io/ioutil"
    "server"
)

func load_config(path string) (config server.ProxyConfig) {
    data, err := ioutil.ReadFile(path)
    if err != nil {
        panic(err)
    }

    conf := server.ProxyConfig{}
    err = goyaml.Unmarshal([]byte(data), &conf)
    if err != nil {
        panic(err)
    }
    return conf
}
