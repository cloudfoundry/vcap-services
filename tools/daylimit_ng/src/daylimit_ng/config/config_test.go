package config

import (
	//	"fmt"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"strconv"
	"strings"
	"testing"
)

type ConfigSuite struct{}

var _ = Suite(&ConfigSuite{})

func Test(t *testing.T) { TestingT(t) }

func getInt(val string) int64 {
	return func(first ...interface{}) int64 { return first[0].(int64) }(strconv.ParseInt(val, 0, 64))
}

func (cs *ConfigSuite) TestLoad(c *C) {
	err := Load("./config.yml")
	c.Assert(err, IsNil)
	cfgs, err := ioutil.ReadFile("./config.yml")
	c.Assert(err, IsNil)
	vals := make(map[string]string)
	for _, line := range strings.Split(string(cfgs), "\n") {
		if strings.Contains(line, "---") {
			continue
		}
		keyval := strings.Split(line, ": ")
		if len(keyval) < 2 {
			continue
		}
		vals[keyval[0]] = keyval[1]
	}
	c.Assert(config.BlockRate, Equals, getInt(vals["blockrate"]))
	c.Assert(config.FetchInteval, Equals, getInt(vals["fetchinteval"]))
	c.Assert(config.LimitSize, Equals, getInt(vals["limitsize"]))
	c.Assert(config.LimitWindow, Equals, getInt(vals["limitwindow"]))
	c.Assert(config.LogFile, Equals, vals["logfile"])
	c.Assert(config.UnblockRate, Equals, getInt(vals["unblockrate"]))
	c.Assert(config.WardenBin, Equals, vals["wardenbin"])
}
