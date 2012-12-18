package main

import (
	"daylimit_ng/config"
	"daylimit_ng/logger"
	"daylimit_ng/warden"
	. "launchpad.net/gocheck"
	"testing"
	"time"
)

type MainSuite struct{}

var _ = Suite(&MainSuite{})

func Test(t *testing.T) { TestingT(t) }

func (ms *MainSuite) TestSizeCheck(c *C) {
	logger.InitLog("")

	w = &warden.Warden{
		Bin:          config.Get().WardenBin,
		BlockRate:    config.Get().BlockRate,
		BlockBurst:   config.Get().BlockRate,
		UnblockRate:  config.Get().UnblockRate,
		UnblockBurst: config.Get().UnblockRate,
	}

	SizeCheck("test", 1000)
	time.Sleep(time.Duration(config.Get().FetchInteval) * time.Second)
	SizeCheck("test", 10000)
	c.Assert(items["test"].Status, Equals, int8(BLOCK))
	time.Sleep(time.Duration(config.Get().LimitWindow+1) * time.Second)
	SizeCheck("test", 10000)
	c.Assert(items["test"].Status, Equals, int8(UNBLOCK))
}
