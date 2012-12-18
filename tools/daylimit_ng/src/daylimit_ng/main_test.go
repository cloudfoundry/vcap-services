package main

import (
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
	opts = CmdOptions{
		LimitWindow:  3,
		LimitSize:    100,
		FetchInteval: 2,
		BlockRate:    600,
		UnblockRate:  1200,
		WardenBin:    "/home/gaoyin/gerrit/warden/warden/bin/warden",
	}

	w = &warden.Warden{
		Bin:          "/home/gaoyin/gerrit/warden/warden/bin/warden",
		BlockRate:    600,
		BlockBurst:   600,
		UnblockRate:  1200,
		UnblockBurst: 1200,
	}

	SizeCheck("test", 1000)
	time.Sleep(1 * time.Second)
	SizeCheck("test", 10000)
	c.Assert(items["test"].Status, Equals, int8(BLOCK))
	time.Sleep(time.Duration(opts.LimitWindow+1) * time.Second)
	SizeCheck("test", 10000)
	c.Assert(items["test"].Status, Equals, int8(UNBLOCK))
}
