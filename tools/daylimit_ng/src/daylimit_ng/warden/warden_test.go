package warden_test

import (
	"daylimit_ng/logger"
	"daylimit_ng/test/helper"
	. "launchpad.net/gocheck"
	"testing"
)

type WardenSuite struct{}

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&WardenSuite{})

func wrapCall(c *C, fun func(c *C)) {
	logger.InitLog("")
	helper.CreateContainer(c)
	defer helper.DestroyContainer(c)
	fun(c)
}

func (ws *WardenSuite) TestBlock(c *C) {
	wrapCall(c, func(c *C) {
		w := helper.Warden()
		containerId := helper.ContainerId()
		w.Block(containerId)
		rate, burst, err := w.GetRate(containerId)
		c.Assert((rate-w.BlockRate)/60, Equals, int64(0))
		c.Assert((burst-w.BlockBurst)/60, Equals, int64(0))
		c.Assert(err, IsNil)
	})
}

func (ws *WardenSuite) TestUnblock(c *C) {
	wrapCall(c, func(c *C) {
		w := helper.Warden()
		containerId := helper.ContainerId()
		w.Unblock(containerId)
		rate, burst, err := w.GetRate(containerId)
		c.Assert((rate-w.UnblockRate)/120, Equals, int64(0))
		c.Assert((burst-w.UnblockBurst)/120, Equals, int64(0))
		c.Assert(err, IsNil)
	})
}
