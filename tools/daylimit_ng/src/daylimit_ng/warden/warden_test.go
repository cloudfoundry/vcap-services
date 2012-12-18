package warden

import (
	"daylimit_ng/logger"
	"fmt"
	. "launchpad.net/gocheck"
	"os/exec"
	"strings"
	"testing"
)

const (
	CREATETP  = "%s -- create"
	DESTROYTP = "%s -- destroy --handle %s"
)

type WardenSuite struct{}

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&WardenSuite{})

var w = &Warden{
	Bin:          "/home/gaoyin/gerrit/warden/warden/bin/warden",
	BlockRate:    600,
	BlockBurst:   600,
	UnblockRate:  1200,
	UnblockBurst: 1200,
}

var containerId string

func createContainer(c *C) {
	cmdStr := fmt.Sprintf(CREATETP, w.Bin)
	cmd := exec.Command(strings.Split(cmdStr, " ")[0], strings.Split(cmdStr, " ")[1:]...)
	if out, err := cmd.Output(); err != nil {
		c.Fatalf("Create new container error [%s]", err)
	} else {
		containerId = strings.TrimRight(strings.Split(string(out), " : ")[1], "\n")
	}
}

func destroyContainer(c *C) {
	cmdStr := fmt.Sprintf(CREATETP, w.Bin)
	cmd := exec.Command(strings.Split(cmdStr, " ")[0], strings.Split(cmdStr, " ")[1:]...)
	if err := cmd.Run(); err != nil {
		c.Fatalf("Create new container error [%s]", err)
	}
	containerId = ""
}

func wrapCall(c *C, fun func(c *C)) {
	logger.InitLog("")
	createContainer(c)
	defer destroyContainer(c)
	fun(c)
}

func (ws *WardenSuite) TestBlock(c *C) {
	wrapCall(c, func(c *C) {
		w.Block(containerId)
		rate, burst, err := w.GetRate(containerId)
		c.Assert((rate-w.BlockRate)/60, Equals, int64(0))
		c.Assert((burst-w.BlockBurst)/60, Equals, int64(0))
		c.Assert(err, IsNil)
	})
}

func (ws *WardenSuite) TestUnblock(c *C) {
	wrapCall(c, func(c *C) {
		w.Unblock(containerId)
		rate, burst, err := w.GetRate(containerId)
		c.Assert((rate-w.UnblockRate)/120, Equals, int64(0))
		c.Assert((burst-w.UnblockBurst)/120, Equals, int64(0))
		c.Assert(err, IsNil)
	})
}
