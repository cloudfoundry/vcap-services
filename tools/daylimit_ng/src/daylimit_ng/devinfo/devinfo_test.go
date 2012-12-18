package devinfo

import (
	"daylimit_ng/test/helper"
	. "launchpad.net/gocheck"
	"testing"
)

type DevSuite struct{}

var _ = Suite(&DevSuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *DevSuite) TestGetList(c *C) {
	helper.DestroyContainer(c)
	helper.CreateContainer(c)
	defer helper.DestroyContainer(c)
	var info map[string]int64
	var err error
	if info, err = GetList(); err != nil {
		c.Fatalf("GetList return error [%s]", err)
	}
	_, ok := info[helper.ContainerId()]
	c.Assert(ok, Equals, true)
}
