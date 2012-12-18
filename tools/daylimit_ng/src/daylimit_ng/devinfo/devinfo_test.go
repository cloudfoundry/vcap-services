package devinfo

import (
	. "launchpad.net/gocheck"
	"os/exec"
	"strings"
	"testing"
)

type DevSuite struct{}

var _ = Suite(&DevSuite{})

func Test(t *testing.T) { TestingT(t) }

func addInterface(c *C) {
	cmds := []string{
		"ip link add name w-xxx-0 type veth peer name w-xxx-1",
		"ip link set w-xxx-0 netns 1",
		"ip link set w-xxx-1 netns 1",
		"ifconfig w-xxx-0 10.253.0.1 netmask 255.255.0.0",
	}
	for _, cmdStr := range cmds {
		cmd := exec.Command(strings.Split(cmdStr, " ")[0], strings.Split(cmdStr, " ")[1:]...)
		if err := cmd.Run(); err != nil {
			c.Fatalf("Run command [%s] error [%s]", cmdStr, err)
		}
	}
}

func clearInterface(c *C) {
	cmds := []string{
		"ip link del w-xxx-0",
		"ip link del w-xxx-1",
	}
	for _, cmdStr := range cmds {
		cmd := exec.Command(strings.Split(cmdStr, " ")[0], strings.Split(cmdStr, " ")[1:]...)
		if err := cmd.Run(); err != nil {
			c.Logf("Run command [%s] error [%s]", cmdStr, err)
		}
	}
}

func (s *DevSuite) TestGetList(c *C) {
	clearInterface(c)
	addInterface(c)
	defer clearInterface(c)
	var info map[string]int64
	var err error
	if info, err = GetList(); err != nil {
		c.Fatalf("GetList return error [%s]", err)
	}
	_, ok := info["xxx"]
	c.Assert(ok, Equals, true)
}
