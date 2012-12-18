package helper

import (
	"daylimit_ng/config"
	"daylimit_ng/warden"
	"fmt"
	. "launchpad.net/gocheck"
	"os/exec"
	"strings"
)

const (
	CREATETP  = "%s -- create"
	DESTROYTP = "%s -- destroy --handle %s"
)

var w = &warden.Warden{
	Bin:          config.Get().WardenBin,
	BlockRate:    config.Get().BlockRate,
	BlockBurst:   config.Get().BlockRate,
	UnblockRate:  config.Get().UnblockRate,
	UnblockBurst: config.Get().UnblockRate,
}

var containerId string

func ContainerId() string {
	return containerId
}

func Warden() *warden.Warden {
	return w
}

func CreateContainer(c *C) {
	cmdStr := fmt.Sprintf(CREATETP, w.Bin)
	cmd := exec.Command(strings.Split(cmdStr, " ")[0], strings.Split(cmdStr, " ")[1:]...)
	if out, err := cmd.Output(); err != nil {
		c.Fatalf("Create new container error [%s]", err)
	} else {
		containerId = strings.TrimRight(strings.Split(string(out), " : ")[1], "\n")
	}
}

func DestroyContainer(c *C) {
	cmdStr := fmt.Sprintf(CREATETP, w.Bin)
	cmd := exec.Command(strings.Split(cmdStr, " ")[0], strings.Split(cmdStr, " ")[1:]...)
	if err := cmd.Run(); err != nil {
		c.Fatalf("Create new container error [%s]", err)
	}
	containerId = ""
}
