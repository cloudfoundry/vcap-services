package helper

import (
	"daylimit_ng/config"
	"daylimit_ng/warden"
	. "launchpad.net/gocheck"
	"os/exec"
	"path"
	"runtime"
	"strings"
)

var w *warden.Warden = nil
var containerId string

func ContainerId() string {
	return containerId
}

func Warden() *warden.Warden {
	if w == nil {
		_, file, _, _ := runtime.Caller(0)
		config.Load(path.Join(path.Dir(file), "../../config/config.yml"))
		w = &warden.Warden{
			Bin:          config.Get().WardenBin,
			BlockRate:    config.Get().BlockRate,
			BlockBurst:   config.Get().BlockRate,
			UnblockRate:  config.Get().UnblockRate,
			UnblockBurst: config.Get().UnblockRate,
		}
	}
	return w
}

func CreateContainer(c *C) {
	cmd := exec.Command(Warden().Bin, "--", "create")
	if out, err := cmd.Output(); err != nil {
		c.Fatalf("Create new container error [%s]", err)
	} else {
		containerId = strings.TrimRight(strings.Split(string(out), " : ")[1], "\n")
	}
}

func DestroyContainer(c *C) {
	cmd := exec.Command(Warden().Bin, "--", "destroy", "--handle", containerId)
	if err := cmd.Run(); err != nil {
		c.Fatalf("Create new container error [%s]", err)
	}
	containerId = ""
}
