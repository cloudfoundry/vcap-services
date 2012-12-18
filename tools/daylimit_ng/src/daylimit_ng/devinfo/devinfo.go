package devinfo

import (
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

const (
	IFCONFIG = "/sbin/ifconfig"
	IDREG    = "w-(\\w+)-0"
	RXTXREG  = "RX bytes:([0-9]+).*TX bytes:([0-9]+).*"
)

func GetList() (info map[string]int64, err error) {
	ifCmd := exec.Command(IFCONFIG)
	var output []byte
	output, err = ifCmd.Output()
	if err != nil {
		return
	}
	var idReg, rxtxReg *regexp.Regexp
	if idReg, err = regexp.Compile(IDREG); err != nil {
		return
	}
	if rxtxReg, err = regexp.Compile(RXTXREG); err != nil {
		return
	}
	info = make(map[string]int64)
	id := ""
	for _, line := range strings.Split(string(output), "\n") {
		if id == "" {
			ms := idReg.FindStringSubmatch(line)
			if ms != nil {
				id = ms[1]
			}
		} else {
			sizes := rxtxReg.FindStringSubmatch(line)
			if sizes != nil {
				rx, _ := strconv.ParseInt(sizes[1], 0, 64)
				tx, _ := strconv.ParseInt(sizes[2], 0, 64)
				info[id] = rx + tx
				id = ""
			}
		}
	}
	return
}
