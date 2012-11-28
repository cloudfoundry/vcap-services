package iptablesrun

import (
	"bytes"
	"daylimit/logger"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

const (
	SAVECMD    = "/sbin/iptables-save"
	RESTORECMD = "/sbin/iptables-restore"
	ARGS       = "-c"
	// Iptables in rule match regexp
	INRULE = "\\[(\\d+):(\\d+)\\] -A throughput-count -i w-(\\w+)-0 -j (ACCEPT|DROP)"
	// Iptables out rule match regexp
	OUTRULE = "\\[(\\d+):(\\d+)\\] -A throughput-count -o w-(\\w+)-0 -j (ACCEPT|DROP)"
	RULETPL = "[0:0] -A throughput-count -%c w-%s-0 -j %s"
)

const (
	ACCEPT = 1
	DROP   = 2
)

const (
	IN  = 0
	OUT = 1
)

// Rule info per container id
type RuleInfo struct {
	Size    int64
	Status  int8
	InRule  string
	OutRule string
}

var blockList = make(map[string]int8)
var unblockList = make(map[string]int8)
var rules = make(map[string]*RuleInfo)
var rawRule string
var changeFrom = map[string]string{
	"ACCEPT": "DROP",
	"DROP":   "ACCEPT",
}

func GetBlockList() map[string]int8 {
	return blockList
}

func GetUnblockList() map[string]int8 {
	return unblockList
}

func SetRules(info map[string]*RuleInfo) {
	rules = info
}

func Block(id string) (ok bool) {
	if _, ok = rules[id]; ok {
		blockList[id] = 1
	}
	return ok
}

func Unblock(id string) (ok bool) {
	if _, ok = rules[id]; ok {
		unblockList[id] = 1
	}
	return ok
}

func Update() (err error) {
	if len(blockList)+len(unblockList) <= 0 {
		return
	}
	oldNews := make([]string, 0, 2*(len(blockList)+len(unblockList)))
	for target, list := range map[string]map[string]int8{
		"ACCEPT": unblockList,
		"DROP":   blockList,
	} {
		for id, _ := range list {
			for inter, rule := range map[byte]string{
				'i': rules[id].InRule,
				'o': rules[id].OutRule,
			} {
				oldNews = append(oldNews, rule, fmt.Sprintf(RULETPL, inter, id, target))
			}
		}
	}

	ruleRep := strings.NewReplacer(oldNews...)
	newRules := ruleRep.Replace(rawRule)
	resCmd := exec.Command(RESTORECMD, ARGS)
	var buf bytes.Buffer
	buf.WriteString(newRules)
	stdin, err := resCmd.StdinPipe()
	if err != nil {
		logger.Log(logger.ERR, "Get stdin pipe error [%s]", err)
		return
	}
	if err = resCmd.Start(); err != nil {
		logger.Log(logger.ERR, "Start iptables-restore error [%s]", err)
		return
	}
	if _, err = buf.WriteTo(stdin); err != nil {
		logger.Log(logger.ERR, "Write rules to iptables-restore error [%s]", err)
		return
	}
	stdin.Close()
	if err = resCmd.Wait(); err != nil {
		logger.Log(logger.ERR, "Wait iptables-restore error [%s]", err)
		return
	}
	blockList = make(map[string]int8)
	unblockList = make(map[string]int8)
	return
}

func FetchAll() (ret map[string]*RuleInfo, err error) {
	type reg struct {
		rExp *regexp.Regexp
		rule string
	}

	regRules := map[string]*reg{
		"in":  &reg{rule: INRULE},
		"out": &reg{rule: OUTRULE},
	}

	for _, r := range regRules {
		if r.rExp, err = regexp.Compile(r.rule); err != nil {
			logger.Log(logger.ERR, "Compile regexp [%s] error [%s]", r.rule, err)
			return
		}
	}

	cmd := exec.Command(SAVECMD, ARGS)
	var out []byte
	out, err = cmd.Output()
	if err != nil {
		logger.Log(logger.ERR, "Run iptables-save error [%s]", err)
		return
	}
	rawRule = string(out)
	var size int64
	ret = make(map[string]*RuleInfo)
	for _, line := range strings.Split(rawRule, "\n") {
		for inOut, r := range map[int]*regexp.Regexp{IN: regRules["in"].rExp, OUT: regRules["out"].rExp} {
			if subs := r.FindStringSubmatch(line); subs != nil {
				id := subs[3]
				if size, err = strconv.ParseInt(subs[2], 10, 64); err != nil {
					return
				}
				if _, ok := ret[id]; !ok {
					ret[id] = &RuleInfo{}
				}
				ret[id].Size += size
				if ret[id].Status = DROP; subs[4] == "ACCEPT" {
					ret[id].Status = ACCEPT
				}
				if inOut == IN {
					ret[id].InRule = subs[0]
				} else {
					ret[id].OutRule = subs[0]
				}
			}
		}
	}
	rules = ret
	return
}
