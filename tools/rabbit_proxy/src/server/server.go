package server

import (
	"logger"
	"os"
	"syscall"
	"time"
)

var fdPath = make(map[int]*Path)
var fdPathPair = make(map[int]*PathPair)
var events = make([]syscall.EpollEvent, 10, 10)
var chExit = make(chan bool, 1)
var epollFd int

func Exit() {
	chExit <- true
}

func Run(paths []*Path) (err error) {
	epollFd, err = syscall.EpollCreate(1024)
	if err != nil {
		logger.Log(logger.ERR, "Create Epoll Fd Error [%s]", err)
		os.Exit(-2)
	}

	for _, p := range paths {
		p.Pairs = make([]*PathPair, 0, 100)
		for _, step := range initStep {
			err = step.Action(p)
			if err != nil {
				logger.Log(logger.ERR, step.Fmt, err)
				os.Exit(-2)
			}
		}
		p.CheckTime = time.Now()
		fdPath[p.LFd] = p
	}
	for {
		en, err := syscall.EpollWait(epollFd, events, 1000)
		if err != nil {
			logger.Log(logger.ERR, "Wail Epoll Fd Error [%s]", err)
			os.Exit(-2)
		} else if en == 0 {
		}
		for i := 0; i < en; i++ {
			ee := events[i]
			pa, ok := fdPath[int(ee.Fd)]
			if ok {
				err = pa.newPair()
				continue
			}
			pp, ok := fdPathPair[int(ee.Fd)]
			if !ok {
				continue
			}
			if ee.Events&syscall.EPOLLIN != 0 {
				pp.handleIn(int(ee.Fd))
			}
			if ee.Events&syscall.EPOLLOUT != 0 {
				pp.handleOut(int(ee.Fd))
			}
			if ee.Events&syscall.EPOLLHUP != 0 {
				pp.shutdown()
			}
		}
	}
	return
}
