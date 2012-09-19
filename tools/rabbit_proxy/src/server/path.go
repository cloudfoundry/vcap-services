package server

import (
	"logger"
	"net"
	"syscall"
	"time"
)

type Path struct {
	EPort     int
	IPort     int
	IIp       net.IP
	PassSize  uint64
	LFd       int
	BlockSize uint64
	Pairs     []*PathPair
	CheckTime time.Time
}

type InitStep struct {
	Fmt    string
	Action func(*Path) error
}

var initStep = [...]InitStep{
	{Fmt: "Create Epoll Fd Error [%s]",
		Action: func(p *Path) (err error) {
			p.LFd, err = syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
			return err
		}},
	{Fmt: "Bind Epoll Fd Error [%s]",
		Action: func(p *Path) error {
			return syscall.Bind(p.LFd, &syscall.SockaddrInet4{Port: p.EPort, Addr: [4]byte{0, 0, 0, 0}})
		}},
	{Fmt: "Listen Fd Error [%s]",
		Action: func(p *Path) error {
			return syscall.Listen(p.LFd, 10)
		}},
	{Fmt: "Add Fd To Epoll Error [%s]",
		Action: func(p *Path) error {
			return syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_ADD, p.LFd, &syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(p.LFd)})
		}},
}

var pathAddStep = [...]PathStep{
	{Fmt: "Port [%d] Accept Error [%s]", Action: func(pp *PathPair) (err error) {
		pp.EFd, _, err = syscall.Accept(pp.RelPath.LFd)
		return
	}},
	{Fmt: "Port [%d] Set Fd Nonblock Error [%s]", Action: func(pp *PathPair) (err error) {
		return syscall.SetNonblock(pp.EFd, true)
	}},
	{Fmt: "Port [%d] Add Fd To Epoll Error [%s]", Action: func(pp *PathPair) (err error) {
		return syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_ADD, pp.EFd, &syscall.EpollEvent{Events: syscall.EPOLLIN | syscall.EPOLLOUT, Fd: int32(pp.EFd)})
	}},
	{Fmt: "Port [%d] Create New Socket Error [%s]", Action: func(pp *PathPair) (err error) {
		pp.IFd, err = syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
		return
	}},
	{Fmt: "Port [%d] Connect To Inner Port Error [%s]", Action: func(pp *PathPair) (err error) {
		p := pp.RelPath
		return syscall.Connect(pp.IFd, &syscall.SockaddrInet4{Port: p.IPort, Addr: [4]byte{p.IIp[0], p.IIp[1], p.IIp[2], p.IIp[3]}})
	}},
	{Fmt: "Port [%d] Set Inner Fd Nonblock Error [%s]", Action: func(pp *PathPair) (err error) {
		return syscall.SetNonblock(pp.IFd, true)
	}},
	{Fmt: "Port [%d] Add Inner Fd To Epoll Error [%s]", Action: func(pp *PathPair) (err error) {
		return syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_ADD, pp.IFd, &syscall.EpollEvent{Events: syscall.EPOLLIN | syscall.EPOLLOUT, Fd: int32(pp.IFd)})
	}},
}

func (p *Path) newPair() (err error) {
	pp := PathPair{RelPath: p}
	p.Pairs = append(p.Pairs, &pp)
	for _, step := range pathAddStep {
		err = step.Action(&pp)
		if err != nil {
			logger.Log(logger.ERR, step.Fmt, p.EPort, err)
			return
		}
	}
	if time.Now().Day() > p.CheckTime.Day() {
		p.CheckTime.Add(time.Duration((time.Now().Day() - p.CheckTime.Day())) * time.Hour)
		p.PassSize = 0
	} else if p.PassSize > p.BlockSize {
		logger.Log(logger.INFO, "Block Port [%d]", p.EPort)
		return pp.shutdown()
	}
	fdPathPair[pp.IFd], fdPathPair[pp.EFd] = &pp, &pp
	return
}
