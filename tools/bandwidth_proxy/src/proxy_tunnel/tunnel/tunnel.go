package tunnel

import (
	"net"
	"proxy_tunnel/logger"
	"syscall"
	"time"
)

type Tunnel struct {
	EPort     uint
	IPort     uint
	IIp       net.IP
	PassSize  uint64
	LFd       int
	Limit     uint64
	Window    uint
	CheckTime time.Time
}

type InitStep struct {
	ErrFmt string
	Action func(*Tunnel) error
}

var initStep = [...]InitStep{
	{ErrFmt: "Create epoll fd error [%s]\n",
		Action: func(t *Tunnel) (err error) {
			t.LFd, err = syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
			return err
		}},
	{ErrFmt: "Bind epoll fd error [%s]\n",
		Action: func(t *Tunnel) error {
			return syscall.Bind(t.LFd, &syscall.SockaddrInet4{Port: int(t.EPort), Addr: [4]byte{0, 0, 0, 0}})
		}},
	{ErrFmt: "Listen fd error [%s]\n",
		Action: func(t *Tunnel) error {
			return syscall.Listen(t.LFd, 10)
		}},
	{ErrFmt: "Add fd to epoll error [%s]\n",
		Action: func(t *Tunnel) error {
			return syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_ADD, t.LFd, &syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(t.LFd)})
		}},
}

var pathAddStep = [...]TunnelStep{
	{ErrFmt: "Port [%d] accept error [%s]", Action: func(tc *TunnelConn) (err error) {
		tc.EFd, _, err = syscall.Accept(tc.RelTunnel.LFd)
		return
	}},
	{ErrFmt: "Port [%d] set fd nonblock error [%s]", Action: func(tc *TunnelConn) (err error) {
		return syscall.SetNonblock(tc.EFd, true)
	}},
	{ErrFmt: "Port [%d] add fd to epoll error [%s]", Action: func(tc *TunnelConn) (err error) {
		return syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_ADD, tc.EFd, &syscall.EpollEvent{Events: syscall.EPOLLIN | syscall.EPOLLOUT, Fd: int32(tc.EFd)})
	}},
	{ErrFmt: "Port [%d] create new socket error [%s]", Action: func(tc *TunnelConn) (err error) {
		tc.IFd, err = syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
		return
	}},
	{ErrFmt: "Port [%d] connect to inner port error [%s]", Action: func(tc *TunnelConn) (err error) {
		t := tc.RelTunnel
		return syscall.Connect(tc.IFd, &syscall.SockaddrInet4{Port: int(t.IPort), Addr: [4]byte{t.IIp[12], t.IIp[13], t.IIp[14], t.IIp[15]}})
	}},
	{ErrFmt: "Port [%d] set inner fd nonblock error [%s]", Action: func(tc *TunnelConn) (err error) {
		return syscall.SetNonblock(tc.IFd, true)
	}},
	{ErrFmt: "Port [%d] add inner fd to epoll error [%s]", Action: func(tc *TunnelConn) (err error) {
		return syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_ADD, tc.IFd, &syscall.EpollEvent{Events: syscall.EPOLLIN | syscall.EPOLLOUT, Fd: int32(tc.IFd)})
	}},
}

func (t *Tunnel) newConn() {
	tc := TunnelConn{RelTunnel: t}
	for _, step := range pathAddStep {
		err := step.Action(&tc)
		if err != nil {
			logger.Log(logger.ERR, step.ErrFmt, t.EPort, err)
		}
	}
	timePassed := uint(time.Now().Sub(t.CheckTime).Seconds())
	if timePassed > t.Window {
		logger.Log(logger.INFO, "Resume port [%d] Capacity [%d]", t.EPort, t.Limit)
		t.CheckTime = t.CheckTime.Add(time.Duration(timePassed/t.Window*t.Window) * time.Second)
		t.PassSize = 0
	} else if t.PassSize > t.Limit {
		logger.Log(logger.INFO, "Block port [%d]", t.EPort)
		tc.shutdown()
	}
	fdTunnelConn[tc.IFd], fdTunnelConn[tc.EFd] = &tc, &tc
}
