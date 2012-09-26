package tunnel

import (
	"proxy_tunnel/logger"
	"syscall"
)

type TunnelConn struct {
	EFd       int
	IFd       int
	RelTunnel *Tunnel
}

type TunnelStep struct {
	ErrFmt string
	Action func(*TunnelConn) error
}

var buf = make([]byte, 65536, 65536)
var writeCache map[int][]byte = make(map[int][]byte)

func (tc *TunnelConn) shutdown() {
	for _, fd := range [...]int{tc.IFd, tc.EFd} {
		syscall.Close(fd)
		delete(writeCache, fd)
		delete(fdTunnelConn, fd)
	}
}

func merge(s1, s2 []byte) (ret []byte) {
	ret = make([]byte, len(s1)+len(s2), len(s1)+len(s2))
	copy(ret, s1)
	copy(ret[len(s1):], s2)
	return ret
}

func (tc *TunnelConn) handleOut(fd int) {
	out, ok := writeCache[fd]
	if !ok {
		return
	}
	num, err := syscall.Write(fd, out)
	if err != nil && err != syscall.EAGAIN {
		logger.Log(logger.ERR, "Write cache to fd [%d] error [%s]", fd, err)
		tc.shutdown()
	} else if err == nil && num < len(out) {
		writeCache[fd] = out[num:]
	} else if err == nil {
		delete(writeCache, fd)
	}
}

func (tc *TunnelConn) handleIn(fd int) {
	for num := len(buf); num == len(buf); {
		var err error
		num, err = syscall.Read(fd, buf)
		if num == 0 || err != nil && err != syscall.EAGAIN {
			tc.shutdown()
			return
		} else if num < 0 && err == syscall.EAGAIN {
			return
		}
		var otherFd int
		if fd == tc.EFd {
			otherFd = tc.IFd
		} else {
			otherFd = tc.EFd
		}
		toSend := buf[:num]
		_, ok := writeCache[otherFd]
		if ok {
			toSend = merge(writeCache[otherFd], buf[:num])
		}
		numSend, err := syscall.Write(otherFd, toSend)
		if err != nil && err != syscall.EAGAIN {
			logger.Log(logger.ERR, "Write fd [%d] send num [%d] ret [%d] error [%s]", fd, num, numSend, err)
			tc.shutdown()
			return
		}
		var left []byte
		if numSend > 0 && numSend < len(toSend) {
			left = toSend[numSend:]
		} else if numSend <= 0 && err == syscall.EAGAIN {
			left = toSend
		}
		if left != nil {
			if !ok {
				writeCache[otherFd] = make([]byte, len(left), len(left))
				copy(writeCache[otherFd], left)
			} else {
				writeCache[otherFd] = left
			}
			continue
		}

		if ok && numSend > 0 {
			delete(writeCache, otherFd)
		}
		tc.RelTunnel.PassSize += uint64(num)
		if tc.RelTunnel.PassSize > tc.RelTunnel.BlockSize {
			logger.Log(logger.INFO, "Block port [%d]", tc.RelTunnel.EPort)
			tc.shutdown()
			return
		}
	}
}
