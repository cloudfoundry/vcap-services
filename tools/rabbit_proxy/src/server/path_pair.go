package server

import (
	"logger"
	"syscall"
)

type PathPair struct {
	EFd     int
	IFd     int
	RelPath *Path
}

type PathStep struct {
	Fmt    string
	Action func(*PathPair) error
}

var buf = make([]byte, 2048, 2048)
var writeCache map[int][]byte = make(map[int][]byte)

func (pp *PathPair) shutdown() (err error) {
	for _, fd := range [...]int{pp.IFd, pp.EFd} {
		err = syscall.Close(fd)
		delete(writeCache, fd)
		delete(fdPathPair, fd)
	}
	return
}

func (pp *PathPair) handleOut(fd int) (err error) {
	out, ok := writeCache[fd]
	if !ok {
		return
	}
	num, err := syscall.Write(fd, out)
	logger.Log(logger.INFO, "Write Cached Fd [%d] [%d]", fd, num)
	if err == syscall.EAGAIN {
		return nil
	} else if err != nil {
		logger.Log(logger.ERR, "Write Cache To Fd [%d] Error [%s]", fd, err)
		return pp.shutdown()
	}
	return
}

func merge(s1, s2 []byte) (ret []byte) {
	ret = make([]byte, len(s1)+len(s2))
	copy(ret, s1)
	copy(ret[len(s1):], s2)
	return
}

func (pp *PathPair) handleIn(fd int) (err error) {
	for num := len(buf); num == len(buf); {
		num, err = syscall.Read(fd, buf)
		if num <= 0 || err != nil && err != syscall.EAGAIN {
			return pp.shutdown()
		}
		var otherFd int
		if fd == pp.EFd {
			otherFd = pp.IFd
		} else {
			otherFd = pp.EFd
		}
		_, err = syscall.Write(otherFd, buf[:num])
		if err == syscall.EAGAIN {
			_, ok := writeCache[otherFd]
			if !ok {
				writeCache[otherFd] = make([]byte, 0)
			}
			writeCache[otherFd] = merge(writeCache[otherFd], buf[:num])
		} else if err != nil {
			logger.Log(logger.ERR, "Write Fd [%d] Error [%s]", fd, err)
			return pp.shutdown()
		}
		pp.RelPath.PassSize += uint64(num)
		if pp.RelPath.PassSize > pp.RelPath.BlockSize {
			logger.Log(logger.ERR, "Block Port [%d]", pp.RelPath.EPort)
			return pp.shutdown()
		}
	}
	return
}
