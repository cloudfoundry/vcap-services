package proxy

import (
	"errors"
	"time"
)

type pair struct {
	err    error
	retval int
}

var asyncread chan pair

var ErrTimeout = errors.New("timeout")

func asyncRead(read func(int, []byte) (int, error), fd int, buf []byte, timeout time.Duration) (int, error) {
	t := time.NewTimer(timeout)
	defer t.Stop()

	if asyncread == nil {
		asyncread = make(chan pair, 1)

		go func() {
			nread, err := read(fd, buf)
			if err != nil {
				asyncread <- pair{err, -1}
			} else {
				asyncread <- pair{nil, nread}
			}
		}()
	}

	select {
	case p := <-asyncread:
		asyncread = nil
		return p.retval, p.err
	case <-t.C:
		return -1, ErrTimeout
	}
	panic("Oops, unreachable")
}
