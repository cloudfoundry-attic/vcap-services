package proxy

import (
	"errors"
	"time"
)

type pair struct {
	err    error
	retval int
}

type AsyncOps struct {
	asyncread chan pair
}

var ErrTimeout = errors.New("timeout")

func (async *AsyncOps) AsyncRead(read func(int, []byte) (int, error), fd int, buf []byte, timeout time.Duration) (int, error) {
	t := time.NewTimer(timeout)
	defer t.Stop()

	if async.asyncread == nil {
		async.asyncread = make(chan pair, 1)

		go func() {
			nread, err := read(fd, buf)
			if err != nil {
				async.asyncread <- pair{err, -1}
			} else {
				async.asyncread <- pair{nil, nread}
			}
		}()
	}

	select {
	case p := <-async.asyncread:
		async.asyncread = nil
		return p.retval, p.err
	case <-t.C:
		return -1, ErrTimeout
	}
	panic("Oops, unreachable")
}

func NewAsyncOps() *AsyncOps {
	return &AsyncOps{
		asyncread: nil}
}
