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
		tc.RelTunnel.PassSize += uint64(num)
		writeCache[fd] = out[num:]
	} else if err == nil {
		tc.RelTunnel.PassSize += uint64(num)
		delete(writeCache, fd)
	}
}

func (tc *TunnelConn) readOnce(fd int) (num int, err error) {
	num, err = syscall.Read(fd, buf)
	if num == 0 || err != nil && err != syscall.EAGAIN {
		tc.shutdown()
	}
	return
}

func (tc *TunnelConn) getPeerFd(fd int) int {
	var otherFd int
	if fd == tc.EFd {
		otherFd = tc.IFd
	} else {
		otherFd = tc.EFd
	}
	return otherFd
}

func getToSend(fd int, read []byte) (ret []byte) {
	ret = read
	_, ok := writeCache[fd]
	if ok {
		ret = merge(writeCache[fd], read)
	}
	return
}

func (tc *TunnelConn) writeOnce(fd int, content []byte) (sent int, left []byte, err error) {
	sent, err = syscall.Write(fd, content)
	if err != nil && err != syscall.EAGAIN {
		logger.Log(logger.ERR, "Write fd [%d] send num [%d] ret [%d] error [%s]", fd, len(content), sent, err)
		tc.shutdown()
		return
	}
	if sent > 0 && sent < len(content) {
		left = content[sent:]
	} else if sent <= 0 && err == syscall.EAGAIN {
		sent = 0
		left = content
		err = nil
	}
	return
}

func saveLeft(fd int, left []byte) {
	_, ok := writeCache[fd]
	if !ok {
		writeCache[fd] = make([]byte, len(left), len(left))
		copy(writeCache[fd], left)
	} else {
		writeCache[fd] = left
	}
}

func (tc *TunnelConn) handleIn(fd int) {
	for num := len(buf); num == len(buf); {
		var err error
		num, err = tc.readOnce(fd)
		if num <= 0 || err != nil {
			return
		}

		targetFd := tc.getPeerFd(fd)
		toSend := getToSend(targetFd, buf[:num])
		sent, left, err := tc.writeOnce(targetFd, toSend)
		if err != nil {
			return
		}

		tc.RelTunnel.PassSize += uint64(sent)
		if left != nil {
			saveLeft(targetFd, left)
			continue
		}
		if _, ok := writeCache[targetFd]; ok {
			delete(writeCache, targetFd)
		}
		if tc.RelTunnel.PassSize > tc.RelTunnel.Limit {
			logger.Log(logger.INFO, "Block port [%d]", tc.RelTunnel.EPort)
			tc.shutdown()
			return
		}
	}
}
