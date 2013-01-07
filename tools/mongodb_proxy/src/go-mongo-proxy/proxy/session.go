package proxy

import (
	"io"
	"net"
	"sync"
)

/*
 * TCP packet length is limited by the 'window size' field in TCP packet header
 * which is a 16-bit integer value, that is to say, the maximum size of each
 * TCP packet payload is 64K.
 */
const BUFFER_SIZE = 64 * 1024

type Session interface {
	Reset(net.Conn, net.Conn, Filter)
	GetSid() int32
	Process()
	WaitForFinish()
}

type SessionManager interface {
	NewSession(net.Conn, net.Conn, Filter) Session
	WaitAllFinish()
	MarkIdle(Session)
}

type ProxySessionImpl struct {
	manager SessionManager

	sid            int32
	clientconn     net.Conn
	serverconn     net.Conn
	filter         Filter
	clientshutdown chan byte
	servershutdown chan byte

	// goroutine wait channel
	lock    sync.Mutex
	running uint32
	wait    chan byte
}

// A simple session manager
type ProxySessionManagerImpl struct {
	actives map[int32]Session // active sessions
	idles   map[int32]Session // idle sessions

	sid  int32 // session id allocator, currently int32 length is enough
	lock sync.Mutex
}

func (session *ProxySessionImpl) Process() {
	go session.ForwardClientMsg()
	go session.ForwardServerMsg()
}

func (session *ProxySessionImpl) Reset(clientfd net.Conn, serverfd net.Conn, f Filter) {
	// session id will never change after allocation
	session.clientconn = clientfd
	session.serverconn = serverfd
	session.filter = f
	session.clientshutdown = make(chan byte, 1)
	session.servershutdown = make(chan byte, 1)
	session.running = 0
	session.wait = make(chan byte, 1)
}

func (session *ProxySessionImpl) GetSid() int32 {
	return session.sid
}

// state machine for client request process
const START_PROCESS_REQUEST = 0
const READ_REQUEST_HEADER = 1
const READ_REQUEST_BODY = 2

func (session *ProxySessionImpl) ForwardClientMsg() {
	var buffer Buffer
	var current_pkt_op, current_pkt_remain_len int
	var nread, nwrite, length int
	var err error

	buffer = NewBuffer(BUFFER_SIZE)
	current_pkt_op = OP_UNKNOWN
	current_pkt_remain_len = 0
	nread = 0
	nwrite = 0
	length = 0
	err = nil

	session.lock.Lock()
	session.running++
	session.lock.Unlock()

	clientfd := session.clientconn
	serverfd := session.serverconn
	filter := session.filter

	state := START_PROCESS_REQUEST
	for {
		select {
		case <-session.clientshutdown:
			break
		default:
		}

		switch state {
		case START_PROCESS_REQUEST:
			buffer.ResetCursor()
			length = buffer.RemainSpace()
		case READ_REQUEST_HEADER:
			length = buffer.RemainSpace()
		case READ_REQUEST_BODY:
			length = current_pkt_remain_len
			if length != 0 {
				buffer.ResetCursor()
				if length > buffer.RemainSpace() {
					length = buffer.RemainSpace()
				}
			} else {
				state = START_PROCESS_REQUEST
				continue
			}
		}

		/*
		 * Refer to Golang src/pkg/net/fd.go#L416
		 *
		 * Here fd is NONBLOCK, but Golang has handled EAGAIN/EOF within Read function.
		 */
		nread, err = clientfd.Read(buffer.LimitedCursor(length))
		if err != nil {
			if err == io.EOF {
				logger.Debugf("TCP session with mongodb client will be closed soon.")
				break
			}
			logger.Warnf("TCP read from client error: [%v].", err)
			break
		}

		switch state {
		case START_PROCESS_REQUEST:
			state = READ_REQUEST_HEADER
			fallthrough
		case READ_REQUEST_HEADER:
			buffer.ForwardCursor(nread)
			if len(buffer.Data()) < STANDARD_HEADER_SIZE {
				// Process further only when we have seen complete mongodb packet header,
				// whose length is 16 bytes.
				continue
			} else {
				pkt_len, op_code := parseMsgHeader(buffer.Data())
				current_pkt_op = int(op_code)
				current_pkt_remain_len = int(pkt_len)

				state = READ_REQUEST_BODY
			}
		case READ_REQUEST_BODY:
			buffer.ForwardCursor(nread)
		}

		if filter.FilterEnabled() && filter.IsDirtyEvent(current_pkt_op) {
			filter.EnqueueDirtyEvent()
		}

		// filter process
		if filter.FilterEnabled() && !filter.PassFilter(current_pkt_op) {
			logger.Error("TCP session with mongodb client is blocked by filter.")
			break
		}

		/*
		 * Refer to Golang src/pkg/net/fd.go#L503
		 *
		 * Here fd is NONBLOCK, but the Write function ensure 'ALL' bytes will be sent out unless
		 * there is something wrong.
		 */
		nwrite, err = serverfd.Write(buffer.Data())
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				logger.Debugf("TCP session with mongodb server encounter unexpected EOF: [%v].", err)
				break
			}
			logger.Warnf("TCP write to server error: [%v].", err)
			break
		}

		current_pkt_remain_len -= nwrite
		/*
		 * One corner case
		 *
		 * If a malformed application establishes a 'RAW' tcp connection to our proxy, then
		 * this application may fill up the packet header length to be M, while the real packet
		 * length is N and N > M, we must prevent this case.
		 */
		if current_pkt_remain_len < 0 {
			current_pkt_remain_len = 0
		}
	}

	// In theory it's better to half disconnection here. However, tcp shutdown recv half actually
	// does nothing, see linux kernel tcp_shutdown implementation. While tcp shutdown send half
	// actually send a FIN packet, however, if client overlooks this notification then client TCP
	// socket will hang at CLOSE_WAIT state until TCP keepalive probe starts. Client may feel panic
	// because CLOSE_WAIT socket is usually seen at server side.
	//
	// Here we call Close to shutdown both send and recv sides, this will impact 'ForwardServerMsg'
	// goroutine, it will encounter a 'Closed Connection' error. Well, this is not a big issue, it
	// is just a signal to tell 'ForwardServerMsg' goroutine to exit.
	clientfd.Close()
	serverfd.Close()

	session.lock.Lock()
	session.running--
	if session.running == 0 {
		session.wait <- STOP_EVENT
		session.manager.MarkIdle(session)
	}
	session.lock.Unlock()

	logger.Debug("ForwardClientMsg go routine exits.")
}

func (session *ProxySessionImpl) ForwardServerMsg() {
	buffer := make([]byte, BUFFER_SIZE)

	session.lock.Lock()
	session.running++
	session.lock.Unlock()

	clientfd := session.clientconn
	serverfd := session.serverconn

	for {
		select {
		case <-session.servershutdown:
			break
		default:
		}

		nread, err := serverfd.Read(buffer)
		if err != nil {
			if err == io.EOF {
				logger.Debug("TCP session with mongodb server will be closed soon.")
				break
			}
			logger.Warnf("TCP read from server error: [%v].", err)
			break
		}

		_, err = clientfd.Write(buffer[0:nread])
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				logger.Debugf("TCP session with mongodb client encounter unexpected EOF: [%v].", err)
				break
			}
			logger.Warnf("TCP write to client error: [%v].", err)
			break
		}
	}

	// In theory it's better to half disconnection here. However, tcp shutdown recv half actually
	// does nothing, see linux kernel tcp_shutdown implementation. While tcp shutdown send half
	// actually send a FIN packet, however, if client overlooks this notification then client TCP
	// socket will hang at CLOSE_WAIT state until TCP keepalive probe starts. Client may feel panic
	// because CLOSE_WAIT socket is usually seen at server side.
	//
	// Here we call Close to shutdown both send and recv sides, this will impact 'ForwardClientMsg'
	// goroutine, it will encounter a 'Closed Connection' error. Well, this is not a big issue, it
	// is just a signal to tell 'ForwardClientMsg' goroutine to exit.
	serverfd.Close()
	clientfd.Close()

	session.lock.Lock()
	session.running--
	if session.running == 0 {
		session.wait <- STOP_EVENT
		session.manager.MarkIdle(session)
	}
	session.lock.Unlock()

	logger.Debug("ForwardServerMsg go routine exits.")
}

func (session *ProxySessionImpl) WaitForFinish() {
	session.clientshutdown <- STOP_EVENT
	session.servershutdown <- STOP_EVENT
	wait := false
	session.lock.Lock()
	if session.running > 0 {
		wait = true
	}
	session.lock.Unlock()
	if wait {
		<-session.wait
	}
}

func (manager *ProxySessionManagerImpl) NewSession(clientfd net.Conn, serverfd net.Conn, f Filter) Session {
	var session Session
	var sid int32

	sid = -1
	manager.lock.Lock()
	for sid, session = range manager.idles {
		break
	}
	if sid >= 0 {
		delete(manager.idles, sid)
		manager.actives[sid] = session
	}
	manager.lock.Unlock()

	if sid >= 0 {
		session.Reset(clientfd, serverfd, f)
	} else {
		session = manager.SpawnSession(clientfd, serverfd, f)
	}

	return session
}

func (manager *ProxySessionManagerImpl) WaitAllFinish() {
	temp := make(map[int32]Session)

	manager.lock.Lock()
	for sid, session := range manager.idles {
		temp[sid] = session
	}
	manager.lock.Unlock()

	for _, session := range temp {
		session.WaitForFinish()
	}
}

// caller is responsible to handle critical section lock
func (manager *ProxySessionManagerImpl) MarkIdle(session Session) {
	if sid := session.GetSid(); sid >= 0 {
		delete(manager.actives, sid)
		manager.idles[sid] = session
	}
}

func (manager *ProxySessionManagerImpl) SpawnSession(clientfd net.Conn, serverfd net.Conn, f Filter) Session {
	var session Session
	var sid int32

	manager.lock.Lock()
	sid = manager.sid
	manager.sid++
	session = &ProxySessionImpl{
		manager:        manager,
		sid:            sid,
		clientconn:     clientfd,
		serverconn:     serverfd,
		filter:         f,
		clientshutdown: make(chan byte, 1),
		servershutdown: make(chan byte, 1),
		running:        0,
		wait:           make(chan byte, 1)}
	manager.actives[sid] = session
	manager.lock.Unlock()

	return session
}

func NewSessionManager() *ProxySessionManagerImpl {
	return &ProxySessionManagerImpl{
		actives: make(map[int32]Session),
		idles:   make(map[int32]Session),
		sid:     0}
}
