package proxy

import (
	"io"
	"net"
)

/*
* TCP packet length is limited by the 'window size' field in TCP packet header
* which is a 16-bit integer value, that is to say, the maximum size of each
* TCP packet payload is 64K.
 */
const BUFFER_SIZE = 64 * 1024

type Session interface {
	Process()
	Shutdown()
}

type ProxySessionImpl struct {
	clientconn     *net.TCPConn
	serverconn     *net.TCPConn
	filter         Filter
	clientshutdown chan bool
	servershutdown chan bool
}

func (session *ProxySessionImpl) Process() {
	go session.ForwardClientMsg()
	go session.ForwardServerMsg()
}

func (session *ProxySessionImpl) ForwardClientMsg() {
	buffer := make([]byte, BUFFER_SIZE)

	clientfd := session.clientconn
	serverfd := session.serverconn
	filter := session.filter

	for {
		select {
		case <-session.clientshutdown:
			break
		default:
		}

		nread, err := clientfd.Read(buffer)
		if err != nil {
			if err == io.EOF {
				logger.Debug("TCP session with mongodb client will be closed soon.")
				break
			}
			logger.Debug("TCP read from client error: [%v].", err)
			continue
		}

		// filter process
		if filter.FilterEnabled() && !filter.PassFilter() {
			logger.Debug("TCP session with mongodb client is blocked by filter.")
			break
		}

		nwrite, err := serverfd.Write(buffer[0:nread])
		if err != nil || nwrite < nread {
			// TODO: error detection & handling
			logger.Debug("TCP write to server error: [%v].", err)
			continue
		}
	}

	// TCP connection half disconnection
	clientfd.CloseRead()
	serverfd.CloseWrite()

	logger.Debug("ForwardClientMsg go routine exits.")
}

func (session *ProxySessionImpl) ForwardServerMsg() {
	buffer := make([]byte, BUFFER_SIZE)

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
			logger.Debug("TCP read from server error: [%v].", err)
			continue
		}

		nwrite, err := clientfd.Write(buffer[0:nread])
		if err != nil || nwrite < nread {
			// TODO: error detection & handling
			logger.Debug("TCP write to client error: [%v].", err)
			continue
		}
	}

	// TCP connection half disconnection
	serverfd.CloseRead()
	clientfd.CloseWrite()

	logger.Debug("ForwardServerMsg go routine exits.")
}

func (session *ProxySessionImpl) Shutdown() {
	session.clientshutdown <- true
	session.servershutdown <- true
}

func NewSession(clientfd *net.TCPConn, serverfd *net.TCPConn, f Filter) *ProxySessionImpl {
	return &ProxySessionImpl{
		clientconn:     clientfd,
		serverconn:     serverfd,
		filter:         f,
		clientshutdown: make(chan bool, 1),
		servershutdown: make(chan bool, 1)}
}
