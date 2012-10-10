package proxy

import (
	"syscall"
)

const NO_ERROR = 0
const READ_ERROR = -1
const WRITE_ERROR = -2
const SESSION_EOF = -3
const FILTER_BLOCK = -4
const UNKNOWN_ERROR = -1024

const MAX_LISTEN_BACKLOG = 100
const BUFFER_SIZE = 512 // TODO: buffer size tuning ???

type IOSocketPeer struct {
	clientfd int // TCP connection with mongo client
	serverfd int // TCP connection with mongo server

	conninfo syscall.Sockaddr // conection peer

	recvpacket func(*NetIOManager, int) int
	sendpacket func(*NetIOManager, int) int
}

type OutputQueue struct {
	packet []byte
	stream []byte
}

type NetIOManager struct {
	max_backlog         int
	skb                 []byte
	io_socket_peers     map[int]*IOSocketPeer
	pending_output_skbs map[int]*OutputQueue
	epoll_fd            int
	proxy_server_fd     int
	filter              *IOFilterProtocol
}

func NewNetIOManager() *NetIOManager {
	io_manager := &NetIOManager{
		max_backlog:         MAX_LISTEN_BACKLOG,
		skb:                 make([]byte, BUFFER_SIZE),
		io_socket_peers:     make(map[int]*IOSocketPeer),
		pending_output_skbs: make(map[int]*OutputQueue),
		epoll_fd:            -1,
		proxy_server_fd:     -1,
		filter:              nil,
	}
	return io_manager
}

func (io *NetIOManager) SetFilter(filter *IOFilterProtocol) *NetIOManager {
	io.filter = filter
	return io
}

func (io *NetIOManager) SetEpollFd(fd int) *NetIOManager {
	io.epoll_fd = fd
	return io
}

func (io *NetIOManager) ProxyNetIsProxyServer(fd int) bool {
	return fd == io.proxy_server_fd
}

func (io *NetIOManager) ProxyNetConnInfo(fd int) (sa syscall.Sockaddr) {
	if io.io_socket_peers[fd] != nil {
		return io.io_socket_peers[fd].conninfo
	}
	return nil
}

func (io *NetIOManager) ProxyNetListen(sa syscall.Sockaddr) error {
	serverfd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM,
		syscall.IPPROTO_TCP)
	if err != nil {
		goto Error
	}

	err = syscall.Bind(serverfd, sa)
	if err != nil {
		goto Cleanup
	}

	err = syscall.Listen(serverfd, io.max_backlog)
	if err != nil {
		goto Cleanup
	}

	err = syscall.EpollCtl(io.epoll_fd, syscall.EPOLL_CTL_ADD, serverfd,
		&syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(serverfd)})
	if err != nil {
		goto Cleanup
	}

	io.proxy_server_fd = serverfd
	return nil

Cleanup:
	syscall.Close(serverfd)
Error:
	return err
}

func (io *NetIOManager) ProxyNetAccept(serverinfo syscall.Sockaddr) (sa syscall.Sockaddr, err error) {
	var clientfd, serverfd int
	// accpet mongodb client connection request
	clientfd, clientinfo, err := syscall.Accept(io.proxy_server_fd)
	if err != nil {
		goto ClientError
	}

	err = syscall.SetNonblock(clientfd, true)
	if err != nil {
		goto ClientCleanup
	}

	err = syscall.EpollCtl(io.epoll_fd, syscall.EPOLL_CTL_ADD, clientfd,
		&syscall.EpollEvent{Events: syscall.EPOLLIN | syscall.EPOLLOUT |
			syscall.EPOLLRDHUP, Fd: int32(clientfd)})
	if err != nil {
		goto ClientCleanup
	}

	// establish connection with mongodb server
	serverfd, err = syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM,
		syscall.IPPROTO_TCP)
	if err != nil {
		goto ServerError
	}

	err = syscall.Connect(serverfd, serverinfo)
	if err != nil {
		goto ServerCleanup
	}

	err = syscall.SetNonblock(serverfd, true)
	if err != nil {
		goto ServerCleanup
	}

	err = syscall.EpollCtl(io.epoll_fd, syscall.EPOLL_CTL_ADD, serverfd,
		&syscall.EpollEvent{Events: syscall.EPOLLIN | syscall.EPOLLOUT |
			syscall.EPOLLRDHUP, Fd: int32(serverfd)})
	if err != nil {
		goto ServerCleanup
	}

	// now proxy server becomes a bridge between client <-> server
	add_sock_peer(io, clientfd, clientinfo, serverfd, serverinfo)
	return clientinfo, nil

ServerCleanup:
	syscall.Close(serverfd)
ServerError:
	syscall.EpollCtl(io.epoll_fd, syscall.EPOLL_CTL_DEL, clientfd,
		&syscall.EpollEvent{Events: syscall.EPOLLIN | syscall.EPOLLOUT |
			syscall.EPOLLRDHUP, Fd: int32(clientfd)})
ClientCleanup:
	syscall.Close(clientfd)
ClientError:
	return nil, err
}

func (io *NetIOManager) DestroyNetIO() {
	for fd := range io.pending_output_skbs {
		delete(io.pending_output_skbs, fd)
	}
	for fd := range io.io_socket_peers {
		delete(io.io_socket_peers, fd)
	}

	if io.proxy_server_fd != -1 {
		syscall.Close(io.proxy_server_fd)
	}
	if io.epoll_fd != -1 {
		syscall.Close(io.epoll_fd)
	}
	if io.filter != nil {
		io.filter.DestroyFilter()
	}
}

func (io *NetIOManager) ProxyNetClosePeers(fd int) {
	if _, ok := io.io_socket_peers[fd]; ok {
		var peerfd int
		if fd == io.io_socket_peers[fd].clientfd {
			peerfd = io.io_socket_peers[fd].serverfd
		} else {
			peerfd = io.io_socket_peers[fd].clientfd
		}
		sock_close(io, fd)
		sock_close(io, peerfd)
	}
}

func (io *NetIOManager) ProxyNetSend(fd int) (errno int) {
	if io.io_socket_peers[fd] != nil {
		return io.io_socket_peers[fd].sendpacket(io, fd)
	}
	return NO_ERROR
}

func (io *NetIOManager) ProxyNetRecv(fd int) (errno int) {
	if io.io_socket_peers[fd] != nil {
		return io.io_socket_peers[fd].recvpacket(io, fd)
	}
	return NO_ERROR
}

/******************************************/
/*                                        */
/*    network read/write io routines      */
/*                                        */
/******************************************/

func skb_read(io *NetIOManager, fd int) (errno int) {
	for {
		num, err := syscall.Read(fd, io.skb)

		if num < 0 && err != nil {
			if err == syscall.EAGAIN {
				return NO_ERROR
			} else if err == syscall.EWOULDBLOCK {
				return NO_ERROR
			} else if err == syscall.EINTR {
				// TODO: anything to do??? retry???
				return READ_ERROR
			} else {
				return READ_ERROR
			}
		} else if num == 0 {
			return SESSION_EOF
		} else {
			// append skb into peer fd's output queue
			var peerfd int
			if fd == io.io_socket_peers[fd].clientfd {
				peerfd = io.io_socket_peers[fd].serverfd
			} else {
				peerfd = io.io_socket_peers[fd].clientfd
			}
			skb_enqueue_output(io, peerfd, io.skb[0:num])

			if num < len(io.skb) {
				break
			}
		}
	}
	return NO_ERROR
}

func skb_write_with_filter(io *NetIOManager, fd int) (errno int) {
	nwrite := 0
	if pending, ok := io.pending_output_skbs[fd]; ok {
		if len(pending.packet) > 0 {
			/*
			 * NOTE: We must get mongodb protocol packet one-by-one from the
			 *       output queue, then we can parse the packet header to
			 *       block insert/update operations if need.
			 */
			nwrite, err := syscall.Write(fd, pending.packet)
			if nwrite < 0 && err != nil {
				if err == syscall.EAGAIN {
					return NO_ERROR
				} else if err == syscall.EWOULDBLOCK {
					return NO_ERROR
				} else if err == syscall.EINTR {
					//TODO: anything to do??? retry???
					return WRITE_ERROR
				} else {
					return WRITE_ERROR
				}
			} else if nwrite == 0 {
				return NO_ERROR
			} else {
				io.pending_output_skbs[fd].packet = pending.packet[nwrite:len(pending.packet)]
				if len(io.pending_output_skbs[fd].packet) > 0 {
					return NO_ERROR
				}
			}
		}

		for {
			message_length, op_code := io.filter.HandleMsgHeader(
				io.pending_output_skbs[fd].stream)
			if message_length > 0 {
				if !io.filter.PassFilter(op_code) {
					// block operation
					return FILTER_BLOCK
				}

				num, err := syscall.Write(fd, io.pending_output_skbs[fd].
					stream[0:message_length])
				if num < 0 && err != nil {
					if err == syscall.EAGAIN {
						return NO_ERROR
					} else if err == syscall.EWOULDBLOCK {
						return NO_ERROR
					} else if err == syscall.EINTR {
						//TODO: anything to do??? retry???
						return WRITE_ERROR
					} else {
						return WRITE_ERROR
					}
				} else if num == 0 {
					return NO_ERROR
				} else {
					nwrite += num
					skb_dequeue_output(io, fd, int(message_length))
					if num < int(message_length) {
						add_partial_skb(io, fd, io.pending_output_skbs[fd].
							stream[num:message_length])
						return NO_ERROR
					}
				}
			} else {
				break
			}
		}
	}
	return NO_ERROR
}

func skb_write_without_filter(io *NetIOManager, fd int) (errno int) {
	if pending, ok := io.pending_output_skbs[fd]; ok {
		num, err := syscall.Write(fd, pending.stream)
		if num < 0 && err != nil {
			if err == syscall.EAGAIN {
				return NO_ERROR
			} else if err == syscall.EWOULDBLOCK {
				return NO_ERROR
			} else if err == syscall.EINTR {
				//TODO: anything to do??? retry???
				return WRITE_ERROR
			} else {
				return WRITE_ERROR
			}
		} else if num == 0 {
			return NO_ERROR
		} else {
			skb_dequeue_output(io, fd, num)
			return NO_ERROR
		}
	}
	return NO_ERROR
}

/******************************************/
/*                                        */
/*       Internel Support Routines        */
/*                                        */
/******************************************/

func skb_enqueue_output(io *NetIOManager, fd int, data []byte) {
	if pending, ok := io.pending_output_skbs[fd]; ok {
		io.pending_output_skbs[fd].stream = append(pending.stream, data)
	} else {
		io.pending_output_skbs[fd] = &OutputQueue{make([]byte, 0), data}
	}
}

func skb_dequeue_output(io *NetIOManager, fd int, num int) {
	if pending, ok := io.pending_output_skbs[fd]; ok {
		io.pending_output_skbs[fd].stream = pending.stream[num:]
	}
}

func add_partial_skb(io *NetIOManager, fd int, data []byte) {
	if _, ok := io.pending_output_skbs[fd]; ok {
		io.pending_output_skbs[fd].packet = data
	} else {
		io.pending_output_skbs[fd] = &OutputQueue{data, make([]byte, 0)}
	}
}

func append(skb1, skb2 []byte) (skb []byte) {
	newskb := make([]byte, len(skb1)+len(skb2))
	copy(newskb, skb1)
	copy(newskb[len(skb1):], skb2)
	return newskb
}

func add_sock_peer(io *NetIOManager,
	clientfd int, clientinfo syscall.Sockaddr,
	serverfd int, serverinfo syscall.Sockaddr) {
	var server_peer IOSocketPeer
	if io.filter.FilterEnabled() {
		/*
		 * NOTE: We only filter request from mongo client to mongo server
		 *       when filter is enabled.
		 */
		server_peer = IOSocketPeer{clientfd, serverfd, clientinfo, skb_read,
			skb_write_with_filter}
	} else {
		server_peer = IOSocketPeer{clientfd, serverfd, clientinfo, skb_read,
			skb_write_without_filter}
	}
	client_peer := IOSocketPeer{clientfd, serverfd, serverinfo, skb_read,
		skb_write_without_filter}
	io.io_socket_peers[clientfd] = &client_peer
	io.io_socket_peers[serverfd] = &server_peer
}

func sock_close(io *NetIOManager, fd int) {
	syscall.EpollCtl(io.epoll_fd, syscall.EPOLL_CTL_DEL, fd,
		&syscall.EpollEvent{Events: syscall.EPOLLIN | syscall.EPOLLOUT |
			syscall.EPOLLRDHUP, Fd: int32(fd)})
	syscall.Close(fd)
	delete(io.pending_output_skbs, fd)
	delete(io.io_socket_peers, fd)
}
