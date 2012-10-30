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
const BUFFER_SIZE = 1024 * 1024 // 1M buffer size

type IOSocketPeer struct {
	clientfd int // TCP connection with mongo client
	serverfd int // TCP connection with mongo server

	conninfo syscall.Sockaddr // conection

	recvpacket func(*NetIOManager, int) int
	sendpacket func(*NetIOManager, int) int
}

type OutputQueue struct {
	// current mongo client -> server packets
	current_packet_op            int32
	current_packet_remain_length int32
	// ring buffer
	write_offset   int32
	read_offset    int32
	available_size int32
	stream         []byte
}

type NetIOManager struct {
	max_backlog         int
	io_socket_peers     map[int]*IOSocketPeer
	pending_output_skbs map[int]*OutputQueue
	epoll_fd            int
	proxy_server_fd     int
	filter              *IOFilterProtocol
}

func NewNetIOManager() *NetIOManager {
	io_manager := &NetIOManager{
		max_backlog:         MAX_LISTEN_BACKLOG,
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
	serverfd, err = syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM,
		0)
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
	// client -> server channel buffer
	alloc_skb_buffer(io, clientfd)
	// server -> client channel buffer
	alloc_skb_buffer(io, serverfd)
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
	var peerfd int

	if peers, ok := io.io_socket_peers[fd]; ok {
		if fd == peers.clientfd {
			peerfd = peers.serverfd
		} else {
			peerfd = peers.clientfd
		}
	} else {
		return UNKNOWN_ERROR
	}

	if pending, ok := io.pending_output_skbs[peerfd]; ok {
		if pending.available_size <= 0 {
			return NO_ERROR
		}

		// determine the maximum size which can be recv
		start_offset := pending.write_offset
		end_offset := start_offset + pending.available_size
		if end_offset > BUFFER_SIZE {
			end_offset = BUFFER_SIZE
		}
		num, error := do_skb_read(fd, pending.stream[start_offset:end_offset])
		if (error == NO_ERROR) && (num > 0) {
			pending.write_offset = (start_offset + num) % BUFFER_SIZE
			pending.available_size -= num
			if num < (end_offset - start_offset) {
				return NO_ERROR
			}
		} else {
			return error
		}

		if pending.available_size <= 0 {
			return NO_ERROR
		}

		// wrap around the ring buffer
		start_offset = pending.write_offset
		end_offset = start_offset + pending.available_size
		num, error = do_skb_read(fd, pending.stream[start_offset:end_offset])
		if (error == NO_ERROR) && (num > 0) {
			pending.write_offset = start_offset + num
			pending.available_size -= num
			return NO_ERROR
		} else {
			return error
		}
	}
	return UNKNOWN_ERROR
}

func skb_write_with_filter(io *NetIOManager, fd int) (errno int) {
	if pending, ok := io.pending_output_skbs[fd]; ok {
		for {
			if pending.current_packet_remain_length == 0 {
				var packet_header []byte

				if BUFFER_SIZE-pending.available_size < STANDARD_HEADER_SIZE {
					return NO_ERROR
				}

				start_offset := pending.read_offset
				end_offset := pending.read_offset + BUFFER_SIZE - pending.available_size
				if end_offset > BUFFER_SIZE {
					end_offset = BUFFER_SIZE
				}

				// determine whether the left size is larger than STANDARD_HEADER_SIZE
				if end_offset-start_offset >= STANDARD_HEADER_SIZE {
					packet_header = pending.stream[start_offset:(start_offset + STANDARD_HEADER_SIZE)]
				} else {
					// wrap around the ring buffer
					packet_header = make([]byte, STANDARD_HEADER_SIZE)
					copy(packet_header, pending.stream[start_offset:end_offset])
					copy(packet_header[end_offset:], pending.stream[0:(STANDARD_HEADER_SIZE+start_offset-end_offset)])
				}

				/*
				 * NOTE: We must get mongodb protocol packet one-by-one from the
				 *       ring buffer, then we can parse the packet header to
				 *       block insert/update operations if need.
				 */
				message_length, op_code := io.filter.HandleMsgHeader(packet_header)
				if message_length > 0 {
					pending.current_packet_op = op_code
					pending.current_packet_remain_length = message_length
				} else {
					pending.current_packet_op = OP_UNKNOWN
					pending.current_packet_remain_length = 0
				}
			}

			if !io.filter.PassFilter(pending.current_packet_op) {
				// block operation
				return FILTER_BLOCK
			}

			// Figure out real buffered data size
			sendlen := pending.current_packet_remain_length
			if BUFFER_SIZE-pending.available_size < sendlen {
				sendlen = BUFFER_SIZE - pending.available_size
			}

			/*
			 * NOTE: Do not enter into 'write' system call if there is nothing
			 *       to transmit, system call is expensive.
			 */
			if sendlen <= 0 {
				return NO_ERROR
			}

			// determine the maximum size which can be send
			start_offset := pending.read_offset
			end_offset := pending.read_offset + sendlen
			if end_offset > BUFFER_SIZE {
				end_offset = BUFFER_SIZE
			}
			num, error := do_skb_write(fd, pending.stream[start_offset:end_offset])
			if (error == NO_ERROR) && (num > 0) {
				pending.read_offset = (start_offset + num) % BUFFER_SIZE
				pending.available_size += num
				sendlen -= num

				pending.current_packet_remain_length -= num

				if num < (end_offset - start_offset) {
					return NO_ERROR
				}
			} else {
				return error
			}

			if sendlen > 0 {
				// wrap around the ring buffer
				start_offset = pending.read_offset
				end_offset = start_offset + sendlen
				num, error = do_skb_write(fd, pending.stream[start_offset:end_offset])
				if (error == NO_ERROR) && (num > 0) {
					pending.read_offset = start_offset + num
					pending.available_size += num

					pending.current_packet_remain_length -= num
				} else {
					return error
				}
			}
		}
	}
	return UNKNOWN_ERROR
}

func skb_write_without_filter(io *NetIOManager, fd int) (errno int) {
	if pending, ok := io.pending_output_skbs[fd]; ok {
		if BUFFER_SIZE-pending.available_size <= 0 {
			return NO_ERROR
		}

		// determine the maximum size which can be send
		start_offset := pending.read_offset
		end_offset := pending.read_offset + BUFFER_SIZE - pending.available_size
		if end_offset > BUFFER_SIZE {
			end_offset = BUFFER_SIZE
		}
		num, error := do_skb_write(fd, pending.stream[start_offset:end_offset])
		if (error == NO_ERROR) && (num > 0) {
			pending.read_offset = (start_offset + num) % BUFFER_SIZE
			pending.available_size += num
			if num < (end_offset - start_offset) {
				return NO_ERROR
			}
		} else {
			return error
		}

		if BUFFER_SIZE-pending.available_size <= 0 {
			return NO_ERROR
		}

		// wrap around the ring buffer
		start_offset = pending.read_offset
		end_offset = start_offset + BUFFER_SIZE - pending.available_size
		num, error = do_skb_write(fd, pending.stream[start_offset:end_offset])
		if (error == NO_ERROR) && (num > 0) {
			pending.read_offset = start_offset + num
			pending.available_size += num
			return NO_ERROR
		} else {
			return error
		}
	}
	return UNKNOWN_ERROR
}

/******************************************/
/*                                        */
/*       Internal Support Routines        */
/*                                        */
/******************************************/
func do_skb_read(fd int, data []byte) (num int32, errno int) {
	nread, err := syscall.Read(fd, data)
	if nread < 0 && err != nil {
		if err == syscall.EAGAIN {
			return 0, NO_ERROR
		} else if err == syscall.EWOULDBLOCK {
			return 0, NO_ERROR
		} else if err == syscall.EINTR {
			// TODO: anything to do??? retry???
			return 0, READ_ERROR
		} else {
			return 0, READ_ERROR
		}
	} else if nread == 0 {
		return 0, SESSION_EOF
	}
	return int32(nread), NO_ERROR
}

func do_skb_write(fd int, data []byte) (num int32, errno int) {
	nwrite, err := syscall.Write(fd, data)
	if nwrite < 0 && err != nil {
		if err == syscall.EAGAIN {
			return 0, NO_ERROR
		} else if err == syscall.EWOULDBLOCK {
			return 0, NO_ERROR
		} else if err == syscall.EINTR {
			//TODO: anything to do??? retry???
			return 0, WRITE_ERROR
		} else {
			return 0, WRITE_ERROR
		}
	} else if nwrite == 0 {
		return 0, NO_ERROR
	}
	return int32(nwrite), NO_ERROR
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
		server_peer = IOSocketPeer{clientfd, serverfd, serverinfo, skb_read,
			skb_write_with_filter}
	} else {
		server_peer = IOSocketPeer{clientfd, serverfd, serverinfo, skb_read,
			skb_write_without_filter}
	}
	client_peer := IOSocketPeer{clientfd, serverfd, clientinfo, skb_read,
		skb_write_without_filter}
	io.io_socket_peers[clientfd] = &client_peer
	io.io_socket_peers[serverfd] = &server_peer
}

func alloc_skb_buffer(io *NetIOManager, fd int) {
	io.pending_output_skbs[fd] = &OutputQueue{
		current_packet_op:            OP_UNKNOWN,
		current_packet_remain_length: 0,
		write_offset:                 0,
		read_offset:                  0,
		available_size:               BUFFER_SIZE,
		stream:                       make([]byte, BUFFER_SIZE),
	}
}

func sock_close(io *NetIOManager, fd int) {
	syscall.EpollCtl(io.epoll_fd, syscall.EPOLL_CTL_DEL, fd,
		&syscall.EpollEvent{Events: syscall.EPOLLIN | syscall.EPOLLOUT |
			syscall.EPOLLRDHUP, Fd: int32(fd)})
	syscall.Close(fd)
	delete(io.pending_output_skbs, fd)
	delete(io.io_socket_peers, fd)
}
