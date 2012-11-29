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
				logger.Debug("TCP session with mongodb client will be closed soon.")
				break
			}
			logger.Debug("TCP read from client error: [%v].", err)
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

		// filter process
		if filter.FilterEnabled() && !filter.PassFilter(current_pkt_op) {
			logger.Debug("TCP session with mongodb client is blocked by filter.")
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
				logger.Debug("TCP session with mongodb server encounter unexpected EOF: [%v].", err)
				break
			}
			logger.Debug("TCP write to server error: [%v].", err)
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
			break
		}

		_, err = clientfd.Write(buffer[0:nread])
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				logger.Debug("TCP session with mongodb client encounter unexpected EOF: [%v].", err)
				break
			}
			logger.Debug("TCP write to client error: [%v].", err)
			break
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
		clientshutdown: make(chan bool),
		servershutdown: make(chan bool)}
}
