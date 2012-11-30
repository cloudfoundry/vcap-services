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
	var buffer Buffer
	var current_pkt_op, current_pkt_remain_len int

	buffer = NewBuffer(BUFFER_SIZE)
	current_pkt_op = OP_UNKNOWN
	current_pkt_remain_len = 0

	clientfd := session.clientconn
	serverfd := session.serverconn
	filter := session.filter

	buffer.ResetCursor()
	for {
		select {
		case <-session.clientshutdown:
			break
		default:
		}

		/*
		 * Refer to Golang src/pkg/net/fd.go#L416
		 *
		 * Here fd is NONBLOCK, but Golang has handled EAGAIN/EOF within Read function.
		 */
		nread, err := clientfd.Read(buffer.Cursor())
		if err != nil {
			if err == io.EOF {
				logger.Debug("TCP session with mongodb client will be closed soon.")
				break
			}
			logger.Debug("TCP read from client error: [%v].", err)
			continue
		} else {
			buffer.ForwardCursor(nread)
		}

		if current_pkt_remain_len == 0 {
			pkt_len, op_code := parseMsgHeader(buffer.Data())
			current_pkt_op = int(op_code)
			current_pkt_remain_len = int(pkt_len)
		}

		if current_pkt_remain_len == 0 {
			// Process further only when we have seen complete mongodb packet header,
			// whose length is 16 bytes.
			continue
		}

		if filter.FilterEnabled() && filter.IsDirtyEvent(current_pkt_op) {
			filter.EnqueueDirtyEvent()
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
		nwrite, err := serverfd.Write(buffer.Data())
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				logger.Debug("TCP session with mongodb server encounter unexpected EOF: [%v].", err)
				break
			}
			logger.Debug("TCP write to server error: [%v].", err)
			continue
		}
		buffer.ResetCursor()

		/*
		 * One corner case
		 *
		 * If a malformed application establishes a 'RAW' tcp connection to our proxy, then
		 * this application may fill up the packet header length to be M, while the real packet
		 * length is N and N > M, we must prevent this case.
		 */
		current_pkt_remain_len -= nwrite
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
			continue
		}

		_, err = clientfd.Write(buffer[0:nread])
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				logger.Debug("TCP session with mongodb client encounter unexpected EOF: [%v].", err)
				break
			}
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
