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
	Reset(*net.TCPConn, *net.TCPConn, Filter)
	GetSid() int32
	Process()
	WaitForFinish()
}

type SessionManager interface {
	NewSession(*net.TCPConn, *net.TCPConn, Filter) Session
	WaitAllFinish()
	MarkIdle(Session)
}

type ProxySessionImpl struct {
	manager SessionManager

	sid            int32
	clientconn     *net.TCPConn
	serverconn     *net.TCPConn
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

func (session *ProxySessionImpl) Reset(clientfd *net.TCPConn, serverfd *net.TCPConn, f Filter) {
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

func (session *ProxySessionImpl) ForwardClientMsg() {
	var buffer []byte
	var current_pkt_op, current_pkt_remain_len, nstart int

	buffer = make([]byte, BUFFER_SIZE)
	current_pkt_op = OP_UNKNOWN
	current_pkt_remain_len = 0
	nstart = 0

	session.lock.Lock()
	session.running++
	session.lock.Unlock()

	clientfd := session.clientconn
	serverfd := session.serverconn
	filter := session.filter

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
		nread, err := clientfd.Read(buffer[nstart:BUFFER_SIZE])
		if err != nil {
			if err == io.EOF {
				logger.Debug("TCP session with mongodb client will be closed soon.")
				break
			}
			logger.Error("TCP read from client error: [%v].", err)
			continue
		}

		if current_pkt_remain_len == 0 {
			pkt_len, op_code := parseMsgHeader(buffer[0:(nstart + nread)])
			current_pkt_op = int(op_code)
			current_pkt_remain_len = int(pkt_len)
		}

		if current_pkt_remain_len == 0 {
			// Process further only when we have seen complete mongodb packet header,
			// whose length is 16 bytes.
			nstart += nread
			continue
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
		nwrite, err := serverfd.Write(buffer[0:(nstart + nread)])
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				logger.Debug("TCP session with mongodb server encounter unexpected EOF: [%v].", err)
				break
			}
			logger.Error("TCP write to server error: [%v].", err)
			continue
		}

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
		nstart = 0
	}

	// TCP connection half disconnection
	clientfd.CloseRead()
	serverfd.CloseWrite()

	session.lock.Lock()
	session.running--
	if session.running == 0 {
		session.wait <- 's'
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
			logger.Error("TCP read from server error: [%v].", err)
			continue
		}

		_, err = clientfd.Write(buffer[0:nread])
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				logger.Debug("TCP session with mongodb client encounter unexpected EOF: [%v].", err)
				break
			}
			logger.Error("TCP write to client error: [%v].", err)
			continue
		}
	}

    // TCP connection half disconnection
	serverfd.CloseRead()
    clientfd.CloseWrite()

	session.lock.Lock()
	session.running--
	if session.running == 0 {
		session.wait <- 's'
		session.manager.MarkIdle(session)
	}
	session.lock.Unlock()

	logger.Debug("ForwardServerMsg go routine exits.")
}

func (session *ProxySessionImpl) WaitForFinish() {
	session.clientshutdown <- 's'
	session.servershutdown <- 's'
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

func (manager *ProxySessionManagerImpl) NewSession(clientfd *net.TCPConn, serverfd *net.TCPConn, f Filter) Session {
	var session Session
	var sid int32

	sid = -1
	manager.lock.Lock()
	for sid, session = range manager.idles {
		break
	}
	if sid >= 0 {
		delete(manager.idles, sid)
	}
	manager.lock.Unlock()

	if sid >= 0 {
		session.Reset(clientfd, serverfd, f)
	} else {
		session = manager.SpawnSession(clientfd, serverfd, f)
	}

	manager.lock.Lock()
	manager.actives[sid] = session
	manager.lock.Unlock()

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

func (manager *ProxySessionManagerImpl) MarkIdle(session Session) {
	manager.lock.Lock()
	if sid := session.GetSid(); sid >= 0 {
		delete(manager.actives, sid)
		manager.idles[sid] = session
	}
	manager.lock.Unlock()
}

func (manager *ProxySessionManagerImpl) SpawnSession(clientfd *net.TCPConn, serverfd *net.TCPConn, f Filter) Session {
	var sid int32

	manager.lock.Lock()
	sid = manager.sid
	manager.sid++
	manager.lock.Unlock()

	return &ProxySessionImpl{
		manager:        manager,
		sid:            sid,
		clientconn:     clientfd,
		serverconn:     serverfd,
		filter:         f,
		clientshutdown: make(chan byte, 1),
		servershutdown: make(chan byte, 1),
		running:        0,
		wait:           make(chan byte, 1)}
}

func NewSessionManager() *ProxySessionManagerImpl {
	return &ProxySessionManagerImpl{
		actives: make(map[int32]Session),
		idles:   make(map[int32]Session),
		sid:     0}
}
