package server

import (
    "syscall"
)

type IOSocketPeer struct {
    clientfd int  // TCP connection with mongo client
    serverfd int  // TCP connection with mongo server

    // ip address
    // port
}

type OutputQueue struct {
    packet []byte
    stream []byte
}

var max_backlog int = 100
var skb = make([]byte, 256) //TODO: performance tunning, mongodb client request size???
var io_socket_peers = make(map[int] *IOSocketPeer)
var pending_output_skbs = make(map[int] *OutputQueue)

func sock_listen(sa syscall.Sockaddr) (fd int, err error) {
    serverfd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
    if err != nil {
        return -1, err
    }

    err = syscall.Bind(serverfd, sa)
    if err != nil {
        return -2, err
    }

    err = syscall.Listen(serverfd, max_backlog)
    if err != nil {
        return -3, err
    }

    err = syscall.EpollCtl(epoll_fd, syscall.EPOLL_CTL_ADD, serverfd, &syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(serverfd)})
    if err != nil {
        return -4, err
    }

    return serverfd, nil
}

func sock_accept(fd int) (clientfd int, err error) {
    nfd, _, err := syscall.Accept(fd)
    if err != nil {
        return -1, err
    }

    err = syscall.SetNonblock(nfd, true)
    if err != nil {
        return -2, err
    }

    err = syscall.EpollCtl(epoll_fd, syscall.EPOLL_CTL_ADD, nfd, &syscall.EpollEvent{Events: syscall.EPOLLIN | syscall.EPOLLOUT | syscall.EPOLLRDHUP, Fd: int32(nfd)})
    if err != nil {
        return -3, err
    }

    return nfd, err
}

func sock_connect(sa syscall.Sockaddr) (client_fd int, err error) {
    nfd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
    if err != nil {
        return -1, err
    }

    err = syscall.Connect(nfd, sa)
    if err != nil {
        return -2, err
    }

    err = syscall.SetNonblock(nfd, true)
    if err != nil {
        return -3, err
    }

    err = syscall.EpollCtl(epoll_fd, syscall.EPOLL_CTL_ADD, nfd, &syscall.EpollEvent{Events: syscall.EPOLLIN | syscall.EPOLLOUT | syscall.EPOLLRDHUP, Fd: int32(nfd)})
    if err != nil {
        return -4, err
    }

    return nfd, nil
}

func sock_read(fd int) (nread int, err error) {
    num, err := syscall.Read(fd, skb)
    if num < 0 && err != nil {
        if err == syscall.EAGAIN {
            return 0, nil
        } else if err == syscall.EWOULDBLOCK {
            return 0, nil
        } else if err == syscall.EINTR {
            logger.Error(err)
            //TODO: signal received, try again?
        } else {
            return num, err
        }
    }
    return num, nil
}

func sock_write_with_filter(fd int) (nwrite int, err error) {
    nwrite = 0
    if pending, ok := pending_output_skbs[fd]; ok {
        if len(pending.packet) > 0 {
            nwrite, err = syscall.Write(fd, pending.packet)
            if nwrite < 0 && err != nil {
                if err == syscall.EAGAIN {
                    return 0, nil
                } else if err == syscall.EWOULDBLOCK {
                    return 0, nil
                } else if err == syscall.EINTR {
                    logger.Error(err)
                    //TODO: signal received, try again?
                } else {
                    return 0, err
                }
            } else if nwrite == 0 {
                return 0, nil
            } else {
                pending_output_skbs[fd].packet = pending.packet[nwrite: len(pending.packet)]
                if len(pending_output_skbs[fd].packet) > 0 {
                    return nwrite, nil
                }
            }
        }

        for {
            message_length, op_code := handle_msg_header(pending_output_skbs[fd].stream)
            if message_length > 0 {
                if !filter(op_code) {
                    // block operation
                    return -1, nil
                }

                num, err := syscall.Write(fd, pending_output_skbs[fd].stream[0:message_length])
                if num < 0 && err != nil {
                    if err == syscall.EAGAIN {
                        return nwrite, nil
                    } else if err == syscall.EWOULDBLOCK {
                        return nwrite, nil
                    } else if err == syscall.EINTR {
                        logger.Error(err)
                        //TODO: signal received, try again?
                    } else {
                        return nwrite, err
                    }
                } else if num == 0 {
                    return nwrite, nil
                } else {
                    nwrite += num
                    remove_done_skb(fd, int(message_length))
                    if num < int(message_length) {
                        add_partial_skb(fd, pending_output_skbs[fd].stream[num:message_length])
                        return nwrite, nil
                    }
                }
            } else {
                break
            }
        }
    }
    return nwrite, nil
}

func sock_write_without_filter(fd int) (nwrite int, err error) {
    if pending, ok := pending_output_skbs[fd]; ok {
        num, err := syscall.Write(fd, pending.stream)
        if num < 0 && err != nil {
            if err == syscall.EAGAIN {
                return num, nil
            } else if err == syscall.EWOULDBLOCK {
                return num, nil
            } else if err == syscall.EINTR {
                logger.Error(err)
                //TODO: signal received, try again?
            } else {
                return num, err
            }
        } else if num == 0 {
            return 0, nil
        } else {
            remove_done_skb(fd, num)
            return num, nil
        }
    }
    return 0, nil
}


func save_pending_skb(fd int, data []byte) {
    if pending, ok := pending_output_skbs[fd]; ok {
        pending_output_skbs[fd].stream = append(pending.stream, data)
    } else {
        pending_output_skbs[fd] = &OutputQueue{make([]byte, 0), data}
    }
}

func sock_close_peers(fd int) {
    if _, ok := io_socket_peers[fd]; ok {
        var peerfd int
        if fd == io_socket_peers[fd].clientfd {
            peerfd = io_socket_peers[fd].serverfd
        } else {
            peerfd = io_socket_peers[fd].clientfd
        }
        sock_close(fd)
        sock_close(peerfd)
    }
}

/*
 * Support Routines
 */
func append(skb1, skb2 []byte) (skb []byte) {
    newskb := make([]byte, len(skb1) + len(skb2))
    copy(newskb, skb1)
    copy(newskb[len(skb1):], skb2)
    return newskb
}

func remove_done_skb(fd int, num int) {
    if pending, ok := pending_output_skbs[fd]; ok {
        pending_output_skbs[fd].stream = pending.stream[num:]
    }
}

func add_partial_skb(fd int, data []byte) {
    if pending, ok := pending_output_skbs[fd]; ok {
        pending_output_skbs[fd].packet = data
    } else {
        pending_output_skbs[fd] = &OutputQueue{data, pending.stream}
    }
}

func add_sock_peer(clientfd, serverfd int) {
    sock_peer := IOSocketPeer{clientfd, serverfd}
    io_socket_peers[clientfd], io_socket_peers[serverfd] = &sock_peer, &sock_peer
}

func sock_close(fd int) {
    syscall.EpollCtl(epoll_fd, syscall.EPOLL_CTL_DEL, fd, &syscall.EpollEvent{Events: syscall.EPOLLIN | syscall.EPOLLOUT | syscall.EPOLLRDHUP, Fd: int32(fd)})
    syscall.Close(fd)
    delete(pending_output_skbs, fd)
    delete(io_socket_peers, fd)
}
