package server

import (
    "syscall"
    "os"
    "os/signal"
    "net"
    "strconv"
    "fmt"
)

import l4g "log4go"

type ProxyConfig struct {
    HOST string
    PORT string

    MONGODB struct {
        HOST string
        PORT string
        DBNAME string
        USER string
        PASS string
    }

    FILTER struct {
        FS_RESERVED_BLOCKS float64
        INTERVAL uint32
        THRESHOLD float64
    }

    LOGGING struct {
        LEVEL string
        PATH  string
    }
}

var max_listen_fds = 1024
var timeout = 1000  // 1000 milliseconds
var quit = false // quit the whole process or not

var proxy_server syscall.SockaddrInet4
var mongo_server syscall.SockaddrInet4

var epoll_fd int
var events []syscall.EpollEvent = make([]syscall.EpollEvent, 100)

var logger l4g.Logger

func exit() {
    logger.Close()
    exit()
}

func Start(conf ProxyConfig, proxy_log l4g.Logger) (err error) {
    parse_config(conf)

    logger = proxy_log

    c := setup_signal()

    setup_filter(conf)

    logger.Info("Mongodb proxy server start.")

    epoll_fd, err = syscall.EpollCreate(max_listen_fds)
    if err != nil {
        logger.Critical("Failed to initialize epoll listener [%s].", err)
        fmt.Println("Failed to initialize epoll listener [%s].", err)
        exit()
    }

    proxy_server_fd, err := sock_listen(&proxy_server)
    if err != nil {
        logger.Critical("Failed to initialize server listener [%s].", err)
        fmt.Println("Failed to initialize server listener [%s].", err)
        exit()
    }

    for {
        wait_signal(c, syscall.SIGTERM)

        if quit {
            break
        }

        nfds, err := syscall.EpollWait(epoll_fd, events, timeout)
        if err != nil {
            logger.Critical("Failed to do epoll wait [%s].", err)
            fmt.Println("Failed to do epoll wait [%s].", err)
            exit()
        } else if nfds == 0 {
            // TODO: need to do something?
        } else {
            for i := 0; i < nfds; i ++  {
                fd := int(events[i].Fd)

                if fd == proxy_server_fd {
                    clientfd, err := sock_accept(proxy_server_fd)

                    if err != nil {
                        logger.Critical("Failed to accept new mongo client request [%s].", err)
                        // TODO: addtional handler?
                    } else {
                        serverfd, err := sock_connect(&mongo_server)
                        if err != nil {
                            logger.Critical("Failed to establish connection with mongo server [%s].", err) // mongodb server not reachable
                            sock_close(clientfd) // only disconect with mongodb client
                        } else {
                            add_sock_peer(clientfd, serverfd)
                        }
                    }
                } else {
                    event := events[i].Events

                    if event & syscall.EPOLLIN != 0 {
                        for {
                            nread, err := sock_read(fd)

                            if nread < 0 {
                                if err != nil {
                                    logger.Error("Failed to read data from ... [%s].", err)
                                    sock_close_peers(fd)
                                } else {
                                    logger.Error("End of communicaiotn from ...")
                                    sock_close_peers(fd)
                                }
                                break
                            } else if nread == 0 {
                                break
                            } else {
                                if fd == io_socket_peers[fd].clientfd {
                                    peerfd := io_socket_peers[fd].serverfd
                                    save_pending_skb(peerfd, skb[0:nread])
                                } else {
                                    peerfd := io_socket_peers[fd].clientfd
                                    save_pending_skb(peerfd, skb[0:nread])
                                }

                                if nread < len(skb) {
                                    break
                                }
                            }
                        }
                    }

                    if event & syscall.EPOLLOUT != 0 {
                        /*
                         * We only filter requests from mongo client to mongo server.
                         */
                        if _, ok := io_socket_peers[fd]; ok {
                            var nwrite int = 0
                            if fd == io_socket_peers[fd].serverfd {
                                nwrite, err = sock_write_with_filter(fd)
                            } else {
                                nwrite, err = sock_write_without_filter(fd)
                            }

                            if nwrite < 0 {
                                if err != nil {
                                    logger.Error("Failed to write data to ... [%s].", err)
                                    sock_close_peers(fd)
                                } else {
                                    logger.Error("Request from ... failed due to filter block.") // filter block
                                    sock_close_peers(fd)
                                }
                            }
                        }
                    }

                    if event & syscall.EPOLLRDHUP != 0 {
                        logger.Info("shutdown ...")
                        sock_close_peers(fd)
                    }

                    if event & syscall.EPOLLHUP != 0 {
                        logger.Info("shutdown ...")
                        sock_close_peers(fd)
                    }
                }
            }
        }
    }

    destroy_filter()

    logger.Info("Mongodb proxy server quit.")
    return 
}

/*
 * Support Routines
 */
func parse_config(conf ProxyConfig) {
    proxy_ipaddr := net.ParseIP(conf.HOST)
    if proxy_ipaddr == nil {
        panic("Proxy ipaddr format error")
    }

    proxy_port, err := strconv.Atoi(conf.PORT)
    if err != nil {
        panic(err)
    }

    // TODO: need a protable way not hard code to parse ip address
    proxy_server = syscall.SockaddrInet4{Port: proxy_port, Addr: [4]byte{proxy_ipaddr[12], proxy_ipaddr[13], proxy_ipaddr[14], proxy_ipaddr[15]}}

    mongo_ipaddr := net.ParseIP(conf.MONGODB.HOST)
    if mongo_ipaddr == nil {
        panic("Mongo ipaddr format error")
    }

    mongo_port, err := strconv.Atoi(conf.MONGODB.PORT)
    if err != nil {
        panic(err)
    }

    // TODO: need a protable way not hard code to parse ip address
    mongo_server = syscall.SockaddrInet4{Port: mongo_port, Addr: [4]byte{mongo_ipaddr[12], mongo_ipaddr[13], mongo_ipaddr[14], mongo_ipaddr[15]}}
}

func setup_signal() (c chan os.Signal) {
    c = make(chan os.Signal, 1)
    signal.Notify(c, syscall.SIGTERM)
    return c
}

func wait_signal(c <-chan os.Signal, sig os.Signal) {
    select {
    case s := <-c:
        if s == sig {
            quit = true
        }
    default:
        return
    }
}
