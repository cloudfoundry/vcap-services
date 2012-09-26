package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"proxy_tunnel/logger"
	"proxy_tunnel/tunnel"
	"time"
)

var logFile string            // Log file name
var ePort uint                // External port proxy listen to
var iIp string                // Inner ip proxy connect to
var iPort uint                // Inner port proxy connect to
var blockSize uint64          // Day block size(in bytes) include bosh inbound and outbound
var signalChan chan os.Signal // Cahnel for signal

func signalHand() {
	signalChan = make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	select {
	case <-signalChan:
		tunnel.Stop()
		os.Exit(-1)
	}
	return
}

func main() {
	flag.UintVar(&ePort, "eport", 0, "port proxy listen")
	flag.StringVar(&iIp, "iip", "127.0.0.1", "inner ip proxy connect to")
	flag.UintVar(&iPort, "iport", 0, "inner port proxy connect to")
	flag.Uint64Var(&blockSize, "bs", 0, "day block size(in bytes)")
	flag.StringVar(&logFile, "l", "", "log file name")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s -eport external_port -iport internal_port [-iip internal_ip] -l log_file\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}
	flag.Parse()
	if ePort == 0 || iPort == 0 || logFile == "" {
		flag.Usage()
	}
	ip := net.ParseIP(iIp)
	if ip == nil {
		fmt.Fprintln(os.Stderr, "Invalid ip:", iIp)
		flag.Usage()
	}

	err := logger.Init(logFile)
	if err != nil {
		os.Exit(2)
	}
	defer logger.Finalize()

	// Handle signal

	go signalHand()

	t := tunnel.Tunnel{
		EPort:     ePort,
		IIp:       ip,
		IPort:     iPort,
		BlockSize: blockSize,
		CheckTime: time.Now(),
	}
	tunnel.Run(&t)
}
