package main

import "go-mongo-proxy/proxy"
import l4g "github.com/moovweb/log4go"
import "path/filepath"
import "syscall"

var log l4g.Logger

func log_init(conf proxy.ProxyConfig) {
	log_level := l4g.INFO
	switch conf.LOGGING.LEVEL {
	case "debug":
		log_level = l4g.DEBUG
	case "info":
		log_level = l4g.INFO
	case "warning":
		log_level = l4g.WARNING
	case "error":
		log_level = l4g.ERROR
	case "critical":
		log_level = l4g.CRITICAL
	}
	log_path := conf.LOGGING.PATH
	syscall.Mkdir(filepath.Dir(log_path), 0755)
	log = make(l4g.Logger)
	log.AddFilter("file", log_level, l4g.NewFileLogWriter(log_path, true))
}

func log_fini() {
	log.Close()
}
