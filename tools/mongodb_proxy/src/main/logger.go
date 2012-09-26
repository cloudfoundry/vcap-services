package main

import "server"
import l4g "log4go"
import "path/filepath"
import "syscall"

var log l4g.Logger
var log_level = l4g.INFO
var log_path string

func log_init(conf server.ProxyConfig) {
    parse_config(conf)
    syscall.Mkdir(filepath.Dir(log_path), 0755) 
    log = make(l4g.Logger)
    log.AddFilter("file", log_level, l4g.NewFileLogWriter(log_path, true))
}

func log_fini() {
    log.Close()
}

/*
 * Support Routines
 */
func parse_config(conf server.ProxyConfig) {
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
    log_path  = conf.LOGGING.PATH
}
