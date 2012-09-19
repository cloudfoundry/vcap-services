package main

import (
	"flag"
	"fmt"
	"logger"
	"os"
	"server"
)

var configFile string
var logFile string

func main() {
	flag.StringVar(&configFile, "cf", "", "file contains port map rules")
	flag.StringVar(&logFile, "l", "", "log file name")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s -cf config_file -l log_file\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}
	flag.Parse()
	if flag.NArg() == 0 && (configFile == "" || logFile == "") {
		flag.Usage()
	}
	items, err := LoadConfig(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Parse Config File[%s] Error: %s\n", configFile, err)
		os.Exit(2)
	}
	err = logger.Init(logFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Init Log Config[%s] Error: %s\n", logFile, err)
		os.Exit(2)
	}
	defer logger.Finalize()
	server.Run(items)
}
