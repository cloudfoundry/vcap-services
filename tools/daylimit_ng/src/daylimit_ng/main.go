package main

import (
	"daylimit_ng/config"
	"daylimit_ng/devinfo"
	"daylimit_ng/logger"
	"daylimit_ng/warden"
	"flag"
	"fmt"
	"os"
	"time"
)

const (
	MAXERR  = 3
	UNBLOCK = 0
	BLOCK   = 1
)

type serviceCheckPoint struct {
	Id        string
	LastCheck time.Time
	Size      int64
	Status    int8
	LastSize  int64
}

var items map[string]*serviceCheckPoint = make(map[string]*serviceCheckPoint)

var configFile string
var w *warden.Warden

func SizeCheck(id string, size int64) {
	ckInfo, ok := items[id]
	if !ok {
		items[id] = &serviceCheckPoint{
			Id:        id,
			LastCheck: time.Now(),
			LastSize:  size,
			Size:      size,
			Status:    UNBLOCK}
		ckInfo = items[id]
	}
	ckInfo.Size = size
	if time.Since(ckInfo.LastCheck) > time.Duration(config.Get().LimitWindow)*time.Second {
		tw := time.Duration(config.Get().LimitWindow)
		ckInfo.LastSize = ckInfo.Size
		ckInfo.LastCheck = ckInfo.LastCheck.Add(time.Since(ckInfo.LastCheck) / time.Second / tw * tw * time.Second)
		if ckInfo.Status == BLOCK {
			// Unblock connection
			if ok := w.Unblock(ckInfo.Id); ok {
				logger.Logger().Infof("Unblock container [%s]", ckInfo.Id)
			} else {
				logger.Logger().Errorf("Unblock container failed [%s]", ckInfo.Id)
			}
			ckInfo.Status = UNBLOCK
		}
	} else if ckInfo.Size-ckInfo.LastSize > config.Get().LimitSize && ckInfo.Status == UNBLOCK {
		// Block connection
		ckInfo.Status = BLOCK
		if ok := w.Block(ckInfo.Id); ok {
			logger.Logger().Infof("Block container [%s]", ckInfo.Id)
		} else {
			logger.Logger().Errorf("Block container failed [%s]", ckInfo.Id)
		}
	}
}

func runDaemon() {
	var errNum int8
	ticker := time.Tick(time.Duration(config.Get().FetchInteval) * time.Second)
	for _ = range ticker {
		info, err := devinfo.GetList()
		if err != nil {
			logger.Logger().Errorf("Get throughput size error:[%s]", err)
			errNum++
			if errNum >= MAXERR {
				os.Exit(2)
			}
			continue
		}
		errNum = 0
		// Check limit match
		for id, size := range info {
			SizeCheck(id, size)
		}
	}
}

func main() {
	flag.StringVar(&configFile, "c", "", "Config file name")
	flag.Parse()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -c config_file", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}

	if configFile == "" {
		flag.Usage()
	}

	if err := config.Load(configFile); err != nil {
		fmt.Fprintf(os.Stderr, "Load config file error: %s", err)
		os.Exit(2)
	}

	if f, err := os.Open(config.Get().WardenBin); err != nil {
		fmt.Fprintf(os.Stderr, "Open warden bin file error: %s", err)
		flag.Usage()
	} else {
		f.Close()
	}

	w = &warden.Warden{
		Bin:          config.Get().WardenBin,
		BlockRate:    config.Get().BlockRate,
		BlockBurst:   config.Get().BlockRate,
		UnblockRate:  config.Get().UnblockRate,
		UnblockBurst: config.Get().UnblockRate,
	}

	logger.InitLog(config.Get().LogFile)
	runDaemon()
}
