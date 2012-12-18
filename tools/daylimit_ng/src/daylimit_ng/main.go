package main

import (
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

type CmdOptions struct {
	LimitWindow  int64
	LimitSize    int64
	LogFile      string
	FetchInteval int64
	BlockRate    int64
	UnblockRate  int64
	WardenBin    string
}

var opts CmdOptions
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
	if time.Since(ckInfo.LastCheck) > time.Duration(opts.LimitWindow)*time.Second {
		tw := time.Duration(opts.LimitWindow)
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
	} else if ckInfo.Size-ckInfo.LastSize > opts.LimitSize && ckInfo.Status == UNBLOCK {
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
	for {
		info, err := devinfo.GetList()
		if err != nil {
			logger.Logger().Errorf("Get throughput size error:[%s]", err)
			time.Sleep(time.Duration(opts.FetchInteval) * time.Second)
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
		time.Sleep(time.Duration(opts.FetchInteval) * time.Second)
	}
}

func main() {
	// Parse options
	flag.StringVar(&opts.LogFile, "l", "", "Log file path")
	flag.StringVar(&opts.WardenBin, "wb", "", "Warden client bin path")
	flag.Int64Var(&opts.LimitWindow, "lw", 86400, "Limit time window default")
	flag.Int64Var(&opts.LimitSize, "ls", 1*1024*1024, "Limit size")
	flag.Int64Var(&opts.FetchInteval, "fi", 5*60, "Interval for get iptables info")
	flag.Int64Var(&opts.BlockRate, "br", 0, "Throughput rate when block")
	flag.Int64Var(&opts.UnblockRate, "ubr", 0, "Normal Throughput rate when unblock")
	flag.Parse()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -br block_throughput_rate -ubr unblock_throughput_rate -wb warden_bin [-l log_file] [-lw limit_window] [-ls limit_size] [-fi fetch_interval]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}

	if opts.BlockRate == 0 || opts.UnblockRate == 0 || opts.WardenBin == "" {
		flag.Usage()
	}

	if f, err := os.Open(opts.WardenBin); err != nil {
		fmt.Fprintf(os.Stderr, "Open warden bin file error: %s", err)
		flag.Usage()
	} else {
		f.Close()
	}

	w = &warden.Warden{
		Bin:          opts.WardenBin,
		BlockRate:    opts.BlockRate,
		BlockBurst:   opts.BlockRate,
		UnblockRate:  opts.UnblockRate,
		UnblockBurst: opts.UnblockRate,
	}

	items = make(map[string]*serviceCheckPoint)

	logger.InitLog(opts.LogFile)
	runDaemon()
}
