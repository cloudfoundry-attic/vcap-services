package main

import (
	"daylimit/daylimit"
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
var warden *daylimit.Warden

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
	config := daylimit.Config()
	if time.Since(ckInfo.LastCheck) > time.Duration(config.LimitWindow)*time.Second {
		tw := time.Duration(config.LimitWindow)
		ckInfo.LastSize = ckInfo.Size
		ckInfo.LastCheck = ckInfo.LastCheck.Add(time.Since(ckInfo.LastCheck) / time.Second / tw * tw * time.Second)
		if ckInfo.Status == BLOCK {
			// Unblock connection
			if ok := warden.Unblock(ckInfo.Id); ok {
				daylimit.Logger().Infof("Unblock container [%s]", ckInfo.Id)
			} else {
				daylimit.Logger().Errorf("Unblock container failed [%s]", ckInfo.Id)
			}
			ckInfo.Status = UNBLOCK
		}
	} else if ckInfo.Size-ckInfo.LastSize > config.LimitSize && ckInfo.Status == UNBLOCK {
		// Block connection
		ckInfo.Status = BLOCK
		if ok := warden.Block(ckInfo.Id); ok {
			daylimit.Logger().Infof("Block container [%s]", ckInfo.Id)
		} else {
			daylimit.Logger().Errorf("Block container failed [%s]", ckInfo.Id)
		}
	}
}

func runDaemon() {
	var errNum int8
	ticker := time.Tick(time.Duration(daylimit.Config().FetchInteval) * time.Second)
	for _ = range ticker {
		info, err := daylimit.GetList()
		if err != nil {
			daylimit.Logger().Errorf("Get throughput size error:[%s]", err)
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

	if err := daylimit.LoadConfig(configFile); err != nil {
		fmt.Fprintf(os.Stderr, "Load config file error: %s", err)
		os.Exit(2)
	}
	config := daylimit.Config()

	if f, err := os.Open(config.WardenBin); err != nil {
		fmt.Fprintf(os.Stderr, "Open warden bin file error: %s", err)
		flag.Usage()
	} else {
		f.Close()
	}

	warden = &daylimit.Warden{
		Bin:          config.WardenBin,
		BlockRate:    config.BlockRate,
		BlockBurst:   config.BlockRate,
		UnblockRate:  config.UnblockRate,
		UnblockBurst: config.UnblockRate,
	}

	daylimit.InitLog(config.LogFile)
	runDaemon()
}
