package main

import (
	"daylimit/iptablesrun"
	"flag"
	"fmt"
	"os"
	"syscall"
	"time"
)

const (
	MAXERR = 3
)

type serviceCheckPoint struct {
	Id        string
	LastCheck time.Time
	Size      int64
	Status    int8
	LastSize  int64
}

var items map[string]*serviceCheckPoint

type CmdOptions struct {
	LimitWindow  int64
	LimitSize    int64
	LogFile      string
	FetchInteval int64
}

var opts CmdOptions

func SizeCheck(id string, netInfo *iptablesrun.RuleInfo) {
	ckInfo, ok := items[id]
	if !ok {
		items[id] = &serviceCheckPoint{
			Id:        id,
			LastCheck: time.Now(),
			LastSize:  netInfo.Size,
			Size:      0,
			Status:    0}
		ckInfo = items[id]
	}
	// Size is set only for ACCEPT rules
	if netInfo.Status == iptablesrun.ACCEPT {
		ckInfo.Size = netInfo.Size
	}
	if time.Since(ckInfo.LastCheck) > time.Duration(opts.LimitWindow)*time.Second {
		tw := time.Duration(opts.LimitWindow)
		ckInfo.LastSize = ckInfo.Size
		ckInfo.LastCheck = ckInfo.LastCheck.Add(time.Since(ckInfo.LastCheck) / time.Second / tw * tw * time.Second)
		if netInfo.Status == iptablesrun.DROP {
			// Unblock connection
			iptablesrun.Unblock(ckInfo.Id)
			iptablesrun.Logger().Infof("Unblock container [%s]", ckInfo.Id)
			ckInfo.Size = 0
			ckInfo.LastSize = 0
		}
	}
	if ckInfo.Size-ckInfo.LastSize > opts.LimitSize && netInfo.Status == iptablesrun.ACCEPT {
		// Block connection
		iptablesrun.Block(ckInfo.Id)
		iptablesrun.Logger().Infof("Block container [%s]", ckInfo.Id)
		ckInfo.Size = 0
		ckInfo.LastSize = 0
	}
}

func runDaemon() {
	var errNum int8
	for {
		// Get iptables all rules
		info, err := iptablesrun.FetchAll()
		if err != nil {
			iptablesrun.Logger().Errorf("Fetch iptables info error:%s", err)
			time.Sleep(time.Duration(opts.FetchInteval) * time.Second)
			errNum++
			if errNum >= MAXERR {
				os.Exit(2)
			}
			continue
		}
		errNum = 0
		// Check limit match
		for id, netInfo := range info {
			SizeCheck(id, netInfo)
		}
		// Update rules
		iptablesrun.Update()
		time.Sleep(time.Duration(opts.FetchInteval) * time.Second)
	}
}

func main() {
	// Parse options
	flag.StringVar(&opts.LogFile, "l", "", "Log file path")
	flag.Int64Var(&opts.LimitWindow, "lw", 86400, "Limit time window default")
	flag.Int64Var(&opts.LimitSize, "ls", 1*1024*1024, "Limit size")
	flag.Int64Var(&opts.FetchInteval, "fi", 5*60, "Interval for get iptables info")
	flag.Parse()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-l log_file] [-lw limit_window] [-ls limit_size] [-fi fetch_interval]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}

	items = make(map[string]*serviceCheckPoint)

	iptablesrun.InitLog(opts.LogFile)
	if err := syscall.Setuid(0); err != nil {
		panic(err)
	}
	runDaemon()
}
