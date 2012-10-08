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
var window uint               // Time window to check the limit(in seconds)
var limit uint64              // Limit size(in bytes) per time window include bosh inbound and outbound
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
	flag.Uint64Var(&limit, "limit", 0, "limit size per time window(in bytes)")
	flag.UintVar(&window, "window", 0, "time window to check the limit(in seconds)")
	flag.StringVar(&logFile, "l", "", "log file name")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s -eport external_port -iport internal_port [-iip internal_ip] -limit limit_size -window time_window -l log_file\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(-1)
	}
	flag.Parse()
	if ePort == 0 || iPort == 0 || logFile == "" || limit == 0 || window == 0 {
		flag.Usage()
	}
	ip := net.ParseIP(iIp)
	if ip == nil {
		fmt.Fprintln(os.Stderr, "Invalid ip:", iIp)
		flag.Usage()
	}

	err := logger.Init(logFile)
	if err != nil {
		fmt.Println("Init log file error:", err)
		os.Exit(-1)
	}
	defer logger.Finalize()

	// Handle signal
	go signalHand()

	t := tunnel.Tunnel{
		EPort:     ePort,
		IIp:       ip,
		IPort:     iPort,
		Limit:     limit,
		Window:    window,
		CheckTime: time.Now(),
	}
	tunnel.Run(&t)
}
