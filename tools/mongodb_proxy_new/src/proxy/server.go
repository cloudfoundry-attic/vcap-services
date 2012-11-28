package main

import (
	"flag"
	"fmt"
	"net"
)

type ConnectionInfo struct {
	HOST   string
	PORT   string
	DBNAME string
	USER   string
	PASS   string
}

type ProxyConfig struct {
	HOST string
	PORT string

	MONGODB ConnectionInfo
}

func startProxyServer(conf *ProxyConfig) error {
	proxyaddrstr := flag.String("proxy listen address", conf.HOST+":"+conf.PORT, "host:port")
	mongoaddrstr := flag.String("mongo listen address", conf.MONGODB.HOST+":"+conf.MONGODB.PORT, "host:port")

	proxyaddr, err := net.ResolveTCPAddr("tcp", *proxyaddrstr)
	if err != nil {
		fmt.Printf("TCP addr resolve error: [%v].\n", err)
		return err
	}

	mongoaddr, err := net.ResolveTCPAddr("tcp", *mongoaddrstr)
	if err != nil {
		fmt.Printf("TCP addr resolve error: [%v].\n", err)
		return err
	}

	proxyfd, err := net.ListenTCP("tcp", proxyaddr)
	if err != nil {
		fmt.Printf("TCP server listen error: [%v].\n", err)
		return err
	}

	fmt.Printf("Start proxy server.\n")

	for {
		clientconn, err := proxyfd.AcceptTCP()
		if err != nil {
			fmt.Printf("TCP server accept error: [%v].\n", err)
			continue
		}

		serverconn, err := net.DialTCP("tcp", nil, mongoaddr)
		if err != nil {
			fmt.Printf("TCP connect error: [%v].\n", err)
			clientconn.Close()
			continue
		}

		session := NewSession(clientconn, serverconn)
		go session.Process()
	}

	fmt.Printf("Stop proxy server.\n")
	return nil
}

// NOTE: Following hard code configuration will be removed finally.
func main() {
	conf := &ProxyConfig{}
	conf.HOST = "127.0.0.1"
	conf.PORT = "29017"
	conf.MONGODB.HOST = "127.0.0.1"
	conf.MONGODB.PORT = "27017"
	conf.MONGODB.DBNAME = "db"
	conf.MONGODB.USER = "admin"
	conf.MONGODB.PASS = "123456"

	startProxyServer(conf)
}
