package tunnel

import (
	"errors"
	"fmt"
	"io"
	"net"
	"proxy_tunnel/logger"
	"testing"
	"time"
)

var data = "1234567890"

func initTunnel(ePort uint, iPort uint) (runTunnel *Tunnel) {
	runTunnel = &Tunnel{EPort: ePort, IPort: iPort, Limit: 65536, Window: 86400, IIp: net.ParseIP("127.0.0.1")}
	return
}

func startTestSvr(port int, dc chan []byte, ec chan error) {
	go func() {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			ec <- err
			return
		}
		for {
			conn, err := ln.Accept()
			if err != nil {
				ec <- err
				return
			}
			buf := make([]byte, 2048, 2048)
			for {
				exit := false
				num, err := conn.Read(buf)
				if err != nil && err != io.EOF {
					ec <- err
					return
				}
				switch {
				case num > 0:
					dc <- buf[:num]
				case num == 0 || err == io.EOF:
					dc <- []byte("close")
					conn.Close()
					exit = true
				default:
					ec <- errors.New("Invalid Read Return")
				}
				if exit {
					break
				}
			}
		}
	}()
}

func startSvr(tu *Tunnel) {
	go func() {
		logger.Init("")
		defer logger.Finalize()
		Run(tu)
	}()
}

func TestRun(t *testing.T) {
	tu := initTunnel(64003, 64004)
	ec := make(chan error, 1)
	startSvr(tu)
	startWait := 3
	select {
	case err := <-ec:
		t.Errorf("Start Running Server Error [%s]", err)
	default:
		time.Sleep(1 * time.Second)
		startWait--
		if startWait <= 0 {
			t.Log("Pass Case [Run]")
		}
	}
}

func TestPass(t *testing.T) {
	tu := initTunnel(64001, 64002)
	dc := make(chan []byte, 1)
	ec := make(chan error, 1)
	startTestSvr(64002, dc, ec)
	startSvr(tu)
	time.Sleep(1 * time.Second)
	conn, err := net.Dial("tcp", "127.0.0.1:64001")
	if err != nil {
		t.Errorf("Connect External Port 64001 Error [%s]", err)
	}
	defer conn.Close()
	conn.Write([]byte(data))
	for i := 0; i < 3; {
		select {
		case err = <-ec:
			t.Errorf("Start Running Server Error [%s]", err)
		case recvData := <-dc:
			if string(recvData) != data {
				t.Errorf("Recv [%s] Not Equal To [%s]", string(recvData), data)
			}
			t.Log("Pass Case [Pass]")
			return
		default:
			time.Sleep(1 * time.Second)
			i++
		}
	}
	t.Error("Recv Data Timeout")
}

func TestBlock(t *testing.T) {
	tu := initTunnel(64005, 64006)
	tu.Limit = 2000
	tu.Window = 2000
	dc := make(chan []byte, 1)
	ec := make(chan error, 1)
	startTestSvr(64006, dc, ec)
	startSvr(tu)
	time.Sleep(1 * time.Second)
	conn, err := net.Dial("tcp", "127.0.0.1:64005")
	if err != nil {
		t.Errorf("Connect External Port 64005 Error [%s]", err)
	}
	defer conn.Close()
	headData := make([]byte, 2000, 2000)
	conn.Write(headData)
	checkHead := false
	for {
		select {
		case err = <-ec:
			t.Errorf("Start Running Server Error [%s]", err)
		case recvData := <-dc:
			if !checkHead && len(recvData) != 2000 {
				t.Errorf("Recv Head Size [%d] Not [%d]", len(recvData), 2000)
			} else if checkHead == false {
				checkHead = true
				conn.Write([]byte(data))
			}
			if string(recvData) == "close" {
				t.Log("Pass Case [Block]")
				return
			}
		}
	}
}

func TestResume(t *testing.T) {
	tu := initTunnel(64007, 64008)
	tu.Limit = 2000
	tu.Window = 3
	dc := make(chan []byte, 1)
	ec := make(chan error, 1)
	startTestSvr(64008, dc, ec)
	startSvr(tu)
	time.Sleep(1 * time.Second)
	conn, err := net.Dial("tcp", "127.0.0.1:64007")
	if err != nil {
		t.Errorf("Connect External Port 64007 Error [%s]", err)
	}
	defer conn.Close()
	headData := make([]byte, 2000, 2000)
	conn.Write(headData)
	checkHead := false
	checkResume := false
	for {
		select {
		case err = <-ec:
			t.Errorf("Start Running Server Error [%s]", err)
		case recvData := <-dc:
			if !checkHead && len(recvData) != 2000 {
				t.Errorf("Recv Head Size [%d] Not [%d]", len(recvData), 2000)
			} else if checkHead == false {
				checkHead = true
				conn.Write([]byte(data))
			}
			if checkResume {
				if string(recvData) == data {
					t.Log("Pass Case [Resume]")
					return
				}
				t.Logf("Data Receive Not Match After Resume Expect [%s] Get [%s]", data, string(recvData))
				return
			} else if string(recvData) == "close" {
				conn.Close()
				time.Sleep(3 * time.Second)
				conn, err := net.Dial("tcp", "127.0.0.1:64007")
				if err != nil {
					t.Errorf("ReConnect External Port 64007 Error [%s]", err)
				}
				checkResume = true
				conn.Write([]byte(data))
			}
		}
	}
}
