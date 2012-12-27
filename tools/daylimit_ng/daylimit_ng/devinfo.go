package daylimit_ng

import (
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

var idReg, rxtxReg *regexp.Regexp

const (
	IFCONFIG = "/sbin/ifconfig"
	IDREG    = "w-(\\w+)-0"
	RXTXREG  = "RX bytes:([0-9]+).*TX bytes:([0-9]+).*"
)

func GetList() (info map[string]int64, err error) {
	ifCmd := exec.Command(IFCONFIG)
	var output []byte
	output, err = ifCmd.Output()
	if err != nil {
		return
	}
	if idReg == nil {
		if idReg, err = regexp.Compile(IDREG); err != nil {
			return
		}
	}
	if rxtxReg == nil {
		if rxtxReg, err = regexp.Compile(RXTXREG); err != nil {
			return
		}
	}
	info = make(map[string]int64)
	id := ""
	// Sample output for ifconfig
	// We need to get the RX bytes and TX bytes for each warden container
	// w-16h43e4pmm8-0 Link encap:Ethernet  HWaddr 96:31:34:1c:6c:2d
	// inet addr:10.254.0.1  Bcast:10.254.0.3  Mask:255.255.255.252
	// inet6 addr: fe80::9431:34ff:fe1c:6c2d/64 Scope:Link
	// UP BROADCAST RUNNING MULTICAST  MTU:1500  Metric:1
	// RX packets:4669 errors:0 dropped:0 overruns:0 frame:0
	// TX packets:4550 errors:0 dropped:0 overruns:0 carrier:0
	// collisions:0 txqueuelen:1000
	// RX bytes:516592 (516.5 KB)  TX bytes:505840 (505.8 KB)
	for _, line := range strings.Split(string(output), "\n") {
		if id == "" {
			ms := idReg.FindStringSubmatch(line)
			if ms != nil {
				id = ms[1]
			}
		} else {
			sizes := rxtxReg.FindStringSubmatch(line)
			if sizes != nil {
				rx, _ := strconv.ParseInt(sizes[1], 0, 64)
				tx, _ := strconv.ParseInt(sizes[2], 0, 64)
				info[id] = rx + tx
				id = ""
			}
		}
	}
	return
}
