package daylimit_ng

import (
	. "launchpad.net/gocheck"
	"os/exec"
	"strings"
	"testing"
)

var w *Warden = nil
var containerId string

func Test(t *testing.T) { TestingT(t) }

func ContainerId() string {
	return containerId
}

func WardenObj() *Warden {
	if w == nil {
		LoadConfig("../config/config.yml")
		config = Config()
		w = &Warden{
			Bin:          config.WardenBin,
			BlockRate:    config.BlockRate,
			BlockBurst:   config.BlockRate,
			UnblockRate:  config.UnblockRate,
			UnblockBurst: config.UnblockRate,
		}
	}
	return w
}

func CreateContainer(c *C) {
	cmd := exec.Command(WardenObj().Bin, "--", "create")
	if out, err := cmd.Output(); err != nil {
		c.Fatalf("Create new container error [%s]", err)
	} else {
		containerId = strings.TrimRight(strings.Split(string(out), " : ")[1], "\n")
	}
}

func DestroyContainer(c *C) {
	cmd := exec.Command(WardenObj().Bin, "--", "destroy", "--handle", containerId)
	if err := cmd.Run(); err != nil {
		c.Fatalf("Create new container error [%s]", err)
	}
	containerId = ""
}
