package main

import (
	"daylimit_ng/daylimit_ng"
	. "launchpad.net/gocheck"
	"testing"
	"time"
)

type MainSuite struct{}

var _ = Suite(&MainSuite{})

func Test(t *testing.T) { TestingT(t) }

func (ms *MainSuite) TestSizeCheck(c *C) {
	daylimit_ng.LoadConfig("./config/config.yml")
	config := daylimit_ng.Config()
	daylimit_ng.InitLog("")
	warden = &daylimit_ng.Warden{
		Bin:          config.WardenBin,
		BlockRate:    config.BlockRate,
		BlockBurst:   config.BlockRate,
		UnblockRate:  config.UnblockRate,
		UnblockBurst: config.UnblockRate,
	}

	SizeCheck("test", 1000)
	time.Sleep(time.Duration(config.FetchInteval) * time.Second)
	SizeCheck("test", 10000)
	c.Assert(items["test"].Status, Equals, int8(BLOCK))
	time.Sleep(time.Duration(config.LimitWindow+1) * time.Second)
	SizeCheck("test", 10000)
	c.Assert(items["test"].Status, Equals, int8(UNBLOCK))
}
