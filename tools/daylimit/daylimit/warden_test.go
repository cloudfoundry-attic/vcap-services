package daylimit

import (
	. "launchpad.net/gocheck"
)

type WardenSuite struct{}

var _ = Suite(&WardenSuite{})

func wrapCall(c *C, fun func(c *C)) {
	InitLog("")
	CreateContainer(c)
	defer DestroyContainer(c)
	fun(c)
}

func (ws *WardenSuite) TestBlock(c *C) {
	wrapCall(c, func(c *C) {
		w := WardenObj()
		containerId := ContainerId()
		w.Block(containerId)
		rate, burst, err := w.GetRate(containerId)
		c.Assert((rate-w.BlockRate)/60, Equals, int64(0))
		c.Assert((burst-w.BlockBurst)/60, Equals, int64(0))
		c.Assert(err, IsNil)
	})
}

func (ws *WardenSuite) TestUnblock(c *C) {
	wrapCall(c, func(c *C) {
		w := WardenObj()
		containerId := ContainerId()
		w.Unblock(containerId)
		rate, burst, err := w.GetRate(containerId)
		c.Assert((rate-w.UnblockRate)/120, Equals, int64(0))
		c.Assert((burst-w.UnblockBurst)/120, Equals, int64(0))
		c.Assert(err, IsNil)
	})
}
