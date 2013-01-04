package daylimit

import (
	. "launchpad.net/gocheck"
)

type DevinfoSuite struct{}

var _ = Suite(&DevinfoSuite{})

func (ts *DevinfoSuite) TestGetList(c *C) {
	CreateContainer(c)
	defer DestroyContainer(c)
	var info map[string]int64
	var err error
	if info, err = GetList(); err != nil {
		c.Fatalf("GetList return error [%s]", err)
	}
	_, ok := info[ContainerId()]
	c.Assert(ok, Equals, true)
}
