package main

import (
	"daylimit/iptablesrun"
	"fmt"
	"testing"
	"time"
)

func TestSizeCheck(t *testing.T) {
	items = make(map[string]*serviceCheckPoint)
	opts = CmdOptions{
		LimitWindow:  10,
		LimitSize:    1000,
		FetchInteval: 5,
	}
	id := "test"

	nInfo := &iptablesrun.RuleInfo{
		Size:    0,
		Status:  iptablesrun.ACCEPT,
		InRule:  fmt.Sprintf("[0:0] -A throughput-count -i w-%s-0 -j ACCEPT", id),
		OutRule: fmt.Sprintf("[0:0] -A throughput-count -o w-%s-0 -j ACCEPT", id),
	}

	iptablesrun.SetRules(
		map[string]*iptablesrun.RuleInfo{
			id: nInfo,
		})
	SizeCheck(id, nInfo)
	nInfo.Size = opts.LimitSize + 1
	SizeCheck(id, nInfo)
	bl := iptablesrun.GetBlockList()
	if _, ok := bl[id]; !ok {
		t.Fatalf("Id [%s] is not blocked when size greater then limit size [%d]\n", id, opts.LimitSize)
	}
	t.Log("Passed the block case")

	time.Sleep(time.Duration(opts.LimitWindow+1) * time.Second)
	nInfo = &iptablesrun.RuleInfo{
		Size:    0,
		Status:  iptablesrun.DROP,
		InRule:  fmt.Sprintf("[0:0] -A throughput-count -i w-%s-0 -j DROP", id),
		OutRule: fmt.Sprintf("[0:0] -A throughput-count -o w-%s-0 -j DROP", id),
	}
	SizeCheck(id, nInfo)
	bl = iptablesrun.GetUnblockList()
	if _, ok := bl[id]; !ok {
		t.Fatalf("Id [%s] is not unblocked when size less then limit size [%d]\n", id, opts.LimitSize)
	}
	t.Log("Passed the unblock case")
}
