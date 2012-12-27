package daylimit_ng

import (
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
)

type Warden struct {
	Bin          string
	BlockRate    int64
	BlockBurst   int64
	UnblockRate  int64
	UnblockBurst int64
}

const (
	CMDTP  = "%s -- limit_bandwidth --handle %s --rate %d --burst %d"
	INFOTP = "%s -- info --handle %s"
)

func (w *Warden) Block(id string) bool {
	return LimitWardenBandwidth(w.Bin, id, w.BlockRate, w.BlockBurst)
}
func (w *Warden) Unblock(id string) bool {
	return LimitWardenBandwidth(w.Bin, id, w.UnblockRate, w.UnblockBurst)
}

func (w *Warden) GetRate(id string) (rate, burst int64, err error) {
	cmdStr := fmt.Sprintf(INFOTP, w.Bin, id)
	cmd := exec.Command(strings.Split(cmdStr, " ")[0], strings.Split(cmdStr, " ")[1:]...)
	info, err := cmd.Output()
	if err != nil {
		Logger().Errorf("Run warden info error [%s]", err)
		return
	}
	rateInfo := map[string]int64{
		"bandwidth_stat.in_rate":   0,
		"bandwidth_stat.out_rate":  0,
		"bandwidth_stat.in_burst":  0,
		"bandwidth_stat.out_burst": 0,
	}
	for _, line := range strings.Split(string(info), "\n") {
		for key, _ := range rateInfo {
			if strings.Contains(line, key) {
				rateInfo[key], _ = strconv.ParseInt(strings.Split(line, " : ")[1], 0, 64)
			}
		}
	}
	rate = int64(math.Max(float64(rateInfo["bandwidth_stat.in_rate"]), float64(rateInfo["bandwidth_stat.out_rate"])))
	burst = int64(math.Max(float64(rateInfo["bandwidth_stat.in_burst"]), float64(rateInfo["bandwidth_stat.out_burst"])))
	return
}

func LimitWardenBandwidth(bin, id string, rate, burst int64) bool {
	cmdStr := fmt.Sprintf(CMDTP, bin, id, rate, burst)
	cmd := exec.Command(strings.Split(cmdStr, " ")[0], strings.Split(cmdStr, " ")[1:]...)
	_, err := cmd.Output()
	if err != nil {
		Logger().Errorf("Run warden limit bandwidth error [%s]", err)
		return false
	}
	return true
}
