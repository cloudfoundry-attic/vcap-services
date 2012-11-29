package proxy

import (
	"sync/atomic"
	"time"
)

const BLOCKED = 1
const UNBLOCKED = 0

type FilterConfig struct {
	BASE_DIR        string // mongo data base dir
	QUOTA_FILES     uint32 // quota file number
	QUOTA_DATA_SIZE uint32 // megabytes
	ENABLED         bool   // enable or not, filter proxy or normal proxy
}

type ConnectionInfo struct {
	HOST   string
	PORT   string
	DBNAME string
	USER   string
	PASS   string
}

type Filter interface {
	FilterEnabled() bool
	PassFilter() bool
	StorageMonitor()
}

type ProxyFilterImpl struct {
	// atomic value, use atomic wrapper function to operate on it
	blocked uint32 // 0 means not block, 1 means block

	config *FilterConfig
	mongo  *ConnectionInfo
}

func NewFilter(conf *FilterConfig, conn *ConnectionInfo) *ProxyFilterImpl {
	return &ProxyFilterImpl{
		blocked: UNBLOCKED,
		config:  conf,
		mongo:   conn}
}

func (filter *ProxyFilterImpl) FilterEnabled() bool {
	return filter.config.ENABLED
}

func (filter *ProxyFilterImpl) PassFilter() bool {
	// FIXME: fake filter handler
	return true
}

func (filter *ProxyFilterImpl) StorageMonitor() {
	for {
		// FIXME: fake monitor handler
		atomic.StoreUint32(&filter.blocked, UNBLOCKED)
		time.Sleep(1 * time.Second)
	}
}
