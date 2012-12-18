package main

import (
	"sync/atomic"
	"time"
)

const BLOCKED = 1
const UNBLOCKED = 0

type FilterConfig struct {
	base_dir        string // mongo data base dir
	quota_files     uint32 // quota file number
	quota_data_size uint32 // megabytes
	enabled         bool   // enable or not, filter proxy or normal proxy
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
}

func NewFilter(conf *FilterConfig) *ProxyFilterImpl {
	return &ProxyFilterImpl{
		blocked: UNBLOCKED,
		config:  conf}
}

func (filter *ProxyFilterImpl) FilterEnabled() bool {
	return filter.config.enabled
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
