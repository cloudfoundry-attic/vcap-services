package proxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync/atomic"
	"syscall"
)

const OP_UNKNOWN = 0
const OP_REPLY = 1
const OP_MSG = 1000
const OP_UPDATE = 2001
const OP_INSERT = 2002
const RESERVED = 2003
const OP_QUERY = 2004
const OP_GETMORE = 2005
const OP_DELETE = 2006
const OP_KILL_CURSORS = 2007

const STANDARD_HEADER_SIZE = 16
const RESPONSE_HEADER_SIZE = 20

const BLOCKED = 1
const UNBLOCKED = 0

type FilterAction struct {
	base_dir        string // mongodb data base dir
	quota_files     uint32 // quota file number
	dbfiles         map[string]int
	quota_data_size uint32    // megabytes
	enabled         bool      // enable or not
	dirty           chan bool // indicate whether write operation received
	// atomic value, use atomic wrapper function to operate on it
	blocked uint32 // 0 means not block, 1 means block
}

type IOFilterProtocol struct {
	conn_info ConnectionInfo
	action    FilterAction
	shutdown  chan bool
}

func NewIOFilterProtocol(conf *ProxyConfig) *IOFilterProtocol {
	filter := &IOFilterProtocol{
		conn_info: conf.MONGODB,

		action: FilterAction{
			base_dir:        conf.FILTER.BASE_DIR,
			quota_files:     conf.FILTER.QUOTA_FILES,
			dbfiles:         make(map[string]int),
			quota_data_size: conf.FILTER.QUOTA_DATA_SIZE,
			enabled:         conf.FILTER.ENABLED,
			dirty:           make(chan bool, 100),
			blocked:         UNBLOCKED},

		shutdown: make(chan bool),
	}

	return filter
}

func (f *IOFilterProtocol) DestroyFilter() {
	f.action.dirty <- true
	f.shutdown <- true
}

func (f *IOFilterProtocol) FilterEnabled() bool {
	return f.action.enabled
}

func (f *IOFilterProtocol) PassFilter(op_code int32) (pass bool) {
	return ((op_code != OP_UPDATE) && (op_code != OP_INSERT)) ||
		(atomic.LoadUint32(&f.action.blocked) == UNBLOCKED)
}

func (f *IOFilterProtocol) HandleMsgHeader(stream []byte) (message_length,
	op_code int32) {
	if len(stream) < STANDARD_HEADER_SIZE {
		return 0, OP_UNKNOWN
	}

	buf := bytes.NewBuffer(stream[0:4])
	// Note that like BSON documents, all data in the mongo wire
	// protocol is little-endian.
	err := binary.Read(buf, binary.LittleEndian, &message_length)
	if err != nil {
		logger.Error("Failed to do binary read message_length [%s].", err)
		return 0, OP_UNKNOWN
	}

	buf = bytes.NewBuffer(stream[12:16])
	err = binary.Read(buf, binary.LittleEndian, &op_code)
	if err != nil {
		logger.Error("Failed to do binary read op_code [%s].", err)
		return 0, OP_UNKNOWN
	}

	if op_code == OP_UPDATE ||
		op_code == OP_INSERT ||
		op_code == OP_DELETE {
		f.action.dirty <- true
	}
	return message_length, op_code
}

func (f *IOFilterProtocol) MonitQuotaFiles() {
	var buf []byte
	var fd, wd int

	conn_info := &f.conn_info
	action := &f.action

	base_dir := action.base_dir
	quota_files := action.quota_files
	filecount := 0

	expr := "^" + conn_info.DBNAME + "\\.[0-9]+"
	re, err := regexp.Compile(expr)
	if err != nil {
		logger.Error("Failed to compile regexp error: [%s].", err)
		goto Error
	}

	filecount = iterate_dbfile(action, base_dir, re)
	logger.Info("At the begining time we have disk files: [%d].", filecount)
	if uint32(filecount) > quota_files {
		logger.Critical("Disk files exceeds quota.")
		atomic.StoreUint32(&action.blocked, BLOCKED)
	}

	fd, err = syscall.InotifyInit()
	if err != nil {
		logger.Error("Failed to call InotifyInit: [%s].", err)
		goto Error
	}

	wd, err = syscall.InotifyAddWatch(fd, base_dir, syscall.IN_CREATE|syscall.IN_OPEN|
		syscall.IN_MOVED_TO|syscall.IN_DELETE)
	if err != nil {
		logger.Error("Failed to call InotifyAddWatch: [%s].", err)
		syscall.Close(fd)
		goto Error
	}

	buf = make([]byte, 256)
	for {
		nread, err := syscall.Read(fd, buf)
		if nread < 0 {
			if err == syscall.EINTR {
				break
			} else {
				logger.Error("Failed to read inotify event: [%s].", err)
			}
		} else {
			err = parse_inotify_event(action, buf[0:nread], re, &filecount)
			if err != nil {
				logger.Error("Failed to parse inotify event.")
				atomic.StoreUint32(&action.blocked, BLOCKED)
			} else {
				logger.Debug("Current db disk file number: [%d].", filecount)
				if uint32(filecount) > quota_files {
					logger.Critical("Disk files exceeds quota.")
					atomic.StoreUint32(&action.blocked, BLOCKED)
				} else {
					atomic.CompareAndSwapUint32(&action.blocked, BLOCKED, UNBLOCKED)
				}
			}
		}
	}

	syscall.InotifyRmWatch(fd, uint32(wd))
	syscall.Close(fd)
	return

Error:
	atomic.StoreUint32(&action.blocked, BLOCKED)
}

func (f *IOFilterProtocol) MonitQuotaDataSize() {
	conn_info := &f.conn_info
	action := &f.action

	var dbsize float64

	for {
		select {
		case <-f.shutdown:
			return
		default:
		}

		// if dirty channel is empty then go routine will block
		<-action.dirty
		// featch all pending requests from the channel
		for {
			select {
			case <-action.dirty:
				continue
			default:
				// NOTE: here 'break' can not skip out of for loop
				goto HandleQuotaDataSize
			}
		}

	HandleQuotaDataSize:
		// if 'blocked' flag is set then it indicates that disk file number
		// exceeds the QuotaFile, then DataSize account is not necessary.
		if atomic.LoadUint32(&f.action.blocked) == BLOCKED {
			continue
		}

		logger.Debug("Recalculate data size after getting message from dirty channel.\n")

		session, err := mgo.Dial(conn_info.HOST + ":" + conn_info.PORT)
		if err != nil {
			logger.Error("Failed to connect to %s:%s [%s].", conn_info.HOST,
				conn_info.PORT, err)
			session = nil
			goto Error
		}

		dbsize = 0.0

		if !read_mongodb_dbsize(f, &dbsize, session) {
			goto Error
		}

		logger.Debug("Get current disk occupied size %v.", dbsize)
		if dbsize >= float64(action.quota_data_size*1024*1024) {
			atomic.StoreUint32(&action.blocked, BLOCKED)
		} else {
			atomic.CompareAndSwapUint32(&action.blocked, BLOCKED, UNBLOCKED)
		}

		session.Close()
		continue

	Error:
		if session != nil {
			session.Close()
		}
		atomic.StoreUint32(&action.blocked, BLOCKED)
	}
}

/******************************************/
/*                                        */
/*          Internal Go Routine           */
/*                                        */
/******************************************/
func read_mongodb_dbsize(f *IOFilterProtocol, size *float64, session *mgo.Session) bool {
	conn_info := &f.conn_info

	var stats bson.M
	var temp float64

	db := session.DB(conn_info.DBNAME)
	err := db.Login(conn_info.USER, conn_info.PASS)
	if err != nil {
		logger.Error("Failed to login database db as %s:%s: [%s].",
			conn_info.USER, conn_info.PASS, err)
		return false
	}

	err = db.Run(bson.D{{"dbStats", 1}, {"scale", 1}}, &stats)
	if err != nil {
		logger.Error("Failed to get database %s stats [%s].",
			conn_info.DBNAME, err)
		return false
	}

	if !parse_dbstats(stats["dataSize"], &temp) {
		logger.Error("Failed to read db_data_size.")
		return false
	}
	db_data_size := temp
	*size += db_data_size

	if !parse_dbstats(stats["indexSize"], &temp) {
		logger.Error("Failed to read db_index_size.")
		return false
	}
	db_index_size := temp
	*size += db_index_size

	logger.Debug("Get db data size %v.", *size)
	return true
}

/******************************************/
/*                                        */
/*       Internal Support Routines        */
/*                                        */
/******************************************/
func iterate_dbfile(f *FilterAction, dirpath string, re *regexp.Regexp) int {
	filecount := 0
	dbfiles := f.dbfiles
	visit_file := func(path string, f os.FileInfo, err error) error {
		if err == nil && !f.IsDir() && re.Find([]byte(f.Name())) != nil {
			dbfiles[f.Name()] = 1
			filecount++
		}
		return nil
	}
	filepath.Walk(dirpath, visit_file)
	return filecount
}

func parse_inotify_event(f *FilterAction, buf []byte, re *regexp.Regexp, filecount *int) error {
	var event syscall.InotifyEvent
	var filename string

	index := 0
	dbfiles := f.dbfiles
	for index < len(buf) {
		err := binary.Read(bytes.NewBuffer(buf[0:len(buf)]), binary.LittleEndian, &event)
		if err != nil {
			logger.Error("Failed to do binary read inotify event: [%s].", err)
			return err
		}
		start := index + syscall.SizeofInotifyEvent
		end := start + int(event.Len)
		filename = string(buf[start:end])
		if re.Find([]byte(filename)) != nil {
			logger.Debug("Get filename from inotify event: [%s].", filename)
			switch event.Mask {
			case syscall.IN_CREATE:
				fallthrough
			case syscall.IN_OPEN:
				fallthrough
			case syscall.IN_MOVED_TO:
				if _, ok := dbfiles[filename]; !ok {
					*filecount++
					dbfiles[filename] = 1
				}
			case syscall.IN_DELETE:
				*filecount--
				delete(dbfiles, filename)
			}
		}
		index = end
	}
	return nil
}

/*
 * NOTE: if disk data file gets very large, then the returned data size value would
 *       be encoded in 'float' format but not 'integer' format, such as
 *       2.098026476e+09, if we parse the value in 'integer' format then we get
 *       error. It always works if we parse an 'integer' value in 'float' format.
 */
func parse_dbstats(value interface{}, result *float64) bool {
	temp, err := strconv.ParseFloat(fmt.Sprintf("%v", value), 64)
	if err != nil {
		logger.Error("Failed to convert data type: [%v].", err)
		return false
	}
	*result = temp
	return true
}
