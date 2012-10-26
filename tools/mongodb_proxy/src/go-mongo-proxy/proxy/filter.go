package proxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
)

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

// file system would reserve 5 precent of blocks by default
const DEFAULT_FS_RESERVED_BLOCKS = 0.05

const BLOCKED = 1
const UNBLOCKED = 0

type FilterAction struct {
	threshold float64   // high water
	enabled   bool      // enable or not
	dirty     chan bool // indicate whether write operation received
	// atomic value, use atomic wrapper function to operate on it
	blocked uint32 // 0 means not block, 1 means block
}

type DiskUsage struct {
	reserved_blocks float64
	total_size      uint64  // bytes, total space size
	static_size     uint64  // bytes, static allocated disk file size
	dynamic_size    uint64  // bytes, dynamic allocated disk file size
	ratio           float64 // percent, dynamic value
}

type IOFilterProtocol struct {
	conn_info  ConnectionInfo
	action     FilterAction
	disk_usage DiskUsage
	shutdown   chan bool
}

func NewIOFilterProtocol(conf *ProxyConfig) *IOFilterProtocol {
	filter := &IOFilterProtocol{
		conn_info: conf.MONGODB,

		action: FilterAction{
			threshold: conf.FILTER.THRESHOLD,
			enabled:   conf.FILTER.ENABLED,
			dirty:     make(chan bool, 100),
			blocked:   UNBLOCKED},

		disk_usage: DiskUsage{
			reserved_blocks: conf.FILTER.FS_RESERVED_BLOCKS},

		shutdown: make(chan bool),
	}

	if conf.FILTER.ENABLED {
		if init_disk_usage(&filter.disk_usage) {
			return filter
		}
	} else {
		return filter
	}

	return nil
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
		return 0, 0
	}

	buf := bytes.NewBuffer(stream[0:4])
	// Note that like BSON documents, all data in the mongo wire
	// protocol is little-endian.
	err := binary.Read(buf, binary.LittleEndian, &message_length)
	if err != nil {
		logger.Error("Failed to do binary read message_length [%s].", err)
		return 0, 0
	}

	buf = bytes.NewBuffer(stream[12:16])
	err = binary.Read(buf, binary.LittleEndian, &op_code)
	if err != nil {
		logger.Error("Failed to do binary read op_code [%s].", err)
		return 0, 0
	}

	if len(stream) >= int(message_length) {
		if op_code == OP_UPDATE ||
			op_code == OP_INSERT ||
			op_code == OP_DELETE {
			f.action.dirty <- true
		}
		return message_length, op_code
	}

	return 0, 0
}

func (f *IOFilterProtocol) MonitDiskUsage() {
	conn_info := &f.conn_info
	disk_usage := &f.disk_usage
	action := &f.action

	var journal_files_size, current_disk_usage uint64

	base_dir := "/store/instance"
	journal_dir := filepath.Join(base_dir, "data", "journal")

	visit_file := func(path string, f os.FileInfo, err error) error {
		if err == nil && !f.IsDir() {
			journal_files_size += uint64(f.Size())
		}
		return nil
	}

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
				goto HandleDiskUsage
			}
		}

	HandleDiskUsage:
		logger.Debug("Recalculate disk usage after getting message from dirty channel.\n")

		session, err := mgo.Dial(conn_info.HOST + ":" + conn_info.PORT)
		if err != nil {
			logger.Error("Failed to connect to %s:%s [%s].", conn_info.HOST,
				conn_info.PORT, err)
			session = nil
			goto Error
		}

		disk_usage.static_size = 0
		disk_usage.dynamic_size = 0
		journal_files_size = 0
		current_disk_usage = 0

		if !read_mongodb_static_size(f, session) {
			goto Error
		}

		if !read_mongodb_dynamic_size(f, session) {
			goto Error
		}

		filepath.Walk(journal_dir, visit_file)
		logger.Debug("Get journal files size %d.", journal_files_size)

		/*
		 * Check condition: (static_size + dynamic_size) >= threshold * total_size
		 */
		current_disk_usage = disk_usage.static_size + disk_usage.dynamic_size + journal_files_size
		logger.Debug("Get current disk occupied size %d.", current_disk_usage)
		disk_usage.ratio = float64(current_disk_usage) /
			float64(disk_usage.total_size)
		if disk_usage.ratio >= action.threshold {
			atomic.StoreUint32(&action.blocked, BLOCKED)
		} else {
			atomic.StoreUint32(&action.blocked, UNBLOCKED)
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
func read_mongodb_static_size(f *IOFilterProtocol, session *mgo.Session) bool {
	conn_info := &f.conn_info
	disk_usage := &f.disk_usage

	var stats bson.M
	var temp int

	admindb := session.DB("admin")
	err := admindb.Login(conn_info.USER, conn_info.PASS)
	if err != nil {
		logger.Error("Failed to login database admin as %s:%s: [%s].",
			conn_info.USER, conn_info.PASS, err)
		return false
	}

	err = admindb.Run(bson.D{{"dbStats", 1}, {"scale", 1}}, &stats)
	if err != nil {
		logger.Error("Failed to get database %s stats [%s].", "admin", err)
		return false
	}

	if !parse_dbstats(stats["nsSizeMB"], &temp) {
		logger.Error("Failed to read admin_namespace_size.")
		return false
	}
	admin_namespace_size := uint64(temp * 1024 * 1024)
	disk_usage.static_size += admin_namespace_size

	if !parse_dbstats(stats["fileSize"], &temp) {
		logger.Error("Failed to read admin_data_file_size.")
		return false
	}
	admin_data_file_size := uint64(temp)
	disk_usage.static_size += admin_data_file_size

	logger.Debug("Get static disk files size %d.", disk_usage.static_size)
	return true
}

func read_mongodb_dynamic_size(f *IOFilterProtocol, session *mgo.Session) bool {
	conn_info := &f.conn_info
	disk_usage := &f.disk_usage

	var stats bson.M
	var temp int

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

	if !parse_dbstats(stats["nsSizeMB"], &temp) {
		logger.Error("Failed to read db_namespace_size.")
		return false
	}
	db_namespace_size := uint64(temp * 1024 * 1024)
	disk_usage.dynamic_size += db_namespace_size

	if !parse_dbstats(stats["dataSize"], &temp) {
		logger.Error("Failed to read db_data_size.")
		return false
	}
	db_data_size := uint64(temp)
	disk_usage.dynamic_size += db_data_size

	if !parse_dbstats(stats["indexSize"], &temp) {
		logger.Error("Failed to read db_index_size.")
		return false
	}
	db_index_size := uint64(temp)
	disk_usage.dynamic_size += db_index_size

	logger.Debug("Get dynamic disk files size %d.", disk_usage.dynamic_size)
	return true
}

/******************************************/
/*                                        */
/*       Internel Support Routines        */
/*                                        */
/******************************************/
func init_disk_usage(disk_usage *DiskUsage) bool {
	if disk_usage.reserved_blocks == 0 {
		disk_usage.reserved_blocks = DEFAULT_FS_RESERVED_BLOCKS
	}
	disk_usage.total_size = 0
	disk_usage.static_size = 0
	disk_usage.dynamic_size = 0
	disk_usage.ratio = 0.0

	base_dir := "/store/instance"
	fd, err := syscall.Open(base_dir, syscall.O_RDONLY, 0x664)
	if err != nil {
		logger.Error("%s does not exist, ignore disk quota filter.", base_dir)
		return false
	}
	defer syscall.Close(fd)

	var statfs syscall.Statfs_t
	err = syscall.Fstatfs(fd, &statfs)
	if err != nil {
		logger.Error("Failed to get %s file system stats [%s].", base_dir, err)
		return false
	}

	total_size := uint64(statfs.Bsize) * uint64(float64(statfs.Blocks)*
		float64(1.0-disk_usage.reserved_blocks))
	logger.Debug("Get total disk size %d.", total_size)
	disk_usage.total_size = total_size
	return true
}

func parse_dbstats(value interface{}, result *int) bool {
	temp, err := strconv.Atoi(fmt.Sprintf("%d", value))
	if err != nil {
		logger.Error("Failed to convert data type: [%v].", err)
		return false
	}
	*result = temp
	return true
}
