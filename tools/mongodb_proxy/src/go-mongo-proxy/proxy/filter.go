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
	"time"
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

type ConnectionInfo struct {
	mongo_host string
	mongo_port string
	mongo_db   string
	mongo_user string
	mongo_pass string
}

type FilterAction struct {
	threshold float64   // high water
	enabled   bool      // enable or not
	dirty     chan bool // inidicate whether write operation received
	// atomic value, use atomic wrapper function to operate on it
	blocked uint32 // 0 means not block, 1 means block
}

type DiskUsageStats struct {
	fs_reserved_blocks float64
	total_size         uint64  // bytes, total space size
	static_size        uint64  // bytes, static allocated disk file size
	dynamic_size       uint64  // bytes, dynamic allocated disk file size
	disk_usage_ratio   float64 // percent, dynamic value
}

type IOFilterProtocol struct {
	conn_info        ConnectionInfo
	filter_action    FilterAction
	disk_usage_stats DiskUsageStats
	shutdown         chan bool
}

func NewIOFilterProtocol(conf *ProxyConfig) *IOFilterProtocol {
	filter := &IOFilterProtocol{
		conn_info: ConnectionInfo{conf.MONGODB.HOST, conf.MONGODB.PORT,
			conf.MONGODB.DBNAME, conf.MONGODB.USER,
			conf.MONGODB.PASS},
		filter_action: FilterAction{
			threshold: conf.FILTER.THRESHOLD,
			enabled:   conf.FILTER.ENABLED,
			dirty:     make(chan bool, 100),
			blocked:   UNBLOCKED},
		disk_usage_stats: DiskUsageStats{
			fs_reserved_blocks: conf.FILTER.FS_RESERVED_BLOCKS},
		shutdown: make(chan bool),
	}

	if conf.FILTER.ENABLED {
		if init_disk_usage_stats(&filter.disk_usage_stats) {
			return filter
		}
	} else {
		return filter
	}

	return nil
}

func (f *IOFilterProtocol) DestroyFilter() {
	f.filter_action.dirty <- true
	f.shutdown <- true
	time.Sleep(time.Second * time.Duration(1))
}

func (f *IOFilterProtocol) FilterEnabled() bool {
	return f.filter_action.enabled
}

func (f *IOFilterProtocol) ProcessFilter(op_code int32) (pass bool) {
	return ((op_code != OP_UPDATE) && (op_code != OP_INSERT)) ||
		(atomic.LoadUint32(&f.filter_action.blocked) == UNBLOCKED)
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
			f.filter_action.dirty <- true
		}
		return message_length, op_code
	}

	return 0, 0
}

func (f *IOFilterProtocol) MonitDiskUsage() {
	conn_info := &f.conn_info
	disk_usage_stats := &f.disk_usage_stats
	filter_action := &f.filter_action

	var journal_files_size, current_disk_usage uint64

	base_dir := "/store/instance"
	journal_dir := filepath.Join(base_dir, "data", "journal")

	visit_file := func(path string, f os.FileInfo, err error) error {
		if err == nil && !f.IsDir() {
			journal_files_size += uint64(f.Size())
		}
		return nil
	}

	session, err := mgo.Dial(conn_info.mongo_host + ":" + conn_info.mongo_port)
	if err != nil {
		logger.Error("Failed to connect to %s:%s [%s].", conn_info.mongo_host,
			conn_info.mongo_port, err)
		os.Exit(-1)
	}
	defer session.Close()

	for {
		select {
		case <-f.shutdown:
			return
		default:
			goto HandleDirtyRequest
		}

	HandleDirtyRequest:
		// if dirty channel is empty then go routine will block
		<-f.filter_action.dirty
		// featch all pending requests from the channel
		for {
			select {
			case <-f.filter_action.dirty:
				continue
			default:
				// NOTE: "break" seems does not work here
				goto HandleDiskUsage
			}
		}

	HandleDiskUsage:
		logger.Debug("Recalculate disk usage after getting message from dirty channel.\n")

		disk_usage_stats.static_size = 0
		disk_usage_stats.dynamic_size = 0
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
		current_disk_usage = disk_usage_stats.static_size +
			disk_usage_stats.dynamic_size +
			journal_files_size
		logger.Debug("Get current disk occupied size %d.", current_disk_usage)
		disk_usage_stats.disk_usage_ratio = float64(current_disk_usage) /
			float64(disk_usage_stats.total_size)
		if disk_usage_stats.disk_usage_ratio >= filter_action.threshold {
			atomic.StoreUint32(&filter_action.blocked, BLOCKED)
		} else {
			atomic.StoreUint32(&filter_action.blocked, UNBLOCKED)
		}

		continue

	Error:
		atomic.StoreUint32(&filter_action.blocked, BLOCKED)
	}
}

/******************************************/
/*                                        */
/*          Internal Go Routine           */
/*                                        */
/******************************************/
func read_mongodb_static_size(f *IOFilterProtocol, session *mgo.Session) bool {
	conn_info := &f.conn_info
	disk_usage_stats := &f.disk_usage_stats

	var stats bson.M
	var temp int

	admindb := session.DB("admin")
	err := admindb.Login(conn_info.mongo_user, conn_info.mongo_pass)
	if err != nil {
		logger.Error("Failed to login database admin as %s:%s: [%s].",
			conn_info.mongo_user, conn_info.mongo_pass, err)
		return false
	}

	err = admindb.Run(bson.D{{"dbStats", 1}, {"scale", 1}}, &stats)
	if err != nil {
		logger.Error("Failed to get database %s stats [%s].", "admin", err)
		return false
	}

	temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["nsSizeMB"]))
	if err != nil {
		logger.Error("Failed to read admin_namespace_size: [%s].", err)
		return false
	}
	admin_namespace_size := uint64(temp * 1024 * 1024)
	disk_usage_stats.static_size += admin_namespace_size

	temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["fileSize"]))
	if err != nil {
		logger.Error("Failed to read admin_data_file_size: [%s].", err)
		return false
	}
	admin_data_file_size := uint64(temp)
	disk_usage_stats.static_size += admin_data_file_size

	logger.Debug("Get static disk files size %d.", disk_usage_stats.static_size)
	return true
}

func read_mongodb_dynamic_size(f *IOFilterProtocol, session *mgo.Session) bool {
	conn_info := &f.conn_info
	disk_usage_stats := &f.disk_usage_stats

	var stats bson.M
	var temp int

	db := session.DB(conn_info.mongo_db)
	err := db.Login(conn_info.mongo_user, conn_info.mongo_pass)
	if err != nil {
		logger.Error("Failed to login database db as %s:%s: [%s].",
			conn_info.mongo_user, conn_info.mongo_pass, err)
		return false
	}

	err = db.Run(bson.D{{"dbStats", 1}, {"scale", 1}}, &stats)
	if err != nil {
		logger.Error("Failed to get database %s stats [%s].",
			conn_info.mongo_db, err)
		return false
	}

	temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["nsSizeMB"]))
	if err != nil {
		logger.Error("Failed to read db_namespace_size: [%s].", err)
		return false
	}
	db_namespace_size := uint64(temp * 1024 * 1024)
	disk_usage_stats.dynamic_size += db_namespace_size

	temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["dataSize"]))
	if err != nil {
		logger.Error("Failed to read db_data_size: [%s].", err)
		return false
	}
	db_data_size := uint64(temp)
	disk_usage_stats.dynamic_size += db_data_size

	temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["indexSize"]))
	if err != nil {
		logger.Error("Failed to read db_index_size: [%s].", err)
		return false
	}
	db_index_size := uint64(temp)
	disk_usage_stats.dynamic_size += db_index_size

	logger.Debug("Get dynamic disk files size %d.", disk_usage_stats.dynamic_size)
	return true
}

/******************************************/
/*                                        */
/*       Internel Support Routines        */
/*                                        */
/******************************************/
func init_disk_usage_stats(disk_usage_stats *DiskUsageStats) bool {
	if disk_usage_stats.fs_reserved_blocks == 0 {
		disk_usage_stats.fs_reserved_blocks = DEFAULT_FS_RESERVED_BLOCKS
	}
	disk_usage_stats.total_size = 0
	disk_usage_stats.static_size = 0
	disk_usage_stats.dynamic_size = 0
	disk_usage_stats.disk_usage_ratio = 0.0

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
		float64(1.0-disk_usage_stats.fs_reserved_blocks))
	logger.Debug("Get total disk size %d.", total_size)
	disk_usage_stats.total_size = total_size
	return true
}
