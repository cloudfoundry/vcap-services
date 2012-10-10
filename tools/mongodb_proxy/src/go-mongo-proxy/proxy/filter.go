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

/*
type StandardHeader struct {
    message_length int32
    request_id     int32
    response_to    int32
    op_code        int32
}
*/

const RESPONSE_HEADER_SIZE = 20

/*
type ResponseHeader struct {
    response_flags  int32
    cursor_id       int64
    starting_from   int32
    number_returned int32
}
*/

type ConnectionInfo struct {
	mongo_host string // "127.0.0.1" by default
	mongo_port string // "27017" by default
	mongo_db   string
	mongo_user string
	mongo_pass string
}

type FilterAction struct {
	interval  uint32  // scan interval, seconds
	threshold float64 // high warter, 0.9 by default
	enabled   bool    // enable or not
	// atmoic value, use atomic wrapper function to operate on it
	io_block uint32 // 0 means not block, 1 means block
}

type DiskUsageStats struct {
	fs_reserved_blocks   float64 // 5 precent of blocks are reserved by default
	total_disk_size      uint64  // bytes, static value
	journal_files_size   uint64  // bytes, dynamic value
	admin_namespace_size uint64  // bytes, static value
	admin_data_file_size uint64  // bytes, static value
	db_namespace_size    uint64  // bytes, dynamic value
	db_data_size         uint64  // bytes, dynamic value
	db_index_size        uint64  // bytes, dynamic value
	disk_usage           float64 // percent, dynamic value
}

type IOFilterProtocol struct {
	conn_info        ConnectionInfo
	filter_action    FilterAction
	disk_usage_stats DiskUsageStats
	shutdown         chan string
}

func NewIOFilterProtocol(conf *ProxyConfig) *IOFilterProtocol {
	filter := &IOFilterProtocol{
		conn_info: ConnectionInfo{conf.MONGODB.HOST, conf.MONGODB.PORT,
			conf.MONGODB.DBNAME, conf.MONGODB.USER,
			conf.MONGODB.PASS},
		filter_action: FilterAction{conf.FILTER.INTERVAL,
			conf.FILTER.THRESHOLD,
			conf.FILTER.ENABLED,
			0},
		disk_usage_stats: DiskUsageStats{},
		shutdown:         make(chan string),
	}

	if conf.FILTER.ENABLED {
		if init_disk_usage_stats(&filter.disk_usage_stats) {
			go report_disk_usage(filter)
			return filter
		}
	} else {
		return filter
	}

	return nil
}

func (f *IOFilterProtocol) DestroyFilter() {
	f.shutdown <- "exit"
	time.Sleep(time.Second * time.Duration(f.filter_action.interval))
}

func (f *IOFilterProtocol) FilterEnabled() bool {
	return f.filter_action.enabled
}

func (f *IOFilterProtocol) ProcessFilter(op_code int32) (pass bool) {
	return ((op_code != OP_UPDATE) && (op_code != OP_INSERT)) ||
		(atomic.LoadUint32(&f.filter_action.io_block) == 0)
}

func (f *IOFilterProtocol) HandleMsgHeader(stream []byte) (message_length,
	op_code int32) {
	if len(stream) >= STANDARD_HEADER_SIZE {
		buf := bytes.NewBuffer(stream[0:4])
		// Note that like BSON documents, all data in the mongo wire
		// protocol is little-endian.
		err := binary.Read(buf, binary.LittleEndian, &message_length)
		if err != nil {
			logger.Error("Failed to do binary read message_length [%s].", err)
			// additional handler?
		}
		buf = bytes.NewBuffer(stream[12:16])
		err = binary.Read(buf, binary.LittleEndian, &op_code)
		if err != nil {
			logger.Error("Failed to do binary read op_code [%s].", err)
			// additional handler?
		}

		if len(stream) >= int(message_length) {
			return message_length, op_code
		}
	}
	return 0, 0
}

/******************************************/
/*                                        */
/*          Internal Go Routine           */
/*                                        */
/******************************************/
func report_disk_usage(filter *IOFilterProtocol) {
	conn_info := &filter.conn_info
	disk_usage_stats := &filter.disk_usage_stats
	filter_action := &filter.filter_action

	var admindb, db *mgo.Database
	var stats bson.M
	var temp int
	var fix_size, occupied uint64

	base_dir := "/store/instance"
	journal_dir := filepath.Join(base_dir, "data", "journal")

	visit_file := func(path string, f os.FileInfo, err error) error {
		if err == nil && !f.IsDir() {
			disk_usage_stats.journal_files_size += uint64(f.Size())
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
		case <-filter.shutdown:
			break
		default:
			time.Sleep(time.Second * time.Duration(filter_action.interval))
		}

		admindb = session.DB("admin")
		err = admindb.Login(conn_info.mongo_user, conn_info.mongo_pass)
		if err != nil {
			logger.Error("Failed to login database admin as %s:%s: [%s].",
				conn_info.mongo_user, conn_info.mongo_pass, err)
			goto Error
		}
		err = admindb.Run(bson.D{{"dbStats", 1}, {"scale", 1}}, &stats)
		if err == nil {
			temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["nsSizeMB"]))
			if err == nil {
				disk_usage_stats.admin_namespace_size = uint64(temp * 1024 * 1024)
			} else {
				logger.Error("Failed to read admin_namespace_size: [%s].", err)
				goto Error
			}
			temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["fileSize"]))
			if err == nil {
				disk_usage_stats.admin_data_file_size = uint64(temp)
			} else {
				logger.Error("Failed to read admin_data_file_size: [%s].", err)
				goto Error
			}
		} else {
			logger.Error("Failed to get database %s stats [%s].", "admin", err)
			goto Error
		}

		fix_size = 0
		fix_size += disk_usage_stats.admin_namespace_size
		fix_size += disk_usage_stats.admin_data_file_size
		logger.Debug("Get fixed disk occupied size %d.", fix_size)

		db = session.DB(conn_info.mongo_db)
		err = db.Login(conn_info.mongo_user, conn_info.mongo_pass)
		if err != nil {
			logger.Error("Failed to login database db as %s:%s: [%s].",
				conn_info.mongo_user, conn_info.mongo_pass, err)
			goto Error
		}
		err = db.Run(bson.D{{"dbStats", 1}, {"scale", 1}}, &stats)
		if err == nil {
			temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["nsSizeMB"]))
			if err == nil {
				disk_usage_stats.db_namespace_size = uint64(temp * 1024 * 1024)
			} else {
				logger.Error("Failed to read db_namespace_size: [%s].", err)
				goto Error
			}
			temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["dataSize"]))
			if err == nil {
				disk_usage_stats.db_data_size = uint64(temp)
			} else {
				logger.Error("Failed to read db_data_size: [%s].", err)
				goto Error
			}
			temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["indexSize"]))
			if err == nil {
				disk_usage_stats.db_index_size = uint64(temp)
			} else {
				logger.Error("Failed to read db_index_size: [%s].", err)
				goto Error
			}
		} else {
			logger.Error("Failed to get database %s stats [%s].",
				conn_info.mongo_db, err)
			goto Error
		}

		disk_usage_stats.journal_files_size = 0
		filepath.Walk(journal_dir, visit_file)

		occupied = fix_size
		occupied += disk_usage_stats.db_namespace_size
		occupied += disk_usage_stats.db_data_size
		occupied += disk_usage_stats.db_index_size
		occupied += disk_usage_stats.journal_files_size
		logger.Debug("Get current disk occupied size %d.", occupied)

		/*
		 * Check condition
		 *
		 * occupied = 0
		 * occupied += journal_disk_size
		 * occupied += admin_namespace_size
		 * occupied += admin_data_file_size
		 * occupied += db_namespace_size
		 * occupied += db_data_size
		 * occupied += db_index_size
		 * occupied >= threshold * total_disk_size ???
		 */
		disk_usage_stats.disk_usage = float64(occupied) /
			float64(disk_usage_stats.total_disk_size)
		if disk_usage_stats.disk_usage >= filter_action.threshold {
			atomic.StoreUint32(&filter_action.io_block, 1)
		} else {
			atomic.StoreUint32(&filter_action.io_block, 0)
		}

		continue

	Error:
		atomic.StoreUint32(&filter_action.io_block, 1)
	}
}

/******************************************/
/*                                        */
/*       Internel Support Routines        */
/*                                        */
/******************************************/
func init_disk_usage_stats(disk_usage_stats *DiskUsageStats) bool {
	disk_usage_stats.fs_reserved_blocks = 0.05
	disk_usage_stats.total_disk_size = 0
	disk_usage_stats.journal_files_size = 0
	disk_usage_stats.admin_namespace_size = 0
	disk_usage_stats.admin_data_file_size = 0
	disk_usage_stats.db_namespace_size = 0
	disk_usage_stats.db_data_size = 0
	disk_usage_stats.db_index_size = 0
	disk_usage_stats.disk_usage = 0.0

	base_dir := "/store/instance"
	fd, err := syscall.Open(base_dir, syscall.O_RDONLY, 0x664)
	if err != nil {
		logger.Error("%s does not exist, ignore disk quota filter.", base_dir)
	} else {
		defer syscall.Close(fd)

		var statfs syscall.Statfs_t
		err = syscall.Fstatfs(fd, &statfs)
		if err != nil {
			logger.Error("Failed to get %s file system stats [%s].",
				base_dir, err)
			// TODO: additional handler?
		} else {
			total_size := uint64(statfs.Bsize) * uint64(float64(statfs.Blocks)*
				float64(1.0-disk_usage_stats.fs_reserved_blocks))
			logger.Debug("Get total disk size %d.", total_size)
			disk_usage_stats.total_disk_size = total_size
			return true
		}
	}
	return false
}
