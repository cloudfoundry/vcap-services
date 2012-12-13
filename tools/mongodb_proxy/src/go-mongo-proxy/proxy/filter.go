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
	base_dir        string    // mongodb data base dir
	quota_files     uint32    // quota file number
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

func (f *IOFilterProtocol) MonitQuotaDataSize() {
	conn_info := &f.conn_info
	action := &f.action

	dbname := conn_info.DBNAME
	base_dir := action.base_dir
	quota_files := action.quota_files

	dbfiles := make(map[string]int)
	upperbound := float64(action.quota_data_size) * float64(1024*1024)

	var session *mgo.Session
	var err error

	var dbsize float64
	pfilecount := 0
	nfilecount := 0

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

		logger.Debug("Recalculate data size after getting message from dirty channel.\n")

		nfilecount = iterate_dbfile(dbname, base_dir, dbfiles)
		if nfilecount < 0 {
			logger.Error("Failed to iterate data files under %s.", base_dir)
			goto Error
		}

		session, err = mgo.Dial(conn_info.HOST + ":" + conn_info.PORT)
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
		session.Close()

		// disk file last allocation meets following 2 conditions
		// 1. nfilecount > quota file number
		// 2. nfilecount > pfilecount
		if (nfilecount > int(quota_files)) && (nfilecount > pfilecount) {
			logger.Critical("Last allocation for a new disk file, quota exceeds.")
			upperbound = dbsize
		} else if nfilecount < pfilecount {
			// Only 'repair' can shrink disk files.
			logger.Info("Repair database is triggered.")
			upperbound = float64(action.quota_data_size) * float64(1024*1024)
		}

		logger.Debug("Get current disk occupied size %v.", dbsize)
		if dbsize >= upperbound {
			atomic.StoreUint32(&action.blocked, BLOCKED)
		} else {
			atomic.CompareAndSwapUint32(&action.blocked, BLOCKED, UNBLOCKED)
		}

		pfilecount = nfilecount
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
func iterate_dbfile(dbname string, dirpath string, dbfiles map[string]int) int {
	filecount := 0

	expr := "^" + dbname + "\\.[0-9]+"
	re, err := regexp.Compile(expr)
	if err != nil {
		logger.Error("Failed to compile regexp error: [%s].", err)
		return -1
	}

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
