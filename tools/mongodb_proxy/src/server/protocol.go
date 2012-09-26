package server

import (
    "syscall"
    "bytes"
    "encoding/binary"
    "path/filepath"
    "os"
    "time"
    "strconv"
    "fmt"
    "sync/atomic"
    "go-mongo/mongo"
)

const OP_REPLY        = 1
const OP_MSG          = 1000
const OP_UPDATE       = 2001
const OP_INSERT       = 2002
const RESERVED        = 2003
const OP_QUERY        = 2004
const OP_GETMORE      = 2005
const OP_DELETE       = 2006
const OP_KILL_CURSORS = 2007

const STANDARD_HEADER_SIZE = 16
const RESPONSE_HEADER_SIZE = 20

type StandardHeader struct {
    message_length int32
    request_id     int32
    response_to    int32
    op_code        int32
}

type ResponseHeader struct {
    response_flags  int32
    cursor_id       int64
    starting_from   int32
    number_returned int32
}

var filter_interval          uint32 = 3   // seconds
var filter_threshold        float64 = 0.8

var mongo_host               string = "127.0.0.1"
var mongo_port               string = "27017"
var mongo_db                 string
var mongo_user               string
var mongo_pass               string

var total_disk_size         float64 = 0.0 // kilobytes, static value
var journal_files_size      float64 = 0.0 // kilobytes, dynamic value
var admin_namespace_size    float64 = 0.0 // kilobytes, static value
var admin_data_file_size    float64 = 0.0 // kilobytes, static value
var db_namespace_size       float64 = 0.0 // kilobytes, dynamic value
var db_data_size            float64 = 0.0 // kilobytes, dynamic value
var db_index_size           float64 = 0.0 // kilobytes, dynamic value

var shutdown            chan string

var disk_usage              float64 = 0.0 // atomic variable ?????
var io_block                 uint32 = 0   // 0 means not block, 1 means block

var base_dir                string = "/store/instance"
var journal_dir             string = filepath.Join(base_dir, "data", "journal")

func setup_filter(conf ProxyConfig) {
    filter_interval  = conf.FILTER.INTERVAL
    filter_threshold = conf.FILTER.THRESHOLD

    mongo_host = conf.MONGODB.HOST
    mongo_port = conf.MONGODB.PORT
    mongo_db   = conf.MONGODB.DBNAME
    mongo_user = conf.MONGODB.USER
    mongo_pass = conf.MONGODB.PASS

    fd, err := syscall.Open(base_dir, syscall.O_RDONLY, 0x664)
    if err != nil {
        logger.Info("%s does not exist, ignore disk quota filter.", base_dir)
    } else {
        defer syscall.Close(fd)

        var statfs syscall.Statfs_t
        err = syscall.Fstatfs(fd, &statfs)
        if err != nil {
            logger.Info("Failed to get %s file system stats [%s].", base_dir, err)
            // TODO: additional handler?
        } else {
            total_disk_size = float64(statfs.Bsize * int64(statfs.Blocks))
            logger.Info("Get total disk size %f.", total_disk_size)

            shutdown = make(chan string)
            go report_disk_usage(shutdown)
        }
    }
}

func destroy_filter() {
    shutdown <- "exit" 
    time.Sleep(time.Second * time.Duration(filter_interval))
}

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
 * occupied >= 0.9 * total_disk_size ???
 *
 */
func report_disk_usage(c chan string) {
    conn, err := mongo.Dial(mongo_host + ":" + mongo_port)
    if err != nil {
        logger.Error("Failed to connect to %s:%s [%s].", mongo_host, mongo_port, err)
        os.Exit(-1)
    }
    defer conn.Close()

    var stats mongo.M
    var temp int

    db := mongo.Database{conn, "admin", mongo.DefaultLastErrorCmd}
    db.Authenticate(mongo_user, mongo_pass)
    err = db.Run(mongo.D{{ "dbStats", 1}, {"scale", 1024 }}, &stats)
    if err == nil {
        temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["nsSize"]))
        if err != nil {
            admin_namespace_size = float64(temp * 1024)
        }
        temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["fileSize"]))
        if err != nil {
            admin_data_file_size = float64(temp)
        }
    } else {
        logger.Error("Failed to get database %s stats [%s].", "admin", err)
    }

    fix_size := 0.0
    fix_size += admin_namespace_size
    fix_size += admin_data_file_size

    for {
        db = mongo.Database{conn, mongo_db, mongo.DefaultLastErrorCmd}
        db.Authenticate(mongo_user, mongo_pass)
        err = db.Run(mongo.D{{ "dbStats", 1}, {"scale", 1024 }}, &stats)
        if err == nil {
            temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["nsSize"]))
            if err != nil {
                db_namespace_size = float64(temp * 1024)
            }
            temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["dataSize"]))
            if err != nil {
                db_data_size = float64(temp)
            }
            temp, err = strconv.Atoi(fmt.Sprintf("%d", stats["indexSize"]))
            if err != nil {
                db_index_size = float64(temp)
            }
        } else {
            logger.Error("Failed to get database %s stats [%s].", mongo_db, err)
        }

        journal_files_size = 0.0
        filepath.Walk(journal_dir, visit_file)
        logger.Info("Get journal files size %f.", journal_files_size)
        
        occupied := fix_size
        occupied += db_namespace_size
        occupied += db_data_size
        occupied += db_index_size
        occupied += journal_files_size
        disk_usage = occupied / total_disk_size
        if disk_usage >= filter_threshold {
            atomic.StoreUint32(&io_block, 1)
        } else {
            atomic.StoreUint32(&io_block, 0)
        }

        select {
        case <-c:
            break
        default:
            time.Sleep(time.Second * time.Duration(filter_interval))
        }
    }
}

func filter(op_code int32) (pass bool) {
    if atomic.LoadUint32(&io_block) == 1 {
        switch op_code {
        case OP_UPDATE:
        case OP_INSERT:
            return false
        }
    }
    return true
}

func handle_msg_header(stream []byte) (message_length, op_code int32) {
    if len(stream) >= STANDARD_HEADER_SIZE {
        buf := bytes.NewBuffer(stream[0:4])
        // Note that like BSON documents, all data in the mongo wire protocol is little-endian.
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

/*
 * Support Routines
 */
func visit_file(path string, f os.FileInfo, err error) error {
    if err == nil && !f.IsDir() {
        journal_files_size += float64(f.Size())
    }
    return nil
}
