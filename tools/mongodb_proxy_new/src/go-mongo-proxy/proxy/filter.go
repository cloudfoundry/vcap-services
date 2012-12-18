package proxy

import (
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const BLOCKED = 1
const UNBLOCKED = 0

const DIRTY_EVENT = 'd'
const STOP_EVENT = 's'

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
	PassFilter(op_code int) bool
	IsDirtyEvent(op_code int) bool
	EnqueueDirtyEvent()
	StartStorageMonitor()
	WaitForFinish()
}

type ProxyFilterImpl struct {
	// atomic value, use atomic wrapper function to operate on it
	mablocked uint32 // 0 means not block, 1 means block
	mfblocked uint32 // 0 means not block, 1 means block

	// event channel
	evtchn1 chan byte // DIRTY event, STOP event
	evtchn2 chan byte // DIRTY event

	config *FilterConfig
	mongo  *ConnectionInfo

	// goroutine wait channel
	lock    sync.Mutex
	running uint32
	wait    chan byte
}

func NewFilter(conf *FilterConfig, conn *ConnectionInfo) *ProxyFilterImpl {
	return &ProxyFilterImpl{
		mablocked: UNBLOCKED,
		mfblocked: UNBLOCKED,
		evtchn1:   make(chan byte, 100),
		evtchn2:   make(chan byte, 1),
		config:    conf,
		mongo:     conn,
		running:   0,
		wait:      make(chan byte, 1)}
}

func (filter *ProxyFilterImpl) FilterEnabled() bool {
	return filter.config.ENABLED
}

// If data size exceeds quota or disk files number exceeds quota,
// then we block the client operations.
func (filter *ProxyFilterImpl) PassFilter(op_code int) bool {
	// When we read state of 'mfblockeded', the state of 'mablocked' may
	// change from 'UNBLOCKED' to 'BLOCKED', so, our implementation only
	// achieves soft limit not hard limit. Since we have over quota storage
	// space settings, this is not a big issue.
	return (op_code != OP_UPDATE && op_code != OP_INSERT) ||
		(atomic.LoadUint32(&filter.mablocked) == UNBLOCKED &&
			atomic.LoadUint32(&filter.mfblocked) == UNBLOCKED)
}

func (filter *ProxyFilterImpl) IsDirtyEvent(op_code int) bool {
	return op_code == OP_UPDATE || op_code == OP_INSERT ||
		op_code == OP_DELETE
}

func (filter *ProxyFilterImpl) EnqueueDirtyEvent() {
	filter.evtchn1 <- DIRTY_EVENT
}

func (filter *ProxyFilterImpl) StartStorageMonitor() {
	go filter.MonitorQuotaDataSize()
	go filter.MonitorQuotaFiles()
}

func (filter *ProxyFilterImpl) WaitForFinish() {
	if filter.config.ENABLED {
		filter.evtchn1 <- STOP_EVENT
		filter.evtchn2 <- STOP_EVENT
		<-filter.wait
	}
}

// Data size monitor depends on output format of mongodb command, the format is
// united in all of current supported versions, 1.8, 2.0 and 2.2. And we must
// get the data size information from mongodb command interface.
func (filter *ProxyFilterImpl) MonitorQuotaDataSize() {
	dbhost := filter.mongo.HOST
	port := filter.mongo.PORT
	dbname := filter.mongo.DBNAME
	user := filter.mongo.USER
	pass := filter.mongo.PASS
	quota_data_size := filter.config.QUOTA_DATA_SIZE

	filter.lock.Lock()
	filter.running++
	filter.lock.Unlock()

	var size float64
	for {
		event := <-filter.evtchn1
		if event == STOP_EVENT {
			break
		}

		if err := startMongoSession(dbhost, port); err != nil {
			logger.Error("Failed to connect to %s:%s, [%s].", dbhost, port, err)
			goto Error
		}

		if !readMongodbSize(dbname, user, pass, &size) {
			logger.Error("Failed to read database '%s' size.", dbname)
			goto Error
		}

		if size >= float64(quota_data_size)*float64(1024*1024) {
			logger.Critical("Data size exceeds quota.")
			atomic.StoreUint32(&filter.mablocked, BLOCKED)
		} else {
			atomic.StoreUint32(&filter.mablocked, UNBLOCKED)
		}

		continue
	Error:
		atomic.StoreUint32(&filter.mablocked, BLOCKED)
	}

	endMongoSession()

	filter.lock.Lock()
	filter.running--
	if filter.running == 0 {
		filter.wait <- STOP_EVENT
	}
	filter.lock.Unlock()
}

// Data file number monitor depends on mongodb disk file layout, the layout is
// united in all of current supported versions, 1.8, 2.0 and 2.2.
//
// For example:
//
// Say base dir path is '/tmp/mongodb' and database name is 'db', then the disk
// file layout would be, /tmp/mongodb/db.ns, /tmp/mongodb/db.0, /tmp/mongodb/db.1,
// and /tmp/mongodb/db.2 ...
func (filter *ProxyFilterImpl) MonitorQuotaFiles() {
	var fd, wd int
	var err error
	buffer := make([]byte, 256)
	dbfiles := make(map[string]int)

	dbname := filter.mongo.DBNAME
	base_dir := filter.config.BASE_DIR
	quota_files := filter.config.QUOTA_FILES

	filter.lock.Lock()
	filter.running++
	filter.lock.Unlock()

	old_filecount := 0
	new_filecount := 0
	old_filecount = iterateDatafile(dbname, base_dir, dbfiles)
	if old_filecount < 0 {
		logger.Error("Failed to iterate data files under %s.", base_dir)
		goto Error
	}

	logger.Info("At the begining time we have disk files: [%d].", old_filecount)
	if old_filecount > int(quota_files) {
		logger.Critical("Disk files exceeds quota.")
		atomic.StoreUint32(&filter.mfblocked, BLOCKED)
	}

	// Golang does not recommend to invoke system call directly, but
	// it does not contain any 'inotify' wrapper function
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

	for {
		select {
		case event := <-filter.evtchn2:
			if event == STOP_EVENT {
				break
			}
		default:
		}

		_, err := asyncRead(syscall.Read, fd, buffer, time.Second)
		if err != nil {
			if err == ErrTimeout {
				continue
			}
			logger.Error("Failed to read inotify event: [%s].", err)
			break
		} else {
			new_filecount = iterateDatafile(dbname, base_dir, dbfiles)
			if new_filecount < 0 {
				logger.Error("Failed to iterate data files under %s.", base_dir)
				atomic.StoreUint32(&filter.mfblocked, BLOCKED)
			} else if new_filecount > int(quota_files) {
				// Both dataSync and journalFlush thread in mongodb will invoke io operation
				// periodically, so we will get lots of inotify events even the data files number
				// does not change, then there will be lots of unuseful 'exceeds quota' logs.
				if new_filecount != old_filecount {
					old_filecount = new_filecount
					logger.Critical("Disk files exceeds quota.")
				}
				atomic.StoreUint32(&filter.mfblocked, BLOCKED)
			} else {
				old_filecount = new_filecount
				logger.Debug("Current db disk file number: [%d].", new_filecount)
				atomic.StoreUint32(&filter.mfblocked, UNBLOCKED)
			}
		}
	}

	syscall.InotifyRmWatch(fd, uint32(wd))
	syscall.Close(fd)

Error:
	atomic.StoreUint32(&filter.mfblocked, BLOCKED)

	filter.lock.Lock()
	filter.running--
	if filter.running == 0 {
		filter.wait <- STOP_EVENT
	}
	filter.lock.Unlock()
}
