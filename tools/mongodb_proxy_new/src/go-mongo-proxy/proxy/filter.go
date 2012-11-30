package proxy

import (
	"sync/atomic"
	"syscall"
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
	PassFilter(op_code int) bool
	IsDirtyEvent(op_code int) bool
	EnqueueDirtyEvent()
	StorageMonitor()
}

type ProxyFilterImpl struct {
	// atomic value, use atomic wrapper function to operate on it
	blocked uint32 // 0 means not block, 1 means block

	// event channel
	evtchn chan byte // 'd' means dirty event, 's' means shutdown event

	config *FilterConfig
	mongo  *ConnectionInfo
}

func NewFilter(conf *FilterConfig, conn *ConnectionInfo) *ProxyFilterImpl {
	return &ProxyFilterImpl{
		blocked: UNBLOCKED,
		evtchn:  make(chan byte, 100),
		config:  conf,
		mongo:   conn}
}

func (filter *ProxyFilterImpl) FilterEnabled() bool {
	return filter.config.ENABLED
}

func (filter *ProxyFilterImpl) PassFilter(op_code int) bool {
	return op_code != OP_UPDATE && op_code != OP_INSERT ||
		atomic.LoadUint32(&filter.blocked) == UNBLOCKED
}

func (filter *ProxyFilterImpl) IsDirtyEvent(op_code int) bool {
	return op_code == OP_UPDATE || op_code == OP_INSERT ||
		op_code == OP_DELETE
}

func (filter *ProxyFilterImpl) EnqueueDirtyEvent() {
	filter.evtchn <- 'd'
}

func (filter *ProxyFilterImpl) StorageMonitor() {
	go filter.MonitorQuotaDataSize()
	go filter.MonitorQuotaFiles()
}

func (filter *ProxyFilterImpl) MonitorQuotaDataSize() {
	dbhost := filter.mongo.HOST
	port := filter.mongo.PORT
	dbname := filter.mongo.DBNAME
	user := filter.mongo.USER
	pass := filter.mongo.PASS
	quota_data_size := filter.config.QUOTA_DATA_SIZE

	var size float64
	for {
		event := <-filter.evtchn
		if event != 'd' {
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

		if size >= float64(quota_data_size*1024*1024) {
			atomic.StoreUint32(&filter.blocked, BLOCKED)
		} else {
			atomic.CompareAndSwapUint32(&filter.blocked, BLOCKED, UNBLOCKED)
		}

		continue
	Error:
		atomic.StoreUint32(&filter.blocked, BLOCKED)
	}

	endMongoSession()
}

func (filter *ProxyFilterImpl) MonitorQuotaFiles() {
	var fd, wd int
	var err error
	buffer := make([]byte, 256)
	dbfiles := make(map[string]int)

	dbname := filter.mongo.DBNAME
	base_dir := filter.config.BASE_DIR
	quota_files := filter.config.QUOTA_FILES

	filecount := 0
	filecount = iterateDatafile(dbname, base_dir, dbfiles)
	if filecount < 0 {
		logger.Error("Failed to iterate data files under %s.", base_dir)
		goto Error
	}

	logger.Info("At the begining time we have disk files: [%d].", filecount)
	if filecount > int(quota_files) {
		logger.Critical("Disk files exceeds quota.")
		atomic.StoreUint32(&filter.blocked, BLOCKED)
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
		nread, err := syscall.Read(fd, buffer)
		if nread < 0 {
			if err == syscall.EINTR {
				break
			} else {
				logger.Error("Failed to read inotify event: [%s].", err)
			}
		} else {
			err = parseInotifyEvent(dbname, buffer[0:nread], &filecount, dbfiles)
			if err != nil {
				logger.Error("Failed to parse inotify event.")
				atomic.StoreUint32(&filter.blocked, BLOCKED)
			} else {
				logger.Debug("Current db disk file number: [%d].", filecount)
				if filecount > int(quota_files) {
					logger.Critical("Disk files exceeds quota.")
					atomic.StoreUint32(&filter.blocked, BLOCKED)
				} else {
					atomic.CompareAndSwapUint32(&filter.blocked, BLOCKED, UNBLOCKED)
				}
			}
		}
	}

	syscall.InotifyRmWatch(fd, uint32(wd))
	syscall.Close(fd)
	return

Error:
	atomic.StoreUint32(&filter.blocked, BLOCKED)
}
