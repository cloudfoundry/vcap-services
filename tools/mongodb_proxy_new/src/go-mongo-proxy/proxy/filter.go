package proxy

import (
	"sync"
	"sync/atomic"
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
	StartStorageMonitor()
	WaitForFinish()
}

type ProxyFilterImpl struct {
	// atomic value, use atomic wrapper function to operate on it
	blocked uint32 // 0 means not block, 1 means block

	// event channel
	evtchn chan byte // 'd' means dirty event, 's' means shutdown event

	config *FilterConfig
	mongo  *ConnectionInfo

	// goroutine wait channel
	lock    sync.Mutex
	running uint32
	wait    chan byte
}

func NewFilter(conf *FilterConfig, conn *ConnectionInfo) *ProxyFilterImpl {
	return &ProxyFilterImpl{
		blocked: UNBLOCKED,
		evtchn:  make(chan byte, 100),
		config:  conf,
		mongo:   conn,
		running: 0,
		wait:    make(chan byte, 1)}
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

func (filter *ProxyFilterImpl) StartStorageMonitor() {
	go filter.MonitorQuotaDataSize()
}

func (filter *ProxyFilterImpl) WaitForFinish() {
	if filter.config.ENABLED {
		filter.evtchn <- 's'
		<-filter.wait
	}
}

func (filter *ProxyFilterImpl) MonitorQuotaDataSize() {
	dbhost := filter.mongo.HOST
	port := filter.mongo.PORT
	dbname := filter.mongo.DBNAME
	user := filter.mongo.USER
	pass := filter.mongo.PASS
	quota_data_size := filter.config.QUOTA_DATA_SIZE

	base_dir := filter.config.BASE_DIR
	quota_files := filter.config.QUOTA_FILES

	filter.lock.Lock()
	filter.running++
	filter.lock.Unlock()

	dbfiles := make(map[string]int)
	upperbound := float64(quota_data_size) * float64(1024*1024)

	var size float64
	pfilecount := 0
	nfilecount := 0
	for {
		event := <-filter.evtchn
		if event == 's' {
			break
		}

		nfilecount = iterateDatafile(dbname, base_dir, dbfiles)
		if nfilecount < 0 {
			logger.Error("Failed to iterate data files under %s.", base_dir)
			goto Error
		}

		if err := startMongoSession(dbhost, port); err != nil {
			logger.Error("Failed to connect to %s:%s, [%s].", dbhost, port, err)
			goto Error
		}

		if !readMongodbSize(dbname, user, pass, &size) {
			logger.Error("Failed to read database '%s' size.", dbname)
			goto Error
		}

		// disk file last allocation meets following 2 conditions
		// 1. nfilecount > quota file number
		// 2. nfilecount > pfilecount
		if (nfilecount > int(quota_files)) && (nfilecount > pfilecount) {
			logger.Critical("Last allocation for a new disk file, quota exceeds.")
			upperbound = size
		} else if nfilecount < pfilecount {
			// Only 'repair' can shrink disk files.
			logger.Info("Repair database is triggered.")
			upperbound = float64(action.quota_data_size) * float64(1024*1024)
		}

		if size >= upperbound {
			atomic.StoreUint32(&filter.blocked, BLOCKED)
		} else {
			atomic.CompareAndSwapUint32(&filter.blocked, BLOCKED, UNBLOCKED)
		}

		pfilecount = nfilecount
		continue
	Error:
		atomic.StoreUint32(&filter.blocked, BLOCKED)
	}

	endMongoSession()

	filter.lock.Lock()
	filter.running--
	if filter.running == 0 {
		filter.wait <- 's'
	}
	filter.lock.Unlock()
}
