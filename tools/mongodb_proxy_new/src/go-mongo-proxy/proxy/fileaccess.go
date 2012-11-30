package proxy

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
)

// If -1 returns then it means something wrong.
func iterateDatafile(dbname string, dirpath string, dbfiles map[string]int) int {
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

// If not nil returns then it means something wrong.
func parseInotifyEvent(dbname string, buffer []byte, filecount *int, dbfiles map[string]int) error {
	var event syscall.InotifyEvent
	var filename string

	expr := "^" + dbname + "\\.[0-9]+"
	re, err := regexp.Compile(expr)
	if err != nil {
		logger.Error("Failed to compile regexp error: [%s].", err)
		return err
	}

	index := 0
	for index < len(buffer) {
		err := binary.Read(bytes.NewBuffer(buffer[0:len(buffer)]), binary.LittleEndian, &event)
		if err != nil {
			logger.Error("Failed to do binary read inotify event: [%s].", err)
			return err
		}

		start := index + syscall.SizeofInotifyEvent
		end := start + int(event.Len)

		// Trim the tailing 'null' byte
		filename = strings.Trim(string(buffer[start:end]), string(0x0))
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
