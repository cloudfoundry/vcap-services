package proxy

import (
	"os"
	"path/filepath"
	"regexp"
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
