package proxy

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var dbdir string
var dbname string

func setupDatafile() {
	dbdir = "/tmp/unittest/"
	dbname = "db"

	os.MkdirAll(filepath.Dir(dbdir), 0755)
	file1, _ := os.Create(filepath.Join(dbdir, fmt.Sprintf("%s.%d", dbname, 0)))
	file2, _ := os.Create(filepath.Join(dbdir, fmt.Sprintf("%s.%d", dbname, 1)))
	file1.Close()
	file2.Close()
}

func cleanDatafile() {
	os.RemoveAll(filepath.Dir(dbdir))
	logger = nil
}

func TestIterateDatafile(t *testing.T) {
	setupDatafile()

	defer cleanDatafile()

	dbfiles := make(map[string]int)
	filecount := iterateDatafile(dbname, dbdir, dbfiles)
	if filecount < 2 {
		t.Errorf("Failed to iterate data files.\n")
	}
	if _, ok := dbfiles["db.0"]; !ok {
		t.Errorf("Failed to get db.0 file.\n")
	}
	if _, ok := dbfiles["db.1"]; !ok {
		t.Errorf("Failed to get db.1 file.\n")
	}
	fmt.Printf("Succeed to iterate all db files.\n")
}
