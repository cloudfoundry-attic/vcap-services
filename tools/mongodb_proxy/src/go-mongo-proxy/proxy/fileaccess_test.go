package proxy

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
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
}

func fileCreator() {
	time.Sleep(1 * time.Second)

	// open a exsting file
	file, _ := os.Open(filepath.Join(dbdir, fmt.Sprintf("%s.%d", dbname, 1)))
	file.Close()

	time.Sleep(1 * time.Second)

	// create a new file
	file, _ = os.Create(filepath.Join(dbdir, fmt.Sprintf("%s.%d", dbname, 2)))
	file.Close()

	time.Sleep(1 * time.Second)

	// delete a file
	os.Remove(filepath.Join(dbdir, "db.2"))

	time.Sleep(1 * time.Second)

	// send quit signal
	file, _ = os.Create(filepath.Join(dbdir, fmt.Sprintf("%s.%d", dbname, 10)))
	file.Close()
}

func TestIterateDatafile(t *testing.T) {
	logger = setupStdoutLogger()

	setupDatafile()

	defer cleanDatafile()

	dbfiles := make(map[string]int)
	filecount := iterateDatafile(dbname, dbdir, dbfiles)
	if filecount < 2 {
		t.Error("Failed to iterate data files.\n")
	}
	if _, ok := dbfiles["db.0"]; !ok {
		t.Error("Failed to get db.0 file.\n")
	}
	if _, ok := dbfiles["db.1"]; !ok {
		t.Error("Failed to get db.1 file.\n")
	}
	fmt.Printf("Succeed to iterate all db files.\n")
}

func TestParseInotifyEvent(t *testing.T) {
	logger = setupStdoutLogger()

	setupDatafile()

	defer cleanDatafile()

	dbfiles := make(map[string]int)
	filecount := iterateDatafile(dbname, dbdir, dbfiles)
	if filecount < 0 {
		t.Error("Failed to parse inotify event.\n")
	}

	fd, err := syscall.InotifyInit()
	if err != nil {
		t.Error("Failed to call InotifyInit: [%s].\n", err)
		return
	}

	wd, err := syscall.InotifyAddWatch(fd, dbdir, syscall.IN_CREATE|syscall.IN_OPEN|
		syscall.IN_MOVED_TO|syscall.IN_DELETE)
	if err != nil {
		t.Error("Failed to call InotifyAddWatch: [%s].\n", err)
		syscall.Close(fd)
		return
	}

	go fileCreator()

	buffer := make([]byte, 256)
	for {
		nread, err := syscall.Read(fd, buffer)
		if nread < 0 {
			t.Error("Failed to read inotify event: [%s].\n", err)
		} else {
			err = parseInotifyEvent(dbname, buffer[0:nread], &filecount, dbfiles)
			if err != nil {
				t.Error("Failed to parse inotify event.\n")
			} else {
				fmt.Printf("Current dbfiles are, %v.\n", dbfiles)
				if _, ok := dbfiles["db.10"]; ok {
					break
				}
			}
		}
	}

	syscall.InotifyRmWatch(fd, uint32(wd))
	syscall.Close(fd)

	if filecount < 3 {
		t.Error("Failed to parse inotify event.\n")
	}
	if _, ok := dbfiles["db.0"]; !ok {
		t.Error("Failed to get db.0 file.\n")
	}
	if _, ok := dbfiles["db.1"]; !ok {
		t.Error("Failed to get db.1 file.\n")
	}
	fmt.Printf("Succeed to parse all inotify events.\n")
}
