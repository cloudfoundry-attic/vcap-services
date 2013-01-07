package proxy

import (
	"fmt"
	"testing"
)

func TestReadMongodbSize(t *testing.T) {
	logger = setupStdoutLogger()

	dbhost := "127.0.0.1"
	port := "27017"
	dbname := "db"
	user := "admin"
	pass := "123456"

	var size float64

	defer endMongoSession()

	err := startMongoSession(dbhost, port)
	if err != nil {
		t.Errorf("Failed to start mongodb session, [%s].\n", err)
		return
	}

	size = 0
	if !readMongodbSize(dbname, user, pass, &size) {
		t.Errorf("Failed to read mongodb size from stats.\n")
		return
	} else {
		fmt.Printf("Get mongodb size %f from stats.\n", size)
	}

	logger = nil
}
