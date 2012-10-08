package logger

import (
	"fmt"
	"log"
	"os"
	"runtime"
)

const (
	ERR   = 1
	WARN  = 2
	DEBUG = 3
	INFO  = 4
)

type Logger struct {
	ch       chan string
	file     *os.File
	fileName string
}

var obj *Logger

func Init(logFile string) error {
	if obj != nil {
		return nil
	}
	obj = new(Logger)
	if logFile != "" {
		outFile, err := os.OpenFile(logFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			return err
		}
		obj.file = outFile
	}
	if obj.file == nil {
		obj.file = os.Stderr
	}
	obj.fileName = logFile
	obj.ch = make(chan string, 100)
	log.SetOutput(obj.file)
	go Start()
	return nil
}

func Start() {
	for {
		msg := <-obj.ch
		if len(msg) == 0 {
			runtime.Goexit()
		}
		log.Println(msg)
	}
}

func Instance() *Logger {
	return obj
}

var LogLevelMap = map[int8]string{
	ERR:   "ERROR",
	WARN:  "WARN",
	DEBUG: "DEBUG",
	INFO:  "INFO",
}

func Log(level int8, fmtStr string, a ...interface{}) (err error) {
	msg := fmt.Sprintf("[%s] %s", LogLevelMap[level], fmt.Sprintf(fmtStr, a...))
	obj.ch <- msg
	return nil
}

func Finalize() {
	if obj != nil {
		obj.ch <- ""
		if obj.fileName != "" {
			obj.file.Close()
		}
	}
}
