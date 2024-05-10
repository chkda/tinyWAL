package tinywal

import (
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	filePrefix = "segment-"
)

type Config struct {
	LogDir      string
	SegmentSize int
	UseFSync    bool
	MaxSegments int
	BatchSize   int
}

type WAL struct {
	logDir      string
	currentLog  *os.File
	enableFSync bool
	maxSegments int
	segmentSize int
	batchSize   int
	lock        sync.Mutex
}

func New(config *Config) (*WAL, error) {
	err := os.Mkdir(config.LogDir, 0755)
	if err != nil {
		return nil, err
	}
	wal := &WAL{
		logDir:      config.LogDir,
		enableFSync: config.UseFSync,
		maxSegments: config.MaxSegments,
		segmentSize: config.SegmentSize,
		batchSize:   config.BatchSize,
	}
	err = wal.createNewLogFile()
	if err != nil {
		return nil, err
	}
	return wal, nil
}

func (w *WAL) createNewLogFile() error {
	timestamp := time.Now().Unix()
	filePath := w.logDir + "/" + filePrefix + "-" + strconv.FormatInt(timestamp, 10)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	w.currentLog = file
	return nil
}
