package tinywal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
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
	SegmentSize int64
	UseFSync    bool
	MaxSegments int
	BatchSize   int
}

type WAL struct {
	logDir      string
	currentLog  *os.File
	enableFSync bool
	maxSegments int
	segmentSize int64
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

func (w *WAL) Write(data []byte) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	err := w.rotateLogIfSizeExceeds()
	if err != nil {
		return err
	}
	offset, err := w.currentLog.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	checksum := crc32.ChecksumIEEE(data)

	checksumBytes := make([]byte, 4)
	lenBytes := make([]byte, 4)
	offsetBytes := make([]byte, 8)

	binary.LittleEndian.PutUint64(offsetBytes, uint64(offset))
	binary.LittleEndian.PutUint32(lenBytes, uint32(len(data)))
	binary.LittleEndian.PutUint32(checksumBytes, checksum)

	_, err = w.currentLog.Write(offsetBytes)
	if err != nil {
		return err
	}

	_, err = w.currentLog.Write(lenBytes)
	if err != nil {
		return err
	}

	_, err = w.currentLog.Write(data)
	if err != nil {
		return err
	}

	_, err = w.currentLog.Write(checksumBytes)
	if err != nil {
		return err
	}
	return nil
}

func (w *WAL) rotateLogIfSizeExceeds() error {
	fileInfo, err := w.currentLog.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()
	if fileSize > w.segmentSize {
		w.createNewLogFile()
	}
	return nil
}
