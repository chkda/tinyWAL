package tinywal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	filePrefix = "segment-"
)

var (
	ErrBytesLength        = errors.New("line less than expected")
	ErrChecksumValidation = errors.New("checksum mismatch")
)

type Config struct {
	LogDir         string
	SegmentSize    int64
	MaxSegments    int
	SyncTimePeriod time.Duration
}

type WAL struct {
	logDir         string
	currentLog     *os.File
	bufWriter      *bufio.Writer
	maxSegments    int
	segmentSize    int64
	lock           sync.Mutex
	syncTimeTicker *time.Ticker
	currentOffset  int64
}

type segmentInfo struct {
	Name      string
	Timestamp int64
}

func New(config *Config) (*WAL, error) {
	err := os.Mkdir(config.LogDir, 0755)
	if err != nil {
		return nil, err
	}
	wal := &WAL{
		logDir:         config.LogDir,
		maxSegments:    config.MaxSegments,
		segmentSize:    config.SegmentSize,
		syncTimeTicker: time.NewTicker(config.SyncTimePeriod),
	}
	err = wal.createNewLogFile()
	if err != nil {
		return nil, err
	}
	go wal.syncInBackground()
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
	w.bufWriter = bufio.NewWriter(file)
	w.currentOffset = 0
	return nil
}

func (w *WAL) Write(data []byte) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	err := w.rotateLogIfSizeExceeds()
	if err != nil {
		return err
	}
	_, err = w.currentLog.Seek(0, io.SeekStart)
	offset := w.currentOffset
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

	_, err = w.bufWriter.Write(offsetBytes)
	if err != nil {
		return err
	}

	_, err = w.bufWriter.Write(lenBytes)
	if err != nil {
		return err
	}

	_, err = w.bufWriter.Write(checksumBytes)
	if err != nil {
		return err
	}

	_, err = w.bufWriter.Write(data)
	if err != nil {
		return err
	}

	if _, err := w.bufWriter.Write([]byte("\n")); err != nil {
		return err
	}
	w.currentOffset += 1
	return nil
}

func (w *WAL) rotateLogIfSizeExceeds() error {
	files, err := w.getAllSegments()
	if err != nil {
		return err
	}
	err = w.processOldSegments(files)
	if err != nil {
		return err
	}
	fileInfo, err := w.currentLog.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()
	if fileSize > w.segmentSize {
		w.Sync()
		w.createNewLogFile()
	}
	return nil
}

func (w *WAL) syncInBackground() {
	for {
		select {
		case <-w.syncTimeTicker.C:
			w.lock.Lock()
			err := w.Sync()
			w.lock.Unlock()
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func (w *WAL) Sync() error {
	return w.bufWriter.Flush()
}

func (w *WAL) Close() error {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.Sync()
}

func (w *WAL) getAllSegments() ([]string, error) {
	entries, err := os.ReadDir(w.logDir)
	if err != nil {
		return nil, err
	}
	fileNames := make([]string, 0, 5)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fileNames = append(fileNames, entry.Name())
	}

	return fileNames, nil
}

func (w *WAL) processOldSegments(segments []string) error {
	if len(segments) < w.maxSegments {
		return nil
	}
	err := w.deleteOldSegments(segments)
	if err != nil {
		return err
	}
	return nil
}

func (w *WAL) deleteOldSegments(segments []string) error {
	segmentsWithInfo, err := w.getSegmentInfos(segments)
	if err != nil {
		return err
	}
	sort.SliceStable(segmentsWithInfo, func(i, j int) bool {
		return segmentsWithInfo[i].Timestamp < segmentsWithInfo[j].Timestamp
	})
	count := len(segments)
	for _, segment := range segmentsWithInfo {
		if count <= w.maxSegments {
			break
		}
		err := os.Remove(w.logDir + "/" + segment.Name)
		if err != nil {
			return err
		}
		count -= 1
	}
	return nil
}

func (w *WAL) getSegmentInfos(segments []string) ([]*segmentInfo, error) {
	segmentsWithInfo := make([]*segmentInfo, 0, 5)
	for _, segment := range segments {
		if !strings.HasPrefix(segment, filePrefix) {
			continue
		}
		timestampStr := strings.ReplaceAll(segment, filePrefix, "")
		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			return nil, err
		}
		segmentsWithInfo = append(segmentsWithInfo, &segmentInfo{
			Name:      segment,
			Timestamp: timestamp,
		})
	}
	return segmentsWithInfo, nil
}

func (w *WAL) Recover(callback func([]byte) error) error {
	segments, err := w.getAllSegments()
	if err != nil {
		return err
	}
	segmentsWithInfo, err := w.getSegmentInfos(segments)
	if err != nil {
		return err
	}
	sort.SliceStable(segmentsWithInfo, func(i, j int) bool {
		return segmentsWithInfo[i].Timestamp < segmentsWithInfo[j].Timestamp
	})
	for _, segmentWithInfo := range segmentsWithInfo {
		segmentPath := w.logDir + "/" + segmentWithInfo.Name
		err = w.recoverSegment(segmentPath, callback)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) recoverSegment(segmentPath string, callback func([]byte) error) error {
	segment, err := os.Open(segmentPath)
	if err != nil {
		return err
	}
	defer segment.Close()
	scanner := bufio.NewScanner(segment)
	for scanner.Scan() {
		info := scanner.Bytes()
		if len(info) < 16 {
			log.Println(ErrBytesLength)
			continue
		}

		checksumBytes := info[12:16]
		data := info[16:]

		precomputedChecksum := binary.LittleEndian.Uint32(checksumBytes)
		calculatedChecksum := crc32.ChecksumIEEE(data)

		if precomputedChecksum != calculatedChecksum {
			log.Println(ErrChecksumValidation)
			continue
		}
		err = callback(data)
		if err != nil {
			break
		}
	}
	return nil
}
