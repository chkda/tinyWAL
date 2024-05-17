package main

import (
	"fmt"
	"time"

	tinywal "github.com/chkda/tinyWAL"
)

func main() {
	config := &tinywal.Config{
		LogDir:         "tmp/",
		MaxSegments:    3,
		SegmentSize:    2 * 1024 * 1024, // 2MB
		SyncTimePeriod: 300 * time.Millisecond,
	}
	wal, err := tinywal.New(config)
	if err != nil {
		panic(err)
	}
	defer wal.Close()
	for i := 0; i < 1000; i++ {
		err := wal.Write([]byte("SET X 23"))
		if err != nil {
			panic(err)
		}

	}
	fmt.Println("Finished Writing")

	err = wal.Sync()
	if err != nil {
		panic(err)
	}
	count := 0
	err = wal.Recover(func(data []byte) error {
		// fmt.Println(string(data))
		count += 1
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("Total lines:", count)
}
