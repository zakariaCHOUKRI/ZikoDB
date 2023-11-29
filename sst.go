package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	magicNumber = uint32(0x23102003)
	version     = uint16(1)
	threshold   = 20000
	interval    = time.Second * 10
)

type SSTFile struct {
	file           *os.File
	entryCount     uint32
	smallestKeyLen uint32
	smallestKey    []byte
	largestKeyLen  uint32
	largestKey     []byte
	version        uint16
	checksum       uint32
	writeMutex     sync.Mutex
}

func NewSSTFile(filename string) (*SSTFile, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &SSTFile{
		file:    file,
		version: version,
	}, nil
}

func flush(memtable *Memtable) {

	if memtable.data.Len() > 0 || memtable.deletedKeys.Len() > 0 {

		timestamp := time.Now().Format("20060102150405.000000000")

		newSSTFile, err := NewSSTFile(fmt.Sprintf("data/sst/%s.sst", timestamp))
		if err != nil {
			fmt.Println("Error creating new SST file:", err)
			return
		}

		if err := newSSTFile.Write(memtable); err != nil {
			fmt.Println("Error flushing memtable to new SST file:", err)
		}

		memtable.Clear()
	}
}

func periodicFlush(memtable *Memtable) {
	for {
		select {
		case <-time.After(interval):
			flush(memtable)
		}
	}
}

func (s *SSTFile) Write(memtable *Memtable) error {
	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()

	if it := memtable.data.Front(); it != nil && memtable.data.Len() > 0 {
		s.smallestKey = it.Key().([]byte)
		s.largestKey = it.Key().([]byte)
		keySize := uint32(len(fmt.Sprintf("%s", s.smallestKey)))
		s.smallestKeyLen = keySize
		s.largestKeyLen = keySize
	} else {
		// handle this if and only if we change the deletion implementation
	}

	for it := memtable.data.Front(); it != nil; it = it.Next() {
		currentKey := it.Key().([]byte)
		keySize := uint32(len(fmt.Sprintf("%s", currentKey)))

		if keySize < s.smallestKeyLen {
			s.smallestKey = currentKey
			s.smallestKeyLen = keySize
		}
		if keySize > s.largestKeyLen {
			s.largestKey = currentKey
			s.largestKeyLen = keySize
		}
	}

	for it := memtable.deletedKeys.Front(); it != nil; it = it.Next() {
		currentKey := it.Key().([]byte)
		keySize := uint32(len(fmt.Sprintf("%s", currentKey)))

		if keySize < s.smallestKeyLen {
			s.smallestKey = currentKey
			s.smallestKeyLen = keySize
		}
		if keySize > s.largestKeyLen {
			s.largestKey = currentKey
			s.largestKeyLen = keySize
		}
	}

	s.entryCount = uint32(memtable.data.Len()) + uint32(memtable.deletedKeys.Len())

	if err := binary.Write(s.file, binary.BigEndian, uint32(magicNumber)); err != nil {
		return err
	}
	if err := binary.Write(s.file, binary.BigEndian, uint32(s.entryCount)); err != nil {
		return err
	}
	if err := binary.Write(s.file, binary.BigEndian, uint32(s.smallestKeyLen)); err != nil {
		return err
	}
	if err := binary.Write(s.file, binary.BigEndian, s.smallestKey); err != nil {
		return err
	}
	if err := binary.Write(s.file, binary.BigEndian, uint32(s.largestKeyLen)); err != nil {
		return err
	}
	if err := binary.Write(s.file, binary.BigEndian, s.largestKey); err != nil {
		return err
	}
	if err := binary.Write(s.file, binary.BigEndian, uint16(s.version)); err != nil {
		return err
	}

	for it := memtable.data.Front(); it != nil; it = it.Next() {
		keyBytes := []byte(fmt.Sprintf("%s", it.Key()))
		valueBytes := []byte(fmt.Sprintf("%s", it.Value))

		keySize := uint32(len(keyBytes))
		valueSize := uint32(len(valueBytes))

		if err := binary.Write(s.file, binary.BigEndian, []byte("S")); err != nil {
			return err
		}
		if err := binary.Write(s.file, binary.BigEndian, uint32(keySize)); err != nil {
			return err
		}
		if err := binary.Write(s.file, binary.BigEndian, keyBytes); err != nil {
			return err
		}
		if err := binary.Write(s.file, binary.BigEndian, uint32(valueSize)); err != nil {
			return err
		}
		if err := binary.Write(s.file, binary.BigEndian, valueBytes); err != nil {
			return err
		}

		fmt.Println("written key", string(keyBytes))
	}

	for it := memtable.deletedKeys.Front(); it != nil; it = it.Next() {
		keyBytes := []byte(fmt.Sprintf("%s", it.Key()))
		keySize := uint32(len(keyBytes))

		if err := binary.Write(s.file, binary.BigEndian, []byte("D")); err != nil {
			return err
		}
		if err := binary.Write(s.file, binary.BigEndian, uint32(keySize)); err != nil {
			return err
		}
		if err := binary.Write(s.file, binary.BigEndian, keyBytes); err != nil {
			return err
		}

		fmt.Println("deleted key", string(keyBytes))
	}

	existingData, err := s.fileBytesForChecksum()
	if err != nil {
		return err
	}

	// Calculate CRC32 checksum of the existing data
	checksum := crc32.ChecksumIEEE(existingData)

	// Include the checksum in the file
	if err := binary.Write(s.file, binary.BigEndian, checksum); err != nil {
		return err
	}

	return nil
}

func (s *SSTFile) fileBytesForChecksum() ([]byte, error) {
	// Open the SST file for reading
	file, err := os.Open(s.file.Name())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read the existing data from the file
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func integrityCheck() {
	sstFiles, err := os.ReadDir("data/sst/")
	if err != nil {
		log.Fatalf("Error reading SST files directory: %v", err)
	}

	fmt.Println("Integrity Check Using Checksums:")
	for _, sstFile := range sstFiles {
		sstFilePath := filepath.Join("data/sst/", sstFile.Name())
		sst, err := os.Open(sstFilePath)
		if err != nil {
			log.Printf("Error opening SST file %s: %v\n", sstFilePath, err)
			continue
		}

		checksum, err := calculateChecksum(sst)
		if err != nil {
			log.Printf("Error calculating checksum for SST file %s: %v\n", sstFilePath, err)
			continue
		}

		storedChecksum, err := readStoredChecksum(sst)
		if err != nil {
			log.Printf("Error reading stored checksum for SST file %s: %v\n", sstFilePath, err)
			continue
		}

		result := "Fail"
		if checksum == storedChecksum {
			result = "Ok"
		}

		fmt.Printf("SST file: %s - %s\n", sstFile.Name(), result)

		sst.Close()
	}
}

func calculateChecksum(file *os.File) (uint32, error) {
	data, err := io.ReadAll(file)
	if err != nil {
		return 0, err
	}

	// Exclude the last 4 bytes (checksum) when calculating checksum
	dataWithoutChecksum := data[:len(data)-4]

	return crc32.ChecksumIEEE(dataWithoutChecksum), nil
}

func readStoredChecksum(file *os.File) (uint32, error) {
	// Seek to the position where checksum is stored (end of file - 4 bytes)
	_, err := file.Seek(-4, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	var storedChecksum uint32
	err = binary.Read(file, binary.BigEndian, &storedChecksum)
	if err != nil {
		return 0, err
	}

	return storedChecksum, nil
}

func (s *SSTFile) Close() error {
	return s.file.Close()
}
