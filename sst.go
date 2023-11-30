package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

const (
	magicNumber = uint32(0x23102003)
	version     = uint16(1)
	threshold   = 500
	interval    = time.Second * 60
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

// Flush the contents of memtable to disk
func flush(memtable *Memtable) {

	if len(memtable.data) > 0 || len(memtable.deletedKeys) > 0 {

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

// Periodically flush memtable to disk
func periodicFlush(memtable *Memtable) {
	for {
		select {
		case <-time.After(interval):
			flush(memtable)
		}
	}
}

// Write the contents of the sst file to disk
func (s *SSTFile) Write(memtable *Memtable) error {

	// Get the max and min of key lengths in order to easily
	// Determine whether a key is present in a sst file just
	// By using its length
	for k := range memtable.data {
		s.smallestKey = []byte(k)
		s.largestKey = []byte(k)

		keySize := uint32(len(k))
		s.smallestKeyLen = keySize
		s.largestKeyLen = keySize

		break
	}

	for k := range memtable.data {
		keySize := uint32(len(k))

		if keySize <= s.smallestKeyLen {
			s.smallestKey = []byte(k)
			s.smallestKeyLen = keySize
		}
		if keySize >= s.largestKeyLen {
			s.largestKey = []byte(k)
			s.largestKeyLen = keySize
		}
	}

	for k := range memtable.deletedKeys {
		keySize := uint32(len(k))

		if keySize <= s.smallestKeyLen {
			s.smallestKey = []byte(k)
			s.smallestKeyLen = keySize
		}
		if keySize >= s.largestKeyLen {
			s.largestKey = []byte(k)
			s.largestKeyLen = keySize
		}
	}

	// Get header elements and write them to sst file
	s.entryCount = uint32(len(memtable.data) + len(memtable.deletedKeys))

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

	// Write set entries to sst file
	for k, v := range memtable.data {
		keyBytes := []byte(k)
		valueBytes := []byte(v)

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

		// fmt.Println("written key", string(keyBytes))
	}

	// Write del entries to sst file
	for k := range memtable.deletedKeys {
		keyBytes := []byte(k)

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

		// fmt.Println("deleted key", string(keyBytes))
	}

	existingData, err := s.fileBytesForChecksum()
	if err != nil {
		return err
	}

	// Calculate CRC32 checksum of the existing data
	// And include it in the file
	checksum := crc32.ChecksumIEEE(existingData)
	if err := binary.Write(s.file, binary.BigEndian, checksum); err != nil {
		return err
	}

	clearWAL("data/wal/wal")

	fmt.Println("Data flushed to ", s.file.Name())
	return nil
}

// Get the file bytes up until where the checksum should be
func (s *SSTFile) fileBytesForChecksum() ([]byte, error) {
	file, err := os.Open(s.file.Name())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Iterate through all sst files and check
// If they are valid using their checksums
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

	dataWithoutChecksum := data[:len(data)-4]

	return crc32.ChecksumIEEE(dataWithoutChecksum), nil
}

func readStoredChecksum(file *os.File) (uint32, error) {
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
