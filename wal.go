package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

type WALEntry struct {
	Action byte
	Key    []byte
	Value  []byte
}

type WAL struct {
	file *os.File
}

func NewWAL(filename string) (*WAL, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Error opening WAL file %s: %v\n", filename, err)
		return nil, err
	}

	return &WAL{
		file: file,
	}, nil
}

// Write data to the wal file
func (w *WAL) Write(entry *WALEntry) error {

	if err := binary.Write(w.file, binary.BigEndian, entry.Action); err != nil {
		log.Printf("Error writing WAL entry: %v\n", err)
		return err
	}

	if err := binary.Write(w.file, binary.BigEndian, uint32(len(entry.Key))); err != nil {
		log.Printf("Error writing WAL entry: %v\n", err)
		return err
	}
	if err := binary.Write(w.file, binary.BigEndian, entry.Key); err != nil {
		log.Printf("Error writing WAL entry: %v\n", err)
		return err
	}

	if err := binary.Write(w.file, binary.BigEndian, uint32(len(entry.Value))); err != nil {
		log.Printf("Error writing WAL entry: %v\n", err)
		return err
	}
	if err := binary.Write(w.file, binary.BigEndian, entry.Value); err != nil {
		log.Printf("Error writing WAL entry: %v\n", err)
		return err
	}

	return nil
}

// Read the entries from the wal file
func ReadWAL(filename string) ([]WALEntry, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []WALEntry

	for {
		var op byte
		if err := binary.Read(file, binary.BigEndian, &op); err != nil {
			break // End of file
		}

		var keyLength uint32
		binary.Read(file, binary.BigEndian, &keyLength)

		key := make([]byte, keyLength)
		binary.Read(file, binary.BigEndian, key)

		var valueLength uint32
		binary.Read(file, binary.BigEndian, &valueLength)

		value := make([]byte, valueLength)
		binary.Read(file, binary.BigEndian, value)

		entry := WALEntry{
			Action: op,
			Key:    key,
			Value:  value,
		}
		entries = append(entries, entry)
	}

	clearWAL(filename)

	return entries, nil
}

// Flush the wal into memory and then into disk
func (wal *WAL) flushWAL(memtable *Memtable) {
	entries, err := ReadWAL(wal.file.Name())
	if err != nil {
		fmt.Println("Error reading WAL:", err)
		return
	}

	if len(entries) == 0 {
		fmt.Println("WAL is empty.")
	} else {
		fmt.Println("Reconstructing WAL entries...")
		for _, entry := range entries {
			if entry.Action == 'S' {
				memtable.Set(string(entry.Key), entry.Value)

				// Remove from deleted table if exists
				if memtable.IsDeleted(string(entry.Key)) {
					delete(memtable.deletedKeys, string(entry.Key))
				}
			} else if entry.Action == 'D' {
				memtable.Del(string(entry.Key))
				memtable.MarkDeleted(string(entry.Key))
			}
		}
		flush(memtable)
	}
}

func clearWAL(filename string) error {
	err := os.Truncate(filename, 0)
	return err
}

func (w *WAL) Close() error {
	return w.file.Close()
}
