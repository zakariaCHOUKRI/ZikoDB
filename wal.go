package main

import (
	"encoding/binary"
	"log"
	"os"
	"sync"
)

type WALEntry struct {
	Action byte
	Key    []byte
	Value  []byte
}

type WAL struct {
	file       *os.File
	writeMutex sync.Mutex
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

func (w *WAL) Write(entry *WALEntry) error {
	// w.writeMutex.Lock()
	// defer w.writeMutex.Unlock()

	// Write entry to the end of the WAL file
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

func (w *WAL) Close() error {
	return w.file.Close()
}
