package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

type KeyValueStoreAPI struct {
	memtable *Memtable
	wal      *WAL
}

func NewKeyValueStoreAPI(memtable *Memtable, wal *WAL) *KeyValueStoreAPI {
	return &KeyValueStoreAPI{
		memtable: memtable,
		wal:      wal,
	}
}

func (api *KeyValueStoreAPI) GetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")

	if api.memtable.deletedKeys[key] != nil {
		w.Write([]byte(fmt.Sprintf("Key not found\n")))
		return
	}

	value := api.memtable.Get(key)
	if value != nil {
		w.Write([]byte(fmt.Sprintf("Value: %s\n", value)))
		return
	}

	api.searchForKeyInSSTFiles(key, w)
}

func (api *KeyValueStoreAPI) SetHandler(w http.ResponseWriter, r *http.Request) {
	var kvPair map[string]string
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&kvPair); err != nil {
		http.Error(w, "Invalid JSON format", http.StatusBadRequest)
		return
	}

	key, exists := kvPair["key"]
	if !exists {
		http.Error(w, "Key not provided in the request body", http.StatusBadRequest)
		return
	}

	value, exists := kvPair["value"]
	if !exists {
		http.Error(w, "Value not provided in the request body", http.StatusBadRequest)
		return
	}

	walEntry := &WALEntry{
		Action: 'S',
		Key:    []byte(fmt.Sprintf("%v", key)),
		Value:  []byte(fmt.Sprintf("%v", value)),
	}
	if err := api.wal.Write(walEntry); err != nil {
		log.Printf("Error writing to WAL: %v\n", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Update memtable
	api.memtable.Set(key, []byte(value))

	// Remove from deleted table if exists
	if api.memtable.IsDeleted(key) {
		delete(api.memtable.deletedKeys, key)
	}

	w.Write([]byte("OK\n"))
}

func (api *KeyValueStoreAPI) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")

	walEntry := &WALEntry{
		Action: 'D',
		Key:    []byte(fmt.Sprintf("%s", key)),
		Value:  []byte(fmt.Sprintf("%s", "")),
	}

	api.wal.Write(walEntry)

	// value :=
	api.memtable.Del(key)
	api.memtable.MarkDeleted(key)

	// if value == nil {
	// 	w.Write([]byte("Key not found\n"))
	// 	return
	// }

	// w.Write([]byte(fmt.Sprintf("Deleted Value: %s\n", value)))

	w.Write([]byte(fmt.Sprintf("Deletion Done.")))
}

func StartAPI(memtable *Memtable, wal *WAL) {
	api := NewKeyValueStoreAPI(memtable, wal)

	http.HandleFunc("/get", api.GetHandler)
	http.HandleFunc("/set", api.SetHandler)
	http.HandleFunc("/del", api.DeleteHandler)

	port := 8080
	fmt.Printf("Listening on port %d...\n", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

type SSTEntry struct {
	OpType byte
	Key    string
	Value  string
}

// Search for given key in sst files, my sst files are designed
// In a way that makes all set entries come before all del entries.
// So we basically iterate backwards through the sst files and
// Forward inside each sst file. We read entries following our
// Design, and if a key matches in a set entry, we store its value.
// If we encounter a del entry, we immediately return that there is
// No such key. This is guaranteed due to the way I implemented the
// Del functionality. Otherwise, if the key is not found in this file
// We look in the next one until we either find something (del or set)
// Or until we finish looking through all the files (key never existed)
func (api *KeyValueStoreAPI) searchForKeyInSSTFiles(key string, w http.ResponseWriter) {
	sstFiles, err := os.ReadDir("data/sst/")
	if err != nil {
		w.Write([]byte("Error reading SST files\n"))
		return
	}

	for i := len(sstFiles) - 1; i >= 0; i-- {
		found := false
		var value string

		sstFilePath := filepath.Join("data/sst/", sstFiles[i].Name())
		sstFile, err := os.Open(sstFilePath)
		if err != nil {
			log.Printf("Error opening SST file %s: %v\n", sstFilePath, err)
			continue
		}
		// fmt.Println("sstFilePath:", sstFilePath)

		reader := bufio.NewReader(sstFile)

		var magicNumber uint32
		if err := binary.Read(reader, binary.BigEndian, &magicNumber); err != nil {
			log.Printf("Error reading magic number from SST file %s: %v\n", sstFilePath, err)
			continue
		}
		// fmt.Println("magicNumber:", magicNumber)

		if magicNumber != 0x23102003 {
			log.Printf("Invalid SST file format in %s\n", sstFilePath)
			continue
		}

		var entryCount, smallestKeyLen, largestKeyLen uint32
		if err := binary.Read(reader, binary.BigEndian, &entryCount); err != nil {
			log.Printf("Error reading entry count from SST file %s: %v\n", sstFilePath, err)
			continue
		}
		// fmt.Println("entryCount:", entryCount)

		if err := binary.Read(reader, binary.BigEndian, &smallestKeyLen); err != nil {
			log.Printf("Error reading smallest key length from SST file %s: %v\n", sstFilePath, err)
			continue
		}
		// fmt.Println("smallestKeyLen:", smallestKeyLen)

		smallestKeyBytes := make([]byte, smallestKeyLen)
		// if _, err := reader.Read(smallestKeyBytes); err != nil {
		if err := binary.Read(reader, binary.BigEndian, smallestKeyBytes); err != nil {
			log.Printf("Error reading smallest key from SST file %s: %v\n", sstFilePath, err)
			continue
		}

		// smallestKey := string(smallestKeyBytes)
		// fmt.Println("smallestKey:", smallestKey)

		if err := binary.Read(reader, binary.BigEndian, &largestKeyLen); err != nil {
			log.Printf("Error reading largest key length from SST file %s: %v\n", sstFilePath, err)
			continue
		}
		// fmt.Println("largestKeyLen:", largestKeyLen)

		largestKeyBytes := make([]byte, largestKeyLen)
		// if _, err := reader.Read(largestKeyBytes); err != nil {
		if err := binary.Read(reader, binary.BigEndian, largestKeyBytes); err != nil {
			log.Printf("Error reading largest key from SST file %s: %v\n", sstFilePath, err)
			continue
		}

		// largestKey := string(largestKeyBytes)
		// fmt.Println("largestKey:", largestKey)

		var version uint16
		if err := binary.Read(reader, binary.BigEndian, &version); err != nil {
			log.Printf("Error reading key length from SST file %s: %v\n", sstFilePath, err)
			break
		}
		// fmt.Println("version:", version)

		// Check if the target key falls within the range defined by the smallest and largest keys.
		if len(key) >= int(smallestKeyLen) && len(key) <= int(largestKeyLen) {
			// Perform a linear search within the SST file for the target key.
			for j := 0; j < int(entryCount); j++ {
				var opType byte
				if err := binary.Read(reader, binary.BigEndian, &opType); err != nil {
					log.Printf("Error reading key length from SST file %s: %v\n", sstFilePath, err)
					break
				}
				// fmt.Println("opType:", opType)

				if opType == 'S' {
					var keyLen, valueLen uint32
					if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
						log.Printf("Error reading key length from SST file %s: %v\n", sstFilePath, err)
						break
					}
					// fmt.Println("keyLen:", keyLen)

					keyBytes := make([]byte, keyLen)
					// if _, err := reader.Read(keyBytes); err != nil {
					if err := binary.Read(reader, binary.BigEndian, keyBytes); err != nil {
						log.Printf("Error reading key from SST file %s: %v\n", sstFilePath, err)
						break
					}

					currentKey := string(keyBytes)
					// fmt.Println("currentKey:", currentKey)

					if err := binary.Read(reader, binary.BigEndian, &valueLen); err != nil {
						log.Printf("Error reading value length from SST file %s: %v\n", sstFilePath, err)
						break
					}
					// fmt.Println("valueLen:", valueLen)

					if currentKey == key {
						found = true
						valueBytes := make([]byte, valueLen)
						// if _, err := reader.Read(valueBytes); err != nil {
						if err := binary.Read(reader, binary.BigEndian, valueBytes); err != nil {
							log.Printf("Error reading value from SST file %s: %v\n", sstFilePath, err)
							break
						}

						value = string(valueBytes)
						fmt.Println("value:", value)
					} else {
						notValueBytes := make([]byte, valueLen)
						// if _, err := reader.Read(notValueBytes); err != nil {
						if err := binary.Read(reader, binary.BigEndian, notValueBytes); err != nil {
							log.Printf("Error reading value from SST file %s: %v\n", sstFilePath, err)
							break
						}

						// note to self:
						// comment the 2 lines below when you finish testing
						// notValue := string(notValueBytes)
						// fmt.Println("notValue:", notValue)
					}
				} else if opType == 'D' {
					var keyLen uint32
					if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
						log.Printf("Error reading key length from SST file %s: %v\n", sstFilePath, err)
						break
					}
					// fmt.Println("keyLen:", keyLen)

					keyBytes := make([]byte, keyLen)
					// if _, err := reader.Read(keyBytes); err != nil {
					if err := binary.Read(reader, binary.BigEndian, keyBytes); err != nil {
						log.Printf("Error reading key from SST file %s: %v\n", sstFilePath, err)
						break
					}

					currentKey := string(keyBytes)
					// fmt.Println("currentKey:", currentKey)

					if currentKey == key {
						w.Write([]byte("Key is deleted\n"))
						return
					}
				}
			}
		}

		sstFile.Close()

		if found {
			w.Write([]byte(fmt.Sprintf("Value: %s\n", value)))
			return
		}
	}

	// If the key is still not found after searching all SST files, respond accordingly
	w.Write([]byte("Key not found\n"))
}
