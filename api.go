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

	value := api.memtable.Get([]byte(key))
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

	// Write to WAL
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
	api.memtable.Set([]byte(key), []byte(value))

	// Remove from deleted table if exists
	if api.memtable.IsDeleted([]byte(key)) {
		api.memtable.deletedKeys.Remove([]byte(key))
	}

	if api.memtable.data.Len() >= threshold {
		flush(api.memtable)
	}

	// Respond to the client
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

	value := api.memtable.Del([]byte(key))
	api.memtable.MarkDeleted([]byte(key))

	if value == nil {
		w.Write([]byte("Key not found\n"))
		return
	}

	if api.memtable.deletedKeys.Len() >= threshold {
		flush(api.memtable)
	}

	w.Write([]byte(fmt.Sprintf("Deleted Value: %s\n", value)))
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
		fmt.Println("sstFilePath:", sstFilePath)

		reader := bufio.NewReader(sstFile)

		// Read the magic number and check if it matches the expected value.
		var magicNumber uint32
		if err := binary.Read(reader, binary.BigEndian, &magicNumber); err != nil {
			log.Printf("Error reading magic number from SST file %s: %v\n", sstFilePath, err)
			continue
		}
		fmt.Println("magicNumber:", magicNumber)

		if magicNumber != 0x23102003 {
			log.Printf("Invalid SST file format in %s\n", sstFilePath)
			continue
		}

		// Read the entry count, smallest key length, smallest key, largest key length, and largest key.
		var entryCount, smallestKeyLen, largestKeyLen uint32
		if err := binary.Read(reader, binary.BigEndian, &entryCount); err != nil {
			log.Printf("Error reading entry count from SST file %s: %v\n", sstFilePath, err)
			continue
		}
		fmt.Println("entryCount:", entryCount)

		if err := binary.Read(reader, binary.BigEndian, &smallestKeyLen); err != nil {
			log.Printf("Error reading smallest key length from SST file %s: %v\n", sstFilePath, err)
			continue
		}
		fmt.Println("smallestKeyLen:", smallestKeyLen)

		smallestKeyBytes := make([]byte, smallestKeyLen)
		if _, err := reader.Read(smallestKeyBytes); err != nil {
			log.Printf("Error reading smallest key from SST file %s: %v\n", sstFilePath, err)
			continue
		}

		smallestKey := string(smallestKeyBytes)
		fmt.Println("smallestKey:", smallestKey)

		if err := binary.Read(reader, binary.BigEndian, &largestKeyLen); err != nil {
			log.Printf("Error reading largest key length from SST file %s: %v\n", sstFilePath, err)
			continue
		}
		fmt.Println("largestKeyLen:", largestKeyLen)

		largestKeyBytes := make([]byte, largestKeyLen)
		if _, err := reader.Read(largestKeyBytes); err != nil {
			log.Printf("Error reading largest key from SST file %s: %v\n", sstFilePath, err)
			continue
		}

		largestKey := string(largestKeyBytes)
		fmt.Println("largestKey:", largestKey)

		var version uint16
		if err := binary.Read(reader, binary.BigEndian, &version); err != nil {
			log.Printf("Error reading key length from SST file %s: %v\n", sstFilePath, err)
			break
		}
		fmt.Println("version:", version)

		// Check if the target key falls within the range defined by the smallest and largest keys.
		if len(key) >= int(smallestKeyLen) && len(key) <= int(largestKeyLen) {
			// Perform a linear search within the SST file for the target key.
			for j := 0; j < int(entryCount); j++ {
				var opType byte
				if err := binary.Read(reader, binary.BigEndian, &opType); err != nil {
					log.Printf("Error reading key length from SST file %s: %v\n", sstFilePath, err)
					break
				}
				fmt.Println("opType:", opType)

				if string(opType) == "S" {
					// Read the key length, key, value length, and value.
					var keyLen, valueLen uint32
					if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
						log.Printf("Error reading key length from SST file %s: %v\n", sstFilePath, err)
						break
					}
					fmt.Println("keyLen:", keyLen)

					keyBytes := make([]byte, keyLen)
					if _, err := reader.Read(keyBytes); err != nil {
						log.Printf("Error reading key from SST file %s: %v\n", sstFilePath, err)
						break
					}

					currentKey := string(keyBytes)
					fmt.Println("currentKey:", currentKey)

					if err := binary.Read(reader, binary.BigEndian, &valueLen); err != nil {
						log.Printf("Error reading value length from SST file %s: %v\n", sstFilePath, err)
						break
					}
					fmt.Println("valueLen:", valueLen)

					if currentKey == key {
						found = true
						valueBytes := make([]byte, valueLen)
						if _, err := reader.Read(valueBytes); err != nil {
							log.Printf("Error reading value from SST file %s: %v\n", sstFilePath, err)
							break
						}

						value = string(valueBytes)
						fmt.Println("value:", value)
					} else {
						notValueBytes := make([]byte, valueLen)
						if _, err := reader.Read(notValueBytes); err != nil {
							log.Printf("Error reading value from SST file %s: %v\n", sstFilePath, err)
							break
						}

						// remove the 2 lines below when you finish testing
						notValue := string(notValueBytes)
						fmt.Println("notValue:", notValue)
					}
				} else if string(opType) == "D" {
					var keyLen uint32
					if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
						log.Printf("Error reading key length from SST file %s: %v\n", sstFilePath, err)
						break
					}
					fmt.Println("keyLen:", keyLen)

					keyBytes := make([]byte, keyLen)
					if _, err := reader.Read(keyBytes); err != nil {
						log.Printf("Error reading key from SST file %s: %v\n", sstFilePath, err)
						break
					}

					currentKey := string(keyBytes)
					fmt.Println("currentKey:", currentKey)

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

	// If the key is still not found after searching all SST files, respond accordingly.
	w.Write([]byte("Key not found in SST files\n"))
}
