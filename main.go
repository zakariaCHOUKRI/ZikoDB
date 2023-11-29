package main

import (
	"fmt"
	"net/http"
)

func main() {

	memtable := NewMemtable()
	wal, err := NewWAL("data/wal/wal")
	if err != nil {
		fmt.Println("Error creating WAL:", err)
		return
	}
	integrityCheck()

	// Start the periodic flush goroutine
	go periodicFlush(memtable)

	// Start the API
	go StartAPI(memtable, wal)

	// Serve the web page
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	// Start the web server
	port := 8080
	fmt.Printf("Web page available at http://localhost:%d\n", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)

	// memtable := NewMemtable()
	// wal, err := NewWAL("data/wal/wal")
	// if err != nil {
	// 	fmt.Println("Error creating WAL:", err)
	// 	return
	// }

	// memtable.Set([]byte("zakaria"), []byte("choukri"))

	// //Start the API
	// go StartAPI(memtable, wal)

	// // Serve the web page
	// http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
	// 	http.ServeFile(w, r, "index.html")
	// })

	// // Start the web server
	// port := 8080
	// fmt.Printf("Web page available at http://localhost:%d\n", port)
	// http.ListenAndServe(fmt.Sprintf(":%d", port), nil)

	// memtable := NewMemtable()
	// memtable.Set([]byte("key1"), []byte("value0"))
	// memtable.Set([]byte("1"), 2)
	// memtable.Set([]byte("2"), 4)
	// memtable.Set([]byte("3"), 6)
	// memtable.Set([]byte("4"), 8)
	// memtable.Set([]byte("key2"), []byte("value2"))

	// fmt.Println("the contents of memtable are")
	// for i := memtable.data.Front(); i != nil; i = i.Next() {
	// 	fmt.Println(i.Key(), i.Value)
	// }

	// fmt.Println("deletion of element with key '4'")
	// memtable.Del([]byte("4"))
	// fmt.Println("set key1 value1")
	// memtable.Set([]byte("key1"), []byte("value1"))
	// fmt.Println("the contents of memtable are")
	// for i := memtable.data.Front(); i != nil; i = i.Next() {
	// 	fmt.Println(i.Key(), i.Value)
	// }

	// flush(memtable)
	// fmt.Println("the contents of memtable are")
	// for i := memtable.data.Front(); i != nil; i = i.Next() {
	// 	fmt.Println(i.Key(), i.Value)
	// }

	// memtable.Set([]byte("zakaria"), []byte("choukri"))
	// memtable.Set([]byte("choukri"), []byte("zakaria"))

	// fmt.Println("the contents of memtable are")
	// for i := memtable.data.Front(); i != nil; i = i.Next() {
	// 	fmt.Println(i.Key(), i.Value)
	// }
	// time.Sleep(10 * time.Second)
	// flush(memtable)
	// fmt.Println("the contents of memtable are")
	// for i := memtable.data.Front(); i != nil; i = i.Next() {
	// 	fmt.Println(i.Key(), i.Value)
	// }

	// wal, err := NewWAL("data/wal/wal1.wal")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer wal.Close()

	// // Example 1: Set operation
	// setEntry := &WALEntry{
	// 	Action: 's',
	// 	Key:    []byte("example_key"),
	// 	Value:  []byte("example_value"),
	// }

	// if err := wal.Write(setEntry); err != nil {
	// 	log.Fatal(err)
	// }

	// // Example 2: Delete operation
	// deleteEntry := &WALEntry{
	// 	Action: 'd',
	// 	Key:    []byte("example_key"),
	// }

	// if err := wal.Write(deleteEntry); err != nil {
	// 	log.Fatal(err)
	// }

	// // Add more entries as needed

	// log.Println("WAL entries written successfully.")

}
