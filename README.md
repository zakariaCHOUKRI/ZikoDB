# ZikoDB

A persistent key-value store with an HTTP API, inspired by the LSM tree model, implemented in Go.

## Overview

This project aims to build a key-value store that exposes HTTP endpoints for interacting with and manipulating its data. The architecture follows the LSM tree model for reading and writing data, similar to projects like [leveldb](https://github.com/google/leveldb) and [rocksdb](https://github.com/facebook/rocksdb).

## Features

- Basic key-value operations: `GET`, `POST`, `DELETE`
- Memtable for in-memory writes
- Write Ahead Log (WAL) for crash safety
- Periodic flushing of Memtable to disk as an SST file
- ~~Compaction process to merge smaller SST files~~
- SST file format in binary
- ~~Extras: Bloom filters, Compression of SST files, Concurrency~~
- User Interface: Accessible through a web browser at http://localhost:8080

## Getting Started

### Prerequisites

- [Go](https://golang.org/) installed on your machine.

### Installation

Clone the repository:

```
git clone https://github.com/zakariaCHOUKRI/ZikoDB.git
cd ZikoDB
```

### Running the Application

Run the main application:

```
go run .
```

The server will start listening on http://localhost:8080.

## Manual Testing

The ZikoDB project has undergone extensive testing through countless tests and test cases to ensure optimal performance and reliability.

To repeat some of the tests performed, you can use the following commands (specific to Windows, adapt for Unix systems as needed):

```
cd stress_testing
cmd < set_commands.txt
cmd < del_commands.txt
cmd < get_commands.txt > get_results.txt
```

Note: If you are using a Unix system, please use the appropriate method for running the commands.

If you wish to modify the values for the set/del/get commands, you can do so in the `script.py` file. Additionally, for experimenting with different thresholds and periods for the automatic flush, you can edit the constants `threshold` and `interval` in the `sst.go` file.

The test results will be displayed both in the terminal and through the HTTP interface. For individual command testing without rewriting them, you can utilize the user interface accessible through http://localhost:8080.

To test the flush to WAL functionality, you can use the pre-made files and follow these steps:

1. Change the `interval` to a relatively large value (e.g., 10 minutes).
2. Set the `threshold` to 600.

Launch the set_commands.txt file, which contains 1000 commands. With the configured parameters, 600 commands will be flushed, and 400 will not. After executing the commands, exit the application, then restart it. You should observe that the WAL has been flushed.

You can further verify this by testing with the get_commands.txt file.


## HTTP Endpoints

- `GET http://localhost:8080/get?key=keyName`: Retrieve the value of the key or print 'Key not found.'
- `POST http://localhost:8080/set`: Set the key and value provided in the request body (use JSON to encode key-value pairs).
- `DELETE http://localhost:8080/del?key=keyName`: Delete a key from the key-value store.


## Notes

1. The project works perfectly but does not have the extra functionality: bloom filters, concurrency, compression.
2. Initially, an external library of a sorted map was used as the in-memory storage medium. However, it made it very difficult to implement additional functionality, and bugs were challenging to debug.
3. Due to the previous point, the data in my SST files is not ordered, so compaction could not be achieved.
4. Although bloom filters were not used, time is saved in lookups thanks to the max and min key lengths present in each SST header.
6. The unit tests are not very detailed because most of the functionality can be accessed through the API
5. The implementation is extremely fast, and you can test it by following the steps in the manual test category.

## Acknowledgments

- Thanks to [Mehdi Cheracher](https://github.com/chermehdi) for the project idea and guidance.