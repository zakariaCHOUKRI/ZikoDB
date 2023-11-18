# ZikoDB

A persistent key-value store with an HTTP API, inspired by the LSM tree model, implemented in Go.

## Overview

This project aims to build a key-value store that exposes HTTP endpoints for interacting with and manipulating its data. The architecture follows the LSM tree model for reading and writing data, similar to projects like [leveldb](https://github.com/google/leveldb) and [rocksdb](https://github.com/facebook/rocksdb).

## Features

- Basic key-value operations: GET, POST, DELETE
- Memtable for in-memory writes
- Write Ahead Log (WAL) for crash safety
- Periodic flushing of Memtable to disk as an SST file
- Compaction process to merge smaller SST files
- SST file format in binary with suggested structure
- Extras: Bloom filters, Compression of SST files

## Getting Started

### Prerequisites

- [Go](https://golang.org/) installed on your machine.

### Installation

Clone the repository:

```
git clone https://github.com/zakariaCHOUKRI/ZikoDB.git
cd go-kvstore
```

### Running the Application

Run the main application:

```
go run cmd/ZikoDB/main.go
```

The server will start listening on http://localhost:8080.

## HTTP Endpoints

- GET http://localhost:8080/get?key=keyName: Retrieve the value of the key or print 'Key not found.'
- POST http://localhost:8080/set: Set the key and value provided in the request body (use JSON to encode key-value pairs).
- DELETE http://localhost:8080/del?key=keyName: Delete a key from the key-value store and return the existing value (if it exists).

## Acknowledgments

- Thanks to [Mehdi Cheracher](https://github.com/chermehdi) for the project idea and guidance.
