package main

import (
	"sync"

	"github.com/huandu/skiplist"
)

type Memtable struct {
	data        *skiplist.SkipList
	mu          sync.RWMutex
	deletedKeys *skiplist.SkipList
}

func NewMemtable() *Memtable {
	return &Memtable{
		data:        skiplist.New(skiplist.Bytes),
		deletedKeys: skiplist.New(skiplist.Bytes),
	}
}

func (m *Memtable) Set(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data.Set(key, value)
}

func (m *Memtable) Get(key []byte) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	value := m.data.Get(key)
	if value != nil {
		return value.Value.([]byte)
	} else {
		return nil
	}
}

func (m *Memtable) Del(key []byte) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	if value := m.data.Remove(key); value != nil {
		return value.Value.([]byte)
	} else {
		return nil
	}
}

func (m *Memtable) MarkDeleted(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.deletedKeys.Set(key, make([]byte, 0))
}

func (m *Memtable) IsDeleted(key []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.deletedKeys.Get(key) == nil {
		return m.deletedKeys.Get(key) == nil
	} else {
		return true
	}

	// return (m.deletedKeys.Get(key) != nil ||
	// 	(m.data.Get(key) == nil && m.deletedKeys.Get(key) == nil))
}

func (m *Memtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// n := m.data.Len()
	// for n > 0 {
	// 	m.data.RemoveFront()
	// }

	m.data = skiplist.New(skiplist.Bytes)
	m.deletedKeys = skiplist.New(skiplist.Byte)

}
