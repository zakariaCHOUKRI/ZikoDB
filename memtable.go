package main

type Memtable struct {
	data        map[string][]byte
	deletedKeys map[string][]byte
}

func NewMemtable() *Memtable {
	return &Memtable{
		data:        make(map[string][]byte),
		deletedKeys: make(map[string][]byte),
	}
}

func (m *Memtable) Set(key string, value []byte) {

	m.data[key] = value

	if len(m.data) >= threshold {
		flush(m)
	}
}

func (m *Memtable) Get(key string) []byte {

	return m.data[key]
}

func (m *Memtable) Del(key string) []byte {

	value := m.data[key]
	delete(m.data, key)
	return value
}

// Add a key to the deleted table
func (m *Memtable) MarkDeleted(key string) {

	m.deletedKeys[key] = []byte("Z")

	if len(m.deletedKeys) >= threshold {
		flush(m)
	}
}

func (m *Memtable) IsDeleted(key string) bool {

	if m.deletedKeys[key] == nil {
		return m.data[key] == nil
	} else {
		return true
	}

	// return (m.deletedKeys.Get(key) != nil ||
	// 	(m.data.Get(key) == nil && m.deletedKeys.Get(key) == nil))
}

// Clear the memtable data and the deleted table
func (m *Memtable) Clear() {

	// n := m.data.Len()
	// for n > 0 {
	// 	m.data.RemoveFront()
	// }

	m.data = make(map[string][]byte)
	m.deletedKeys = make(map[string][]byte)

}
