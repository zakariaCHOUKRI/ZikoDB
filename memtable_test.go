package main

import "testing"

func TestSet(t *testing.T) {
	memtable := NewMemtable()
	memtable.Set("1", []byte("1"))
	if string(memtable.data["1"]) != "1" {
		t.Errorf("Set(1, 1) did not correctly set the data")
	}
}

func TestGet(t *testing.T) {
	memtable := NewMemtable()
	memtable.Set("1", []byte("1"))
	if string(memtable.Get("1")) != "1" {
		t.Errorf("Get(1) did not get the correct data")
	}
}

func TestDel(t *testing.T) {
	memtable := NewMemtable()
	memtable.Set("1", []byte("1"))
	memtable.Del("1")
	if memtable.Get("1") != nil {
		t.Errorf("Del(1) did not delete the data")
	}
}

func TestClear(t *testing.T) {
	memtable := NewMemtable()
	memtable.Set("1", []byte("1"))
	memtable.Clear()
	if len(memtable.data) != 0 {
		t.Errorf("Memtable didn't get cleared")
	}
}
