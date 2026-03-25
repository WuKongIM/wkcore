package multiraft

import raft "go.etcd.io/raft/v3"

type storageAdapter struct {
	storage Storage
	memory  *raft.MemoryStorage
}

func newStorageAdapter(storage Storage) *storageAdapter {
	return &storageAdapter{storage: storage}
}
