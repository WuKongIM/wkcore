package multiraft

type storageAdapter struct {
	storage Storage
}

func newStorageAdapter(storage Storage) *storageAdapter {
	return &storageAdapter{storage: storage}
}
