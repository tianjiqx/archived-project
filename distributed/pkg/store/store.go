package store

import (
	"github.com/matrixorigin/talent-challenge/matrixbase/distributed/pkg/cfg"
)

// Store the store interface
type Store interface {
	// Set set key-value to store
	Set(key []byte, value []byte) error
	// Get returns the value from store
	Get(key []byte) ([]byte, error)
	// Delete remove the key from store
	Delete(key []byte) error

	Close() error
}

// NewStore create the raft store
func NewStore(cfg cfg.StoreCfg) (Store, error) {
	if cfg.Memory {
		return newMemoryStore()
	}

	// TODO: need to implement
	var peers = [][]string{{"http://node1:22101"}, {"http://node1:22201"}}
	return newPebbleStore(cfg, "node1:8080", peers)
}

// NewStore create the raft store
func NewStoreWithName(cfg cfg.StoreCfg, name string) (Store, error) {
	if cfg.Memory {
		return newMemoryStore()
	}

	// TODO: need to implement
	var peers = [][]string{{"http://node1:22101", "http://node2:22101", "http://node3:22101"},
		{"http://node1:22201", "http://node2:22201", "http://node3:22201"}}
	return newPebbleStore(cfg, name, peers)
}
