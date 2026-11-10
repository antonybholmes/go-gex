package gexdb

import (
	"sync"

	"github.com/antonybholmes/go-gex"
)

var (
	instance *gex.GexDB
	once     sync.Once
)

func InitGexDB(path string) (*gex.GexDB, error) {
	once.Do(func() {
		instance = gex.NewGexDB(path)
	})

	return instance, nil
}

func Instance() *gex.GexDB {
	return instance
}
