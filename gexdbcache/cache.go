package gexdbcache

import (
	"sync"

	"github.com/antonybholmes/go-dna"
	"github.com/antonybholmes/go-gex"
)

var instance *gex.DatasetCache
var once sync.Once

func InitCache(dir string) (*gex.DatasetCache, error) {
	once.Do(func() {
		instance = gex.NewMutationDBCache(dir)
	})

	return instance, nil
}

func GetInstance() *gex.DatasetCache {
	return instance
}

func Dir() string {
	return instance.Dir()
}

func GetDataset(uuid string) (*gex.Dataset, error) {
	return instance.GetDataset(uuid)
}

func Search(location *dna.Location, uuids []string) (*gex.SearchResults, error) {
	return instance.Search(location, uuids)
}
