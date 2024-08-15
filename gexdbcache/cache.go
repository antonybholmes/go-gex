package gexdbcache

import (
	"sync"

	"github.com/antonybholmes/go-gex"
)

var instance *gex.DatasetCache
var once sync.Once

func InitCache(dir string) (*gex.DatasetCache, error) {
	once.Do(func() {
		instance = gex.NewGexDBCache(dir)
	})

	return instance, nil
}

func GetInstance() *gex.DatasetCache {
	return instance
}

func Dir() string {
	return instance.Dir()
}

func GexTypes() ([]*gex.GexType, error) {
	return instance.GexTypes()
}

func Datasets(gexType int) ([]*gex.Dataset, error) {
	return instance.Datasets(gexType)
}

func GetGenes(genes []string) ([]*gex.GexGene, error) {
	return instance.GetGenes(genes)
}

func RNASeqValues(genes []*gex.GexGene, datasets []int) ([]*gex.RNASeqGeneResults, error) {
	return instance.RNASeqValues(genes, datasets)
}

// func GetDataset(uuid string) (*gex.Dataset, error) {
// 	return instance.GetDataset(uuid)
// }

// func Search(location *dna.Location, uuids []string) (*gex.SearchResults, error) {
// 	return instance.Search(location, uuids)
// }
