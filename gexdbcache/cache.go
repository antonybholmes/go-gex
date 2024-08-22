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

func Platforms() ([]*gex.Platform, error) {
	return instance.Plaforms()
}

func GexValueTypes(platform *gex.Platform) ([]*gex.GexValueType, error) {
	return instance.GexValueTypes(platform)
}

func Datasets(platform *gex.Platform) ([]*gex.Dataset, error) {
	return instance.Datasets(platform)
}

func GetGenes(genes []string) ([]*gex.GexGene, error) {
	return instance.GetGenes(genes)
}

func RNASeqValues(genes []*gex.GexGene, platform *gex.Platform, gexValueType *gex.GexValueType, datasets []int) ([]*gex.ResultGene, error) {
	return instance.RNASeqValues(genes, platform, gexValueType, datasets)
}

func MicroarrayValues(genes []*gex.GexGene, platform *gex.Platform, gexValueType *gex.GexValueType, datasets []int) ([]*gex.ResultGene, error) {
	return instance.MicroarrayValues(genes, platform, gexValueType, datasets)
}

// func GetDataset(uuid string) (*gex.Dataset, error) {
// 	return instance.GetDataset(uuid)
// }

// func Search(location *dna.Location, uuids []string) (*gex.SearchResults, error) {
// 	return instance.Search(location, uuids)
// }
