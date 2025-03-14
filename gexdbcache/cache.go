package gexdbcache

import (
	"sync"

	"github.com/antonybholmes/go-gex"
)

var instance *gex.DatasetsCache
var once sync.Once

func InitCache(dir string) (*gex.DatasetsCache, error) {
	once.Do(func() {
		instance = gex.NewDatasetsCache(dir)
	})

	return instance, nil
}

func GetInstance() *gex.DatasetsCache {
	return instance
}

func Dir() string {
	return instance.Dir()
}

func Species() ([]string, error) {
	return instance.Species()
}

func Platforms(species string) ([]string, error) {
	return instance.Plaforms(species)
}

func Datasets(species string, platform string) ([]*gex.Dataset, error) {
	return instance.Datasets(species, platform)
}

func FindRNASeqValues(datasetId string,
	gexType string,
	geneIds []string) (*gex.SearchResults, error) {
	return instance.FindRNASeqValues(datasetId, gexType, geneIds)
}

func FindMicroarrayValues(datasetId string,
	geneIds []string) (*gex.SearchResults, error) {
	return instance.FindMicroarrayValues(datasetId, geneIds)
}

// func GetDataset(uuid string) (*gex.Dataset, error) {
// 	return instance.GetDataset(uuid)
// }

// func Search(location *dna.Location, uuids []string) (*gex.SearchResults, error) {
// 	return instance.Search(location, uuids)
// }
