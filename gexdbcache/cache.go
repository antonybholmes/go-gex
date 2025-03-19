package gexdbcache

import (
	"sync"

	"github.com/antonybholmes/go-gex"
)

var instance *gex.DatasetsCache
var once sync.Once
var platforms []gex.Platform

func init() {
	platforms = []gex.Platform{
		{PublicId: "8wyay6lyvz9f", Name: "RNA-seq", GexTypes: []string{"Counts", "TPM", "VST"}},
		{PublicId: "4fdknkjpa95h", Name: "Microarray", GexTypes: []string{"RMA"}}}

}

func Platforms() []gex.Platform {
	return platforms
}

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

// func Platforms(species string) ([]string, error) {
// 	return instance.Plaforms(species)
// }

func Datasets(species string, platform string) ([]*gex.Dataset, error) {
	return instance.Datasets(species, platform)
}

func FindRNASeqValues(datasetIds []string,
	gexType string,
	geneIds []string) ([]*gex.SearchResults, error) {
	return instance.FindRNASeqValues(datasetIds, gexType, geneIds)
}

func FindMicroarrayValues(datasetIds []string,
	geneIds []string) ([]*gex.SearchResults, error) {
	return instance.FindMicroarrayValues(datasetIds, geneIds)
}

// func GetDataset(uuid string) (*gex.Dataset, error) {
// 	return instance.GetDataset(uuid)
// }

// func Search(location *dna.Location, uuids []string) (*gex.SearchResults, error) {
// 	return instance.Search(location, uuids)
// }
