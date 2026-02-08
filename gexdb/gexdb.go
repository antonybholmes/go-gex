package gexdb

import (
	"sync"

	"github.com/antonybholmes/go-gex"
)

var (
	instance *gex.GexDB
	once     sync.Once
)

func InitGexDB(dir string) (*gex.GexDB, error) {
	once.Do(func() {
		instance = gex.NewGexDB(dir)
	})

	return instance, nil
}

func GetInstance() *gex.GexDB {
	return instance
}

func Dir() string {
	return instance.Dir()
}

func Genomes() ([]*gex.Idtype, error) {
	return instance.Genomes()
}

// func Platforms(species string) ([]string, error) {
// 	return instance.Plaforms(species)
// }

func ExprTypes(datasets []string, isAdmin bool, permissions []string) ([]*gex.Idtype, error) {
	return instance.ExprTypes(datasets, isAdmin, permissions)
}

func Datasets(species string, technology string, isAdmin bool, permissions []string) ([]*gex.Dataset, error) {
	return instance.Datasets(species, technology, permissions, isAdmin)
}

func AllTechnologies() (map[string]map[string][]string, error) {
	return instance.AllTechnologies()
}

func FindSeqValues(datasetId string, exprTypeId string, genes []string, isAdmin bool, permissions []string) (*gex.SearchResults, error) {
	return instance.FindSeqValues(datasetId, exprTypeId, genes, isAdmin, permissions)
}

func FindMicroarrayValues(datasetId string, exprTypeId string, genes []string, isAdmin bool, permissions []string) (*gex.SearchResults, error) {
	return instance.FindMicroarrayValues(datasetId, exprTypeId, genes, isAdmin, permissions)
}

// func GetDataset(uuid string) (*gex.Dataset, error) {
// 	return instance.GetDataset(uuid)
// }

// func Search(location *dna.Location, uuids []string) (*gex.SearchResults, error) {
// 	return instance.Search(location, uuids)
// }
