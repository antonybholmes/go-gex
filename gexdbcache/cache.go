package gexdbcache

import (
	"sync"

	"github.com/antonybholmes/go-gex"
)

var (
	instance     *gex.DatasetsCache
	once         sync.Once
	technologies []gex.Technology
)

func init() {
	technologies = []gex.Technology{
		{PublicId: "8wyay6lyvz9f", Name: "RNA-seq", GexTypes: []string{"Counts", "TPM", "VST"}},
		{PublicId: "4fdknkjpa95h", Name: "Microarray", GexTypes: []string{"RMA"}}}

}

func Technologies() []gex.Technology {
	return technologies
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

func ExprTypes(datasetIds []string) ([]*gex.ExprType, error) {
	return instance.ExprTypes(datasetIds)
}

func Datasets(species string, technology string) ([]*gex.Dataset, error) {
	return instance.Datasets(species, technology)
}

func AllTechnologies() (map[string]map[string][]string, error) {
	return instance.AllTechnologies()
}

func FindSeqValues(datasetId string, exprType *gex.ExprType, geneIds []string) (*gex.SearchResults, error) {
	return instance.FindSeqValues(datasetId, exprType, geneIds)
}

func FindMicroarrayValues(datasetId string, geneIds []string) (*gex.SearchResults, error) {
	return instance.FindMicroarrayValues(datasetId, geneIds)
}

// func GetDataset(uuid string) (*gex.Dataset, error) {
// 	return instance.GetDataset(uuid)
// }

// func Search(location *dna.Location, uuids []string) (*gex.SearchResults, error) {
// 	return instance.Search(location, uuids)
// }
