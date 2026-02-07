package gexdb

import (
	"sync"

	"github.com/antonybholmes/go-gex"
	"github.com/antonybholmes/go-sys"
)

var (
	instance *gex.GexDB
	once     sync.Once

	technologies = []gex.Technology{
		{Id: sys.BlankUUID, Name: "RNA-seq", ExprTypes: []gex.ExprType{
			{Id: sys.BlankUUID, Name: "Counts"},
			{Id: sys.BlankUUID, Name: "TPM"},
			{Id: sys.BlankUUID, Name: "VST"}}},
		{Id: sys.BlankUUID, Name: "Microarray", ExprTypes: []gex.ExprType{{Id: sys.BlankUUID, Name: "RMA"}}},
	}
)

func Technologies() []gex.Technology {
	return technologies
}

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

func Genomes() ([]*gex.Genome, error) {
	return instance.Genomes()
}

// func Platforms(species string) ([]string, error) {
// 	return instance.Plaforms(species)
// }

func ExprTypes(datasets []string, isAdmin bool, permissions []string) ([]*gex.ExprType, error) {
	return instance.ExprTypes(datasets, isAdmin, permissions)
}

func Datasets(species string, technology string, isAdmin bool, permissions []string) ([]*gex.Dataset, error) {
	return instance.Datasets(species, technology, permissions, isAdmin)
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
