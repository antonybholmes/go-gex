package gexdb

import (
	"sync"

	"github.com/antonybholmes/go-gex"
	"github.com/antonybholmes/go-sys"
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

func Genomes() ([]*sys.Entity, error) {
	return instance.Genomes()
}

// func Platforms(species string) ([]string, error) {
// 	return instance.Plaforms(species)
// }

func ExprTypes(datasets []string, isAdmin bool, permissions []string) ([]*sys.Entity, error) {
	return instance.ExprTypes(datasets, isAdmin, permissions)
}

func Datasets(genome string, technology string, isAdmin bool, permissions []string) ([]*gex.Dataset, error) {
	return instance.Datasets(genome, technology, permissions, isAdmin)
}

func Technologies() ([]*sys.Entity, error) {
	return instance.Technologies()
}

func Expression(datasetId string, exprTypeId *sys.Entity, probes []*gex.Probe, isAdmin bool, permissions []string) (*gex.SearchResults, error) {
	return instance.Expression(datasetId, exprTypeId, probes, isAdmin, permissions)
}

func ExprType(id string) (*sys.Entity, error) {
	return instance.ExprType(id)
}

func FindProbes(genes []string) ([]*gex.Probe, error) {
	return instance.FindProbes(genes)
}

// func FindSeqValues(datasetId string, exprTypeId string, genes []string, isAdmin bool, permissions []string) (*gex.SearchResults, error) {
// 	return instance.FindSeqValues(datasetId, exprTypeId, genes, isAdmin, permissions)
// }

// func FindMicroarrayValues(datasetId string, exprTypeId string, genes []string, isAdmin bool, permissions []string) (*gex.SearchResults, error) {
// 	return instance.FindMicroarrayValues(datasetId, exprTypeId, genes, isAdmin, permissions)
// }

// func GetDataset(uuid string) (*gex.Dataset, error) {
// 	return instance.GetDataset(uuid)
// }

// func Search(location *dna.Location, uuids []string) (*gex.SearchResults, error) {
// 	return instance.Search(location, uuids)
// }
