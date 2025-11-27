package gex

import (
	"database/sql"
	"path/filepath"
	"sort"

	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-sys/log"
)

type (
	Technology struct {
		Name      string     `json:"name"`
		Id        string     `json:"id"`
		ExprTypes []ExprType `json:"exprTypes"`
	}

	DatasetsCache struct {
		dir  string
		path string
	}
)

const (
	GenesSql = `SELECT 
		genome.id, 
		genome.gene_id, 
		genome.gene_symbol 
		FROM genes 
		ORDER BY genome.gene_symbol`

	SpeciesSql = `SELECT DISTINCT
		species,
		FROM datasets
		ORDER BY species`

	TechnologiesSQL = `SELECT
		datasets.platform
		FROM datasets
		WHERE datasets.species = ?1 
		ORDER BY datasets.platform`

	AllTechnologiesSQL = `SELECT DISTINCT 
		species, technology, platform 
		FROM datasets 
		ORDER BY species, technology, platform`

	DatasetsSQL = `SELECT 
		datasets.id,
		datasets.path
		FROM datasets 
		WHERE datasets.species = ?1 AND datasets.technology = ?2
		ORDER BY datasets.id`

	DatasetFromPublicIdSQL = `SELECT 
		datasets.id,
		datasets.path
		FROM datasets 
		WHERE datasets.public_id = ?1`
)

func NewDatasetsCache(dir string) *DatasetsCache {

	path := filepath.Join(dir, "gex.db")

	// db, err := sql.Open("sqlite3", path)

	// if err != nil {
	// 	log.Fatal().Msgf("%s", err)
	// }

	// defer db.Close()

	return &DatasetsCache{dir: dir, path: path}
}

func (cache *DatasetsCache) Dir() string {
	return cache.dir
}

func (cache *DatasetsCache) Species() ([]string, error) {
	db, err := sql.Open(sys.Sqlite3DB, cache.path)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	species := make([]string, 0, 10)

	rows, err := db.Query(SpeciesSql)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var name string

		err := rows.Scan(
			&name)

		if err != nil {
			return nil, err
		}

		species = append(species, name)
	}

	return species, nil
}

func (cache *DatasetsCache) Technologies(species string) ([]string, error) {
	db, err := sql.Open(sys.Sqlite3DB, cache.path)

	if err != nil {

		return nil, err
	}

	defer db.Close()

	platforms := make([]string, 0, 10)

	rows, err := db.Query(TechnologiesSQL, species)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var platform string

		err := rows.Scan(
			&platform)

		if err != nil {
			return nil, err
		}

		platforms = append(platforms, platform)
	}

	return platforms, nil
}

func (cache *DatasetsCache) AllTechnologies() (map[string]map[string][]string, error) {
	db, err := sql.Open(sys.Sqlite3DB, cache.path)

	if err != nil {

		return nil, err
	}

	defer db.Close()

	technologies := make(map[string]map[string][]string)

	rows, err := db.Query(AllTechnologiesSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var species string
	var technology string
	var platform string

	for rows.Next() {

		err := rows.Scan(&species,
			&technology,
			&platform)

		if err != nil {
			return nil, err
		}

		if technologies[species] == nil {
			technologies[species] = make(map[string][]string)
		}

		if technologies[species][technology] == nil {
			technologies[species][technology] = make([]string, 0, 10)
		}

		technologies[species][technology] = append(technologies[species][technology], platform)

	}

	return technologies, nil
}

func (cache *DatasetsCache) Datasets(species string, technology string) ([]*Dataset, error) {

	db, err := sql.Open(sys.Sqlite3DB, cache.path)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	datasetRows, err := db.Query(DatasetsSQL, species, technology)

	if err != nil {
		return nil, err
	}

	defer datasetRows.Close()

	var id int
	var path string

	datasets := make([]*Dataset, 0, 10)

	for datasetRows.Next() {

		err := datasetRows.Scan(
			&id,
			&path)

		if err != nil {
			return nil, err
		}

		// the largest dataset is around 500 samples
		// so use that as an estimate
		//dataset.Samples = make([]*Sample, 0, DatasetSize)

		log.Debug().Msgf("db %s", filepath.Join(cache.dir, path))

		datasetCache := NewDatasetCache(cache.dir, path)

		dataset, err := datasetCache.Dataset()

		if err != nil {
			return nil, err
		}

		datasets = append(datasets, dataset)
	}

	return datasets, nil
}

func (cache *DatasetsCache) DatasetCacheFromId(datasetId string) (*DatasetCache, error) {
	db, err := sql.Open(sys.Sqlite3DB, cache.path)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	var id int
	var path string

	err = db.QueryRow(DatasetFromPublicIdSQL, datasetId).Scan(
		&id,
		&path)

	if err != nil {
		return nil, err
	}

	datasetCache := NewDatasetCache(cache.dir, path)

	return datasetCache, nil
}

func (cache *DatasetsCache) ExprTypes(datasetIds []string) ([]*ExprType, error) {

	allExprTypes := make(map[string]*ExprType)

	for _, datasetId := range datasetIds {
		c, err := cache.DatasetCacheFromId(datasetId)

		if err != nil {
			return nil, err
		}

		exprTypes, err := c.ExprTypes()

		if err != nil {
			return nil, err
		}

		for _, exprType := range exprTypes {
			allExprTypes[exprType.Id] = exprType
		}
	}

	ret := make([]*ExprType, 0, len(datasetIds))

	for _, exprType := range allExprTypes {
		ret = append(ret, exprType)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Name < ret[j].Name
	})

	return ret, nil
}

func (cache *DatasetsCache) FindSeqValues(datasetId string,
	exprType *ExprType,
	geneIds []string) (*SearchResults, error) {

	c, err := cache.DatasetCacheFromId(datasetId)

	if err != nil {
		return nil, err
	}

	res, err := c.FindSeqValues(exprType, geneIds)

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (cache *DatasetsCache) FindMicroarrayValues(datasetId string,
	geneIds []string) (*SearchResults, error) {

	c, err := cache.DatasetCacheFromId(datasetId)

	if err != nil {
		return nil, err
	}

	res, err := c.FindMicroarrayValues(geneIds)

	if err != nil {
		return nil, err
	}

	return res, nil
}
