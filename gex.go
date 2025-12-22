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

	GexDB struct {
		dir  string
		path string
		db   *sql.DB
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
		WHERE datasets.species = :species 
		ORDER BY datasets.platform`

	AllTechnologiesSQL = `SELECT DISTINCT 
		species, technology, platform 
		FROM datasets 
		ORDER BY species, technology, platform`

	DatasetsSQL = `SELECT 
		datasets.id,
		datasets.path
		FROM datasets 
		WHERE datasets.species = :species AND datasets.technology = :technology
		ORDER BY datasets.id`

	DatasetFromIdSQL = `SELECT 
		datasets.id,
		datasets.path
		FROM datasets 
		WHERE datasets.id = :id`
)

func NewGexDB(dir string) *GexDB {

	path := filepath.Join(dir, "gex.db")

	// db, err := sql.Open("sqlite3", path)

	// if err != nil {
	// 	log.Fatal().Msgf("%s", err)
	// }

	// defer db.Close()

	return &GexDB{dir: dir, path: path, db: sys.Must(sql.Open(sys.Sqlite3DB, path))}
}

func (gdb *GexDB) Close() error {
	return gdb.db.Close()
}

func (gdb *GexDB) Dir() string {
	return gdb.dir
}

func (gdb *GexDB) Species() ([]string, error) {

	species := make([]string, 0, 10)

	rows, err := gdb.db.Query(SpeciesSql)

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

func (gdb *GexDB) Technologies(species string) ([]string, error) {

	platforms := make([]string, 0, 10)

	rows, err := gdb.db.Query(TechnologiesSQL, sql.Named("species", species))

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

func (gdb *GexDB) AllTechnologies() (map[string]map[string][]string, error) {

	technologies := make(map[string]map[string][]string)

	rows, err := gdb.db.Query(AllTechnologiesSQL)

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

func (gdb *GexDB) Datasets(species string, technology string) ([]*Dataset, error) {

	datasetRows, err := gdb.db.Query(DatasetsSQL, sql.Named("species", species), sql.Named("technology", technology))

	if err != nil {
		return nil, err
	}

	defer datasetRows.Close()

	var id string
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

		log.Debug().Msgf("db %s", filepath.Join(gdb.dir, path))

		dsdb := NewDatasetDB(gdb.dir, path)

		defer dsdb.Close()

		dataset, err := dsdb.Dataset()

		if err != nil {
			return nil, err
		}

		datasets = append(datasets, dataset)
	}

	return datasets, nil
}

func (gdb *GexDB) DatasetCacheFromId(datasetId string) (*DatasetDB, error) {

	var id string
	var path string

	err := gdb.db.QueryRow(DatasetFromIdSQL, sql.Named("id", datasetId)).Scan(
		&id,
		&path)

	if err != nil {
		return nil, err
	}

	datasetCache := NewDatasetDB(gdb.dir, path)

	return datasetCache, nil
}

func (gdb *GexDB) ExprTypes(datasetIds []string) ([]*ExprType, error) {

	allExprTypes := make(map[string]*ExprType)

	for _, datasetId := range datasetIds {
		dsdb, err := gdb.DatasetCacheFromId(datasetId)

		if err != nil {
			return nil, err
		}

		defer dsdb.Close()

		exprTypes, err := dsdb.ExprTypes()

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

func (gdb *GexDB) FindSeqValues(datasetId string,
	exprType *ExprType,
	geneIds []string) (*SearchResults, error) {

	dsdb, err := gdb.DatasetCacheFromId(datasetId)

	if err != nil {
		log.Error().Msgf("error finding dataset cache from id %s: %v", datasetId, err)
		return nil, err
	}

	defer dsdb.Close()

	res, err := dsdb.FindSeqValues(exprType, geneIds)

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (gdb *GexDB) FindMicroarrayValues(datasetId string,
	geneIds []string) (*SearchResults, error) {

	dsdb, err := gdb.DatasetCacheFromId(datasetId)

	if err != nil {
		return nil, err
	}

	defer dsdb.Close()

	res, err := dsdb.FindMicroarrayValues(geneIds)

	if err != nil {
		return nil, err
	}

	return res, nil
}
