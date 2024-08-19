package gex

import (
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/rs/zerolog/log"
)

// approx size of dataset
const DATASET_SIZE = 500

const GENES_SQL = `SELECT 
	genes.id, 
	genes.gene_id, 
	genes.gene_symbol 
	FROM genes 
	ORDER BY genes.gene_symbol`

const GENE_SQL = `SELECT 
	genes.id, 
	genes.gene_id, 
	genes.gene_symbol 
	FROM genes
	WHERE genes.gene_id LIKE ?1 OR genes.gene_symbol LIKE ?1 
	LIMIT 1`

const GEX_TYPES_SQL = `SELECT 
	gex_types.id, 
	gex_types.name 
	FROM gex_types 
	ORDER BY gex_types.name`

const DATASETS_SQL = `SELECT 
	datasets.id, 
	datasets.name, 
	datasets.institution 
	FROM datasets 
	WHERE datasets.gex_type_id = ?1
	ORDER BY datasets.name`

// const DATASETS_SQL = `SELECT
// 	name
// 	FROM datasets
// 	ORDER BY datasets.name`

const SAMPLES_SQL = `SELECT
	samples.id,
	samples.name, 
	samples.coo, 
	samples.lymphgen
	FROM samples
	WHERE samples.dataset_id = ?1
	ORDER BY samples.name`

const RNA_SQL = `SELECT 
	rna_seq.sample_id,
	rna_seq.counts,
	rna_seq.tpm,
	rna_seq.vst
	FROM rna_seq 
	WHERE rna_seq.gene_id = ?1 AND rna_seq.dataset_id = ?2`

type GexGene struct {
	Id         int    `json:"id"`
	GeneId     string `json:"geneId"`
	GeneSymbol string `json:"geneSymbol"`
}

type GexType struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

type Sample struct {
	Id       int    `json:"id"`
	Name     string `json:"name"`
	COO      string `json:"coo"`
	Lymphgen string `json:"lymphgen"`
	//Dataset  int    `json:"dataset"`
}

type Dataset struct {
	Id          int    `json:"id"`
	Name        string `json:"name"`
	Institution string `json:"institution"`
	//GexType     *GexType  `json:"gexType"`
	Samples []*Sample `json:"samples"`

	//db                *sql.DB
	//findMutationsStmt *sql.Stmt
}

type RNASeqGex struct {
	Dataset int     `json:"dataset"`
	Sample  int     `json:"sample"`
	Gene    int     `json:"gene"`
	Counts  int     `json:"counts"`
	TPM     float32 `json:"tpm"`
	VST     float32 `json:"vst"`
}

type MicroarrayGex struct {
	Dataset int     `json:"dataset"`
	Sample  int     `json:"sample"`
	Gene    int     `json:"gene"`
	RMA     float32 `json:"vst"`
}

type RNASeqSampleResults struct {
	//Dataset int     `json:"dataset"`
	Id int `json:"id"`
	//Gene    int     `json:"gene"`
	Counts int     `json:"counts"`
	TPM    float32 `json:"tpm"`
	VST    float32 `json:"vst"`
}

type RNASeqDatasetResults struct {
	Id int `json:"id"`

	Samples []*RNASeqSampleResults `json:"samples"`
}

type RNASeqGeneResults struct {
	Gene     *GexGene                `json:"gene"`
	Datasets []*RNASeqDatasetResults `json:"datasets"`
}

type DatasetCache struct {
	dir string
}

func NewGexDBCache(dir string) *DatasetCache {

	path := filepath.Join(dir, "gex.db")

	db, err := sql.Open("sqlite3", path)

	if err != nil {
		log.Fatal().Msgf("%s", err)
	}

	defer db.Close()

	return &DatasetCache{path}
}

func (cache *DatasetCache) Dir() string {
	return cache.dir
}

func (cache *DatasetCache) GetGenes(genes []string) ([]*GexGene, error) {
	db, err := sql.Open("sqlite3", cache.dir)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := make([]*GexGene, 0, len(genes))

	for _, gene := range genes {
		var gexGene GexGene

		err := db.QueryRow(GENE_SQL, fmt.Sprintf("%%%s%%", gene)).Scan(&gexGene.Id, &gexGene.GeneId, &gexGene.GeneSymbol)

		if err == nil {
			ret = append(ret, &gexGene)
		}
	}

	return ret, nil
}

func (cache *DatasetCache) GexTypes() ([]*GexType, error) {
	db, err := sql.Open("sqlite3", cache.dir)

	if err != nil {
		log.Debug().Msgf("err 1 %s", err)
		return nil, err
	}

	defer db.Close()

	gexTypes := make([]*GexType, 0, 10)

	rows, err := db.Query(GEX_TYPES_SQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var gexType GexType

		err := rows.Scan(
			&gexType.Id,
			&gexType.Name)

		if err != nil {
			return nil, err
		}

		gexTypes = append(gexTypes, &gexType)
	}

	return gexTypes, nil
}

func (cache *DatasetCache) Datasets(gexType int) ([]*Dataset, error) {

	db, err := sql.Open("sqlite3", cache.dir)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	datasets := make([]*Dataset, 0, 10)

	datasetRows, err := db.Query(DATASETS_SQL, gexType)

	if err != nil {
		log.Debug().Msgf("err 1 %s", err)
		return nil, err
	}

	defer datasetRows.Close()

	for datasetRows.Next() {
		var dataset Dataset

		err := datasetRows.Scan(
			&dataset.Id,
			&dataset.Name,
			&dataset.Institution)

		if err != nil {
			log.Debug().Msgf("err 2 %s", err)
			return nil, err
		}

		// the largest dataset is around 500 samples
		// so use that as an estimate
		dataset.Samples = make([]*Sample, 0, DATASET_SIZE)

		sampleRows, err := db.Query(SAMPLES_SQL, dataset.Id)

		if err != nil {
			log.Debug().Msgf("err 3 %s %d", err, dataset.Id)
			return nil, err
		}

		defer sampleRows.Close()

		for sampleRows.Next() {
			var sample Sample

			err := sampleRows.Scan(
				&sample.Id,
				&sample.Name,
				&sample.COO,
				&sample.Lymphgen)

			if err != nil {
				log.Debug().Msgf("err 5 %s", err)
				return nil, err
			}

			dataset.Samples = append(dataset.Samples, &sample)
		}

		datasets = append(datasets, &dataset)
	}

	return datasets, nil
}

func (cache *DatasetCache) RNASeqValues(genes []*GexGene, datasets []int) ([]*RNASeqGeneResults, error) {

	db, err := sql.Open("sqlite3", cache.dir)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := make([]*RNASeqGeneResults, 0, len(genes))

	for _, gene := range genes {
		var geneResults RNASeqGeneResults

		geneResults.Gene = gene

		geneResults.Datasets = make([]*RNASeqDatasetResults, 0, len(datasets))

		for _, dataset := range datasets {
			var datasetResults RNASeqDatasetResults

			datasetResults.Id = dataset
			datasetResults.Samples = make([]*RNASeqSampleResults, 0, DATASET_SIZE)

			sampleRows, err := db.Query(RNA_SQL, gene.Id, dataset)

			if err != nil {
				log.Debug().Msgf("err 3 %s", err)
				return nil, err
			}

			defer sampleRows.Close()

			for sampleRows.Next() {
				var sample RNASeqSampleResults

				err := sampleRows.Scan(
					&sample.Id,
					&sample.Counts,
					&sample.TPM,
					&sample.TPM)

				if err != nil {
					log.Debug().Msgf("err 5 %s", err)
					return nil, err
				}

				datasetResults.Samples = append(datasetResults.Samples, &sample)
			}

			geneResults.Datasets = append(geneResults.Datasets, &datasetResults)
		}

		ret = append(ret, &geneResults)
	}

	return ret, nil
}

// func (cache *DatasetCache) Search(location *dna.Location, uuids []string) (*SearchResults, error) {
// 	results := SearchResults{Location: location, DatasetResults: make([]*DatasetResults, 0, len(uuids))}

// 	for _, uuid := range uuids {
// 		dataset, err := cache.GetDataset(uuid)

// 		if err != nil {
// 			return nil, err
// 		}

// 		datasetResults, err := dataset.Search(location)

// 		if err != nil {
// 			return nil, err
// 		}

// 		results.DatasetResults = append(results.DatasetResults, datasetResults)
// 	}

// 	return &results, nil
// }
