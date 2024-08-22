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

const PLATFORMS_SQL = `SELECT
	platforms.id,
	platforms.name 
	FROM platforms 
	ORDER BY platforms.id`

const VALUE_TYPES_SQL = `SELECT 
	gex_value_types.id,
	gex_value_types.name
	FROM gex_value_types 
	WHERE gex_value_types.platform_id = ?1
	ORDER BY gex_value_types.id`

const DATASETS_SQL = `SELECT 
	datasets.id, 
	datasets.name, 
	datasets.institution 
	FROM datasets 
	WHERE datasets.platform_id = ?1
	ORDER BY datasets.id`

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

const MICROARRAY_SQL = `SELECT 
	microarray.sample_id,
	microarray.rma
	FROM microarray 
	WHERE microarray.gene_id = ?1 AND microarray.dataset_id = ?2`

// type GexValueType string

const (
	GEX_VALUE_TYPE_COUNTS string = "Counts"
	GEX_VALUE_TYPE_TPM    string = "TPM"
	GEX_VALUE_TYPE_VST    string = "VST"
	GEX_VALUE_TYPE_RMA    string = "RMA"
)

type ValueType struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

type GexValueType = ValueType

// type GexType string

// const (
// 	GEX_TYPE_RNA_SEQ        GexType = "RNA-seq"
// 	GEX_TYPE_RNA_MICROARRAY GexType = "Microarray"
//)

type GexGene struct {
	Id         int    `json:"id"`
	GeneId     string `json:"geneId"`
	GeneSymbol string `json:"geneSymbol"`
}

type Platform = ValueType

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

// type RNASeqGex struct {
// 	Dataset int     `json:"dataset"`
// 	Sample  int     `json:"sample"`
// 	Gene    int     `json:"gene"`
// 	Counts  int     `json:"counts"`
// 	TPM     float32 `json:"tpm"`
// 	VST     float32 `json:"vst"`
// }

// type MicroarrayGex struct {
// 	Dataset int     `json:"dataset"`
// 	Sample  int     `json:"sample"`
// 	Gene    int     `json:"gene"`
// 	RMA     float32 `json:"vst"`
// }

type ResultSample struct {
	//Dataset int     `json:"dataset"`
	Id int `json:"id"`
	//Gene    int     `json:"gene"`
	//Counts int     `json:"counts"`
	////TPM    float32 `json:"tpm"`
	//VST    float32 `json:"vst"`
	Value float32 `json:"value"`
}

type ResultDataset struct {
	Id int `json:"id"`

	Samples []*ResultSample `json:"samples"`
}

type ResultGene struct {
	Gene         *GexGene         `json:"gene"`
	Platform     *Platform        `json:"platform"`
	GexValueType *GexValueType    `json:"gexValueType"`
	Datasets     []*ResultDataset `json:"datasets"`
}

type DatasetCache struct {
	dir string
}

func NewGexDBCache(dir string) *DatasetCache {

	path := filepath.Join(dir, "gex.db")

	// db, err := sql.Open("sqlite3", path)

	// if err != nil {
	// 	log.Fatal().Msgf("%s", err)
	// }

	// defer db.Close()

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

func (cache *DatasetCache) Plaforms() ([]*Platform, error) {
	db, err := sql.Open("sqlite3", cache.dir)

	if err != nil {
		log.Debug().Msgf("err 1 %s", err)
		return nil, err
	}

	defer db.Close()

	gexTypes := make([]*Platform, 0, 10)

	rows, err := db.Query(PLATFORMS_SQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var gexType Platform

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

func (cache *DatasetCache) GexValueTypes(platform *Platform) ([]*GexValueType, error) {

	db, err := sql.Open("sqlite3", cache.dir)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	valueTypes := make([]*GexValueType, 0, 10)

	rows, err := db.Query(VALUE_TYPES_SQL, platform.Id)

	if err != nil {
		log.Debug().Msgf("err 1 %s", err)
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var valueType GexValueType

		err := rows.Scan(
			&valueType.Id,
			&valueType.Name)

		if err != nil {
			log.Debug().Msgf("err 2 %s", err)
			return nil, err
		}

		valueTypes = append(valueTypes, &valueType)
	}

	return valueTypes, nil
}

func (cache *DatasetCache) Datasets(platform *Platform) ([]*Dataset, error) {

	db, err := sql.Open("sqlite3", cache.dir)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	datasets := make([]*Dataset, 0, 10)

	datasetRows, err := db.Query(DATASETS_SQL, platform.Id)

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

func (cache *DatasetCache) RNASeqValues(genes []*GexGene, platform *Platform, gexValueType *GexValueType, datasets []int) ([]*ResultGene, error) {

	db, err := sql.Open("sqlite3", cache.dir)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := make([]*ResultGene, 0, len(genes))

	for _, gene := range genes {
		geneResults := ResultGene{Gene: gene,
			Platform:     platform,
			GexValueType: gexValueType,
			Datasets:     make([]*ResultDataset, 0, len(datasets))}

		for _, dataset := range datasets {
			var datasetResults ResultDataset

			datasetResults.Id = dataset
			datasetResults.Samples = make([]*ResultSample, 0, DATASET_SIZE)

			sampleRows, err := db.Query(RNA_SQL, gene.Id, dataset)

			if err != nil {
				log.Debug().Msgf("err 3 %s", err)
				return nil, err
			}

			defer sampleRows.Close()

			var counts int
			var tpm float32
			var vst float32

			for sampleRows.Next() {
				var sample ResultSample

				err := sampleRows.Scan(
					&sample.Id,
					&counts,
					&tpm,
					&vst)

				switch gexValueType.Name {
				case GEX_VALUE_TYPE_TPM:
					sample.Value = tpm
				case GEX_VALUE_TYPE_VST:
					sample.Value = vst
				default:
					sample.Value = float32(counts)
				}

				//log.Debug().Msgf("hmm %s %f %f", gexValueType, sample.Value, tpm)

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

func (cache *DatasetCache) MicroarrayValues(genes []*GexGene, platform *Platform, gexValueType *GexValueType, datasets []int) ([]*ResultGene, error) {

	db, err := sql.Open("sqlite3", cache.dir)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := make([]*ResultGene, 0, len(genes))

	for _, gene := range genes {
		geneResults := ResultGene{Gene: gene,
			Platform:     platform,
			GexValueType: gexValueType,
			Datasets:     make([]*ResultDataset, 0, len(datasets))}

		for _, dataset := range datasets {
			var datasetResults ResultDataset

			datasetResults.Id = dataset
			datasetResults.Samples = make([]*ResultSample, 0, DATASET_SIZE)

			sampleRows, err := db.Query(MICROARRAY_SQL, gene.Id, dataset)

			if err != nil {
				log.Debug().Msgf("err 3 %s", err)
				return nil, err
			}

			defer sampleRows.Close()

			for sampleRows.Next() {
				var sample ResultSample

				err := sampleRows.Scan(
					&sample.Id,
					&sample.Value)

				//log.Debug().Msgf("hmm %s %f %f", gexValueType, sample.Value, tpm)

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
