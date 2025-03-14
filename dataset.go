package gex

import (
	"database/sql"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

const SAMPLES_SQL = `SELECT
	samples.id,
	samples.public_id,
	samples.name, 
	samples.alt_names 
	FROM samples
	ORDER BY samples.name`

const SAMPLE_DATA_SQL = `SELECT
	sample_data.name,
	sample_data.value
	FROM sample_data
	WHERE sample_data.sample_id = ?1
	ORDER by sample_data.name`

const GENE_SQL = `SELECT 
	genes.id, 
	genes.hugo_id, 
	genes.gene_symbol 
	FROM genes
	WHERE genes.gene_symbol LIKE ?1 OR genes.hugo_id = ?1 OR genes.ensembl_id LIKE ?1 OR genes.refseq_id LIKE ?1 
	LIMIT 1`

const RNA_SQL = `SELECT
	expression.id,
	expression.counts,
	expression.tpm,
	expression.vst
	FROM expression 
	WHERE expression.gene_id = ?1`

const MICROARRAY_SQL = `SELECT
	expression.id,
	expression.rma
	FROM expression 
	WHERE expression.gene_id = ?1`

const (
	GEX_TYPE_COUNTS string = "Counts"
	GEX_TYPE_TPM    string = "TPM"
	GEX_TYPE_VST    string = "VST"
	GEX_TYPE_RMA    string = "RMA"
)

type DatasetCache struct {
	dir     string
	dataset *Dataset
}

func NewDatasetCache(dir string, dataset *Dataset) *DatasetCache {
	return &DatasetCache{dir: dir, dataset: dataset}
}

func (cache *DatasetCache) FindGenes(genes []string) ([]*GexGene, error) {

	db, err := sql.Open("sqlite3", filepath.Join(cache.dir, cache.dataset.Path))

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := make([]*GexGene, 0, len(genes))

	for _, g := range genes {
		var gene GexGene
		err := db.QueryRow(GENE_SQL, g).Scan(
			&gene.Id,
			&gene.HugoId,
			&gene.GeneSymbol)

		if err != nil {
			return nil, err
		}

		ret = append(ret, &gene)
	}

	return ret, nil
}

func (cache *DatasetCache) FindRNASeqValues(gexType string,
	geneIds []string) (*SearchResults, error) {

	genes, err := cache.FindGenes(geneIds)

	if err != nil {
		return nil, err
	}

	log.Debug().Msgf("aha %v", genes[0])

	return cache.RNASeqValues(gexType, genes)
}

func (cache *DatasetCache) RNASeqValues(gexType string,
	genes []*GexGene) (*SearchResults, error) {

	log.Debug().Msgf("cripes %v", filepath.Join(cache.dir, cache.dataset.Path))

	db, err := sql.Open("sqlite3", filepath.Join(cache.dir, cache.dataset.Path))

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := SearchResults{

		Dataset: cache.dataset.PublicId,
		GexType: gexType,
		Genes:   make([]*ResultGene, 0, len(genes))}

	var id int
	var counts string
	var tpm string
	var vst string
	var gex string

	for _, gene := range genes {
		err := db.QueryRow(RNA_SQL, gene.Id).Scan(
			&id,
			&counts,
			&tpm,
			&vst)

		if err != nil {
			return nil, err
		}

		switch gexType {
		case GEX_TYPE_TPM:
			gex = tpm
		case GEX_TYPE_VST:
			gex = vst
		default:
			gex = counts
		}

		values := make([]float32, 0, DATASET_SIZE)

		for stringValue := range strings.SplitSeq(gex, ",") {
			value, err := strconv.ParseFloat(stringValue, 32)

			if err != nil {
				return nil, err
			}

			values = append(values, float32(value))
		}

		//log.Debug().Msgf("hmm %s %f %f", gexType, sample.Value, tpm)

		//datasetResults.Samples = append(datasetResults.Samples, &sample)
		ret.Genes = append(ret.Genes, &ResultGene{Gene: gene, Expression: values})

	}

	return &ret, nil
}

func (cache *DatasetCache) FindMicroarrayValues(
	geneIds []string) (*SearchResults, error) {

	genes, err := cache.FindGenes(geneIds)

	if err != nil {
		return nil, err
	}

	return cache.MicroarrayValues(genes)
}

func (cache *DatasetCache) MicroarrayValues(

	genes []*GexGene) (*SearchResults, error) {

	db, err := sql.Open("sqlite3", filepath.Join(cache.dir, cache.dataset.Path))

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := SearchResults{
		Dataset: cache.dataset.PublicId,
		GexType: "rma",
		Genes:   make([]*ResultGene, 0, len(genes))}

	var id int
	var counts string
	var rma string

	for _, gene := range genes {
		err := db.QueryRow(MICROARRAY_SQL, gene.Id).Scan(
			&id,
			&counts,
			&rma)

		if err != nil {
			return nil, err
		}

		values := make([]float32, 0, DATASET_SIZE)

		for stringValue := range strings.SplitSeq(rma, ",") {
			value, err := strconv.ParseFloat(stringValue, 32)

			if err != nil {
				return nil, err
			}

			values = append(values, float32(value))
		}

		//datasetResults.Samples = append(datasetResults.Samples, &sample)
		ret.Genes = append(ret.Genes, &ResultGene{Gene: gene, Expression: values})

	}

	return &ret, nil
}
