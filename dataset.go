package gex

import (
	"database/sql"
	"path/filepath"

	"github.com/rs/zerolog/log"

	"github.com/antonybholmes/go-sys"
)

type (
	DatasetCache struct {
		dataset *Dataset
		dir     string
	}

	ExprType struct {
		Id       uint   `json:"id"`
		PublicId string `json:"publicId"`
		Name     string `json:"name"`
	}
)

// keep them in the entered order so we can preserve
// groupings such as N/GC/M which are not alphabetical
const (
	SamplesSQL = `SELECT
	samples.id,
	samples.public_id,
	samples.name
	FROM samples
	ORDER BY samples.id`

	SampleAltNamesSQL = `SELECT
	sample_alt_names.name,
	sample_alt_names.value
	FROM sample_alt_names
	WHERE sample_alt_names.sample_id = ?1
	ORDER by sample_alt_names.id`

	SampleMetadataSQL = `SELECT
	sample_metadata.name,
	sample_metadata.value
	FROM sample_metadata
	WHERE sample_metadata.sample_id = ?1
	ORDER by sample_metadata.id`

	GeneSQL = `SELECT 
	genes.id, 
	genes.hugo_id,
	genes.mgi_id,
	genes.ensembl_id,
	genes.refseq_id,
	genes.gene_symbol 
	FROM genes
	WHERE genes.gene_symbol LIKE ?1 OR 
	genes.hugo_id = ?1 OR 
	genes.ensembl_id LIKE ?1 OR 
	genes.refseq_id LIKE ?1 
	LIMIT 1`

	ExprTypesSQL = `SELECT
	expr_types.id,
	expr_types.public_id,
	expr_types.name
	FROM expr_types
	ORDER BY expr_types.id`

	ExpressionSQL = `SELECT
	expression.id,
	expression.sample_id,
	expression.gene_id,
	expression.probe_id,
	expression.value
	FROM expression 
	WHERE expression.gene_id = ?1 AND
	expression.expr_type_id = ?2
	ORDER BY expression.sample_id`

	// MICROARRAY_SQL = `SELECT
	// expression.id,
	// expression.feature_id AS probe_id,
	// expression.value
	// FROM expression
	// WHERE expression.gene_id = ?1 AND
	// expression.expr_type = 'rma'`

	GexTypeCounts string = "Counts"
	GexTypeTPM    string = "TPM"
	GexTypeVST    string = "VST"
	GexTypeRMA    string = "RMA"
)

var (
	ExprTypeRMA = ExprType{Id: 1, PublicId: "00000000-0000-0000-0000-000000000001", Name: GexTypeRMA}
)

func NewDatasetCache(dir string, dataset *Dataset) *DatasetCache {
	return &DatasetCache{dir: dir, dataset: dataset}
}

// func (cache *DatasetCache) Samples() ([]*Sample, error) {

// 	db, err := sql.Open("sqlite3", filepath.Join(cache.dir, cache.dataset.Path))

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer db.Close()

// 	rows, err := db.Query(SAMPLES_SQL)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer rows.Close()

// 	ret := make([]*Sample, 0, DATASET_SIZE)

// 	for rows.Next() {
// 		var sample Sample

// 		err := rows.Scan(
// 			&sample.Id,
// 			&sample.PublicId,
// 			&sample.Name)

// 		if err != nil {
// 			return nil, err
// 		}

// 		// get alt names
// 		altRows, err := db.Query(SAMPLE_ALT_NAMES_SQL, sample.Id)

// 		if err != nil {
// 			return nil, err
// 		}

// 		sample.AltNames = make([]NameValueType, 0, 10)

// 		for altRows.Next() {
// 			var nv NameValueType

// 			err := altRows.Scan(&nv.Name, &nv.Value)

// 			if err != nil {
// 				return nil, err
// 			}

// 			sample.AltNames = append(sample.AltNames, nv)
// 		}

// 		altRows.Close()

// 		// get metadata
// 		metaRows, err := db.Query(SAMPLE_METADATA_SQL, sample.Id)

// 		if err != nil {
// 			return nil, err
// 		}

// 		sample.Metadata = make([]NameValueType, 0, 10)

// 		for metaRows.Next() {
// 			var nv = NameValueType{}

// 			err := metaRows.Scan(&nv.Name, &nv.Value)

// 			if err != nil {
// 				return nil, err
// 			}

// 			sample.Metadata = append(sample.Metadata, nv)
// 		}

// 		metaRows.Close()

// 		ret = append(ret, &sample)
// 	}

// 	return ret, nil
// }

func (cache *DatasetCache) ExprTypes() ([]*ExprType, error) {

	db, err := sql.Open("sqlite3", filepath.Join(cache.dir, cache.dataset.Path))

	if err != nil {
		return nil, err
	}

	defer db.Close()

	rows, err := db.Query(ExprTypesSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	ret := make([]*ExprType, 0, 10)

	for rows.Next() {
		var exprType ExprType

		err := rows.Scan(
			&exprType.Id,
			&exprType.PublicId,
			&exprType.Name)

		if err != nil {
			return nil, err
		}

		ret = append(ret, &exprType)
	}

	return ret, nil
}

func (cache *DatasetCache) FindGenes(genes []string) ([]*GexGene, error) {

	db, err := sql.Open(sys.Sqlite3DB, filepath.Join(cache.dir, cache.dataset.Path))

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := make([]*GexGene, 0, len(genes))

	for _, g := range genes {
		var gene GexGene
		err := db.QueryRow(GeneSQL, g).Scan(
			&gene.Id,
			&gene.Hugo,
			&gene.Mgi,
			&gene.Ensembl,
			&gene.Refseq,
			&gene.GeneSymbol)

		if err == nil {
			// add as many genes as possible
			ret = append(ret, &gene)
		} else {
			// log that we couldn't find a gene, but continue
			// anyway
			log.Debug().Msgf("gene not found: %s", g)
			//return nil, err
		}
	}

	return ret, nil
}

func (cache *DatasetCache) FindSeqValues(exprType ExprType,
	geneIds []string) (*SearchResults, error) {

	genes, err := cache.FindGenes(geneIds)

	if err != nil {
		return nil, err
	}

	return cache.GexValues(exprType, genes)
}

func (cache *DatasetCache) GexValues(exprType ExprType,
	genes []*GexGene) (*SearchResults, error) {

	db, err := sql.Open("sqlite3", filepath.Join(cache.dir, cache.dataset.Path))

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := SearchResults{
		Dataset:  cache.dataset.PublicId,
		GexType:  exprType.Name,
		Features: make([]*ResultFeature, 0, len(genes))}

	var id uint
	var sampleId uint
	var geneId uint
	var probeId sql.NullString
	var value float32

	for _, gene := range genes {

		rows, err := db.Query(ExpressionSQL, gene.Id, exprType.Id)

		if err != nil {
			return nil, err
		}

		defer rows.Close()

		// to store expression values for each sample
		var values = make([]float32, 0, len(cache.dataset.Samples))

		for rows.Next() {

			err := rows.Scan(&id,
				&sampleId,
				&geneId,
				&probeId,
				&value)

			if err != nil {
				return nil, err
			}

			values = append(values, value)

			//log.Debug().Msgf("hmm %s %f %f", gexType, sample.Value, tpm)
		}

		feature := ResultFeature{Gene: gene, Expression: values}

		if probeId.Valid {
			feature.ProbeId = &probeId.String
		}

		log.Debug().Msgf("got %d values for gene %s", len(values), gene.GeneSymbol)

		ret.Features = append(ret.Features, &feature)

	}

	return &ret, nil
}

func (cache *DatasetCache) FindMicroarrayValues(
	geneIds []string) (*SearchResults, error) {

	genes, err := cache.FindGenes(geneIds)

	if err != nil {
		return nil, err
	}

	return cache.GexValues(ExprTypeRMA, genes)
}

// func (cache *DatasetCache) MicroarrayValues(

// 	genes []*GexGene) (*SearchResults, error) {

// 	db, err := sql.Open("sqlite3", filepath.Join(cache.dir, cache.dataset.Path))

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer db.Close()

// 	ret := SearchResults{
// 		Dataset:  cache.dataset.PublicId,
// 		GexType:  "rma",
// 		Features: make([]*ResultFeature, 0, len(genes))}

// 	var id int
// 	var probeId string
// 	var rma string

// 	for _, gene := range genes {
// 		err := db.QueryRow(MICROARRAY_SQL, gene.Id).Scan(
// 			&id,
// 			&probeId,
// 			&rma)

// 		if err != nil {
// 			return nil, err
// 		}

// 		values := make([]float32, 0, DATASET_SIZE)

// 		for stringValue := range strings.SplitSeq(rma, ",") {
// 			value, err := strconv.ParseFloat(stringValue, 32)

// 			if err != nil {
// 				return nil, err
// 			}

// 			values = append(values, float32(value))
// 		}

// 		//datasetResults.Samples = append(datasetResults.Samples, &sample)
// 		ret.Features = append(ret.Features, &ResultFeature{ProbeId: probeId, Gene: gene, Expression: values})

// 	}

// 	return &ret, nil
// }
