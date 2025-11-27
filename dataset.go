package gex

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"path/filepath"

	"github.com/antonybholmes/go-sys/log"

	"github.com/antonybholmes/go-sys"
)

type (
	ExprType struct {
		Name string `json:"name"`
		Id   string `json:"id"`
	}

	Dataset struct {
		Id         string `json:"id"`
		Name       string `json:"name"`
		Species    string `json:"species"`
		Technology string `json:"technology"`
		Platform   string `json:"platform"`

		Institution string      `json:"institution"`
		Description string      `json:"description"`
		Samples     []*Sample   `json:"samples"`
		ExprTypes   []*ExprType `json:"exprTypes"`
	}

	DatasetCache struct {
		// directory where the sqlite db files are stored
		dir string
		// full path to the sqlite db file
		db string
	}

	Idtype struct {
		Name string `json:"name"`
		Id   int    `json:"id"`
	}

	NameValueType struct {
		Name  string `json:"name"`
		Value string `json:"value"`
		Color string `json:"color,omitempty"`
	}

	Metadata struct {
		Id          string `json:"id"`
		Name        string `json:"name"`
		Value       string `json:"value"`
		Description string `json:"description,omitempty"`
		Color       string `json:"color,omitempty"`
	}

	Sample struct {
		Id   string `json:"id"`
		Name string `json:"name"`
		//AltNames []NameValueType `json:"altNames"`
		Metadata []*NameValueType `json:"metadata"`
	}

	GexGene struct {
		Ensembl string `json:"ensembl,omitempty"`
		Refseq  string `json:"refseq,omitempty"`
		//Hugo       string `json:"hugo,omitempty"`
		//Mgi        string `json:"mgi,omitempty"`
		GeneSymbol string `json:"geneSymbol"`
		Ncbi       int    `json:"ncbi,omitempty"`
		Id         string `json:"id"` // hugo or mgi
	}

	SearchResults struct {
		// we use the simpler value type for platform in search
		// results so that the value types are not repeated in
		// each search. The useful info in a search is just
		// the platform name and id

		//Dataset *Dataset      `json:"dataset"`
		Dataset  string           `json:"dataset"`
		ExprType *ExprType        `json:"exprType"`
		Features []*ResultFeature `json:"features"`
	}

	// Either a probe or gene
	ResultFeature struct {
		ProbeId *string  `json:"probeId,omitempty"` // distinguish between null and ""
		Gene    *GexGene `json:"gene"`
		//Platform     *ValueType       `json:"platform"`
		//GexValue *GexValue    `json:"gexType"`
		Expr []float32 `json:"expr"`
	}
)

// keep them in the entered order so we can preserve
// groupings such as N/GC/M which are not alphabetical
const (
	DefaultNumSamples = 500

	DatasetSQL = `SELECT 
		dataset.id,
		dataset.species,
		dataset.technology,
		dataset.platform,
		dataset.institution,
		dataset.name,
		dataset.description
		FROM dataset`

	SamplesSQL = `SELECT
		samples.id,
		samples.name
		FROM samples
		ORDER BY samples.id`

	// SampleAltNamesSQL = `SELECT
	// 	sample_alt_names.id,
	// 	sample_alt_names.sample_id,
	// 	sample_alt_names.name,
	// 	sample_alt_names.value
	// 	FROM sample_alt_names
	// 	ORDER by sample_alt_names.sample_id, sample_alt_names.id`

	MetadataSQL = `SELECT
		metadata.id,
		metadata_types.name,
		metadata.value,
		metadata.decription,
		metadata.color
		FROM metadata
		JOIN metadata_types ON metadata.metadata_type_id = metadata_types.id
		ORDER BY metadata_types.id, metadata.id`

	SampleMetadataSQL = `SELECT
		sample_metadata.id,
		sample_metadata.sample_id,
		metadata_types.name,
		metadata.value,
		metadata.color
		FROM sample_metadata
		JOIN metadata ON sample_metadata.metadata_id = metadata.id
		JOIN metadata_types ON metadata.metadata_type_id = metadata_types.id
		ORDER by sample_metadata.sample_id, metadata_types.id, metadata.id`

	GeneSQL = `SELECT 
		genes.id, 
		genes.ensembl,
		genes.refseq,
		genes.ncbi,
		genes.gene_symbol 
		FROM genes
		WHERE genes.gene_symbol LIKE ?1 OR 
		genes.hugo = ?1 OR 
		genes.ensembl LIKE ?1 OR 
		genes.refseq LIKE ?1 
		LIMIT 1`

	// ExprSQL = `SELECT
	// 	expr.id,
	// 	expr.sample_id,
	// 	expr.gene_id,
	// 	expr.probe_id,
	// 	expr.value
	// 	FROM expr
	// 	WHERE expr.gene_id = ?1 AND
	// 	expr.expr_type_id = ?2
	// 	ORDER BY expr.sample_id`

	// for expr values stored as binary blobs
	ExprSQL = `SELECT
		expr.id,
		expr.gene_id,
		expr.probe_id,
		data
		FROM expr 
		WHERE expr.gene_id = ?1 AND
		expr.expr_type_id = ?2`

	ExprTypesSQL = `SELECT
		expr_types.id,
		expr_types.public_id,
		expr_types.name
		FROM expr_types
		ORDER BY expr_types.id`

	GexTypeCounts string = "Counts"
	GexTypeTPM    string = "TPM"
	GexTypeVST    string = "VST"
	GexTypeRMA    string = "RMA"
)

var (
	ExprTypeRMA = &ExprType{Id: sys.BlankUUID, Name: GexTypeRMA}
)

func NewDatasetCache(dir string, path string) *DatasetCache {

	return &DatasetCache{dir: dir, db: filepath.Join(dir, path)}
}

func (cache *DatasetCache) Dataset() (*Dataset, error) {

	db, err := sql.Open(sys.Sqlite3DB, cache.db)

	if err != nil {
		return nil, err
	}

	var dataset Dataset

	err = db.QueryRow(DatasetSQL).Scan(
		&dataset.Id,
		&dataset.Species,
		&dataset.Technology,
		&dataset.Platform,
		&dataset.Institution,
		&dataset.Name,
		&dataset.Description)

	if err != nil {
		return nil, err
	}

	dataset.Samples, err = cache.Samples()

	if err != nil {
		return nil, err
	}

	dataset.ExprTypes, err = cache.ExprTypes()

	if err != nil {
		return nil, err
	}

	return &dataset, nil
}

func (cache *DatasetCache) ExprTypes() ([]*ExprType, error) {
	db, err := sql.Open(sys.Sqlite3DB, cache.db)

	if err != nil {
		return nil, err
	}

	rows, err := db.Query(ExprTypesSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	exprTypes := make([]*ExprType, 0, 5)

	for rows.Next() {
		var exprType ExprType

		err := rows.Scan(
			&exprType.Id,
			&exprType.Name)

		if err != nil {
			return nil, err
		}

		exprTypes = append(exprTypes, &exprType)
	}

	db.Close()

	return exprTypes, nil
}

func (cache *DatasetCache) Metadata() ([]*Metadata, error) {
	db, err := sql.Open(sys.Sqlite3DB, cache.db)

	if err != nil {
		return nil, err
	}

	rows, err := db.Query(MetadataSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	metadata := make([]*Metadata, 0, 20)

	for rows.Next() {
		var m Metadata

		err := rows.Scan(
			&m.Id,
			&m.Name,
			&m.Value,
			&m.Description,
			&m.Color)

		if err != nil {
			return nil, err
		}

		metadata = append(metadata, &m)
	}

	db.Close()

	return metadata, nil
}

func (cache *DatasetCache) Dir() string {
	return cache.dir
}

func (cache *DatasetCache) Samples() ([]*Sample, error) {

	db, err := sql.Open(sys.Sqlite3DB, cache.db)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	rows, err := db.Query(SamplesSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	samples := make([]*Sample, 0, DefaultNumSamples)

	for rows.Next() {
		var sample Sample

		err := rows.Scan(
			&sample.Id,
			&sample.Name)

		if err != nil {
			return nil, err
		}

		// initialize alt names and metadata slices
		// to avoid nil slices
		// we can estimate the size to avoid too many allocations
		//sample.AltNames = make([]NameValueType, 0, 10)
		sample.Metadata = make([]*NameValueType, 0, 10)

		samples = append(samples, &sample)
	}

	var id int
	var sampleId int

	// add sample alt names to samples

	// rows, err = db.Query(SampleAltNamesSQL)

	// if err != nil {
	// 	return nil, err
	// }

	// defer rows.Close()

	// for rows.Next() {
	// 	var nv = NameValueType{}

	// 	err := rows.Scan(&id, &sampleId, &nv.Name, &nv.Value)

	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	// samples are ordered by sample id starting at 1 so
	// 	// we can use sampleId - 1 as the index
	// 	// otherwise we would need a map
	// 	// which would be less efficient
	// 	index := sampleId - 1

	// 	samples[index].AltNames = append(samples[index].AltNames, nv)
	// }

	// add sample metadata to samples

	rows, err = db.Query(SampleMetadataSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var nv = NameValueType{}

		err := rows.Scan(&id, &sampleId, &nv.Name, &nv.Value, &nv.Color)

		if err != nil {
			return nil, err
		}

		index := sampleId - 1

		samples[index].Metadata = append(samples[index].Metadata, &nv)
	}

	return samples, nil
}

// FindGenes looks up genes by their gene symbol, hugo id, ensembl id or refseq id
// since expr values are stored by gene id
func (cache *DatasetCache) FindGenes(genes []string) ([]*GexGene, error) {

	db, err := sql.Open(sys.Sqlite3DB, cache.db)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := make([]*GexGene, 0, len(genes))

	for _, g := range genes {
		var gene GexGene
		err := db.QueryRow(GeneSQL, g).Scan(
			&gene.Id,
			&gene.Ensembl,
			&gene.Refseq,
			&gene.Ncbi,
			&gene.GeneSymbol)

		if err != nil {
			// log that we couldn't find a gene, but continue
			// anyway to find as many as possible
			log.Info().Msgf("gene not found: %s", g)

			//return nil, err
			continue
		}

		ret = append(ret, &gene)
	}

	return ret, nil
}

func (cache *DatasetCache) FindSeqValues(exprType *ExprType, geneIds []string) (*SearchResults, error) {

	genes, err := cache.FindGenes(geneIds)

	if err != nil {
		return nil, err
	}

	return cache.Expr(exprType, genes)
}

// func (cache *DatasetCache) Expr(exprType *ExprType, genes []*GexGene) (*SearchResults, error) {

// 	dataset, err := cache.Dataset()

// 	if err != nil {
// 		return nil, err
// 	}

// 	samples, err := cache.Samples()

// 	if err != nil {
// 		return nil, err
// 	}

// 	db, err := sql.Open(sys.Sqlite3DB, cache.db)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer db.Close()

// 	ret := SearchResults{
// 		Dataset:  dataset.PublicId,
// 		ExprType: exprType,
// 		Features: make([]*ResultFeature, 0, len(genes))}

// 	var id int
// 	var sampleId int
// 	var geneId int
// 	var probeId sql.NullString
// 	var value float32

// 	for _, gene := range genes {

// 		rows, err := db.Query(ExprSQL, gene.Id, exprType.Id)

// 		if err != nil {
// 			return nil, err
// 		}

// 		defer rows.Close()

// 		// to store expression values for each sample
// 		var values = make([]float32, 0, len(samples))

// 		for rows.Next() {

// 			err := rows.Scan(&id,
// 				&sampleId,
// 				&geneId,
// 				&probeId,
// 				&value)

// 			if err != nil {
// 				return nil, err
// 			}

// 			values = append(values, value)

// 			//log.Debug().Msgf("hmm %s %f %f", gexType, sample.Value, tpm)
// 		}

// 		feature := ResultFeature{Gene: gene, Expr: values}

// 		if probeId.Valid {
// 			feature.ProbeId = &probeId.String
// 		}

// 		log.Debug().Msgf("got %d values for gene %s", len(values), gene.GeneSymbol)

// 		ret.Features = append(ret.Features, &feature)
// 	}

// 	return &ret, nil
// }

// using binary blobs for expression values
func (cache *DatasetCache) Expr(exprType *ExprType, genes []*GexGene) (*SearchResults, error) {

	dataset, err := cache.Dataset()

	if err != nil {
		return nil, err
	}

	samples, err := cache.Samples()

	if err != nil {
		return nil, err
	}

	db, err := sql.Open(sys.Sqlite3DB, cache.db)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := SearchResults{
		Dataset:  dataset.Id,
		ExprType: exprType,
		Features: make([]*ResultFeature, 0, len(genes))}

	var id int
	var geneId int
	var probeId sql.NullString
	var blob []byte
	var f float32

	for _, gene := range genes {

		err := db.QueryRow(ExprSQL, gene.Id, exprType.Id).Scan(
			&id,
			&geneId,
			&probeId,
			&blob)

		if err != nil {
			return nil, err
		}

		buf := bytes.NewReader(blob)

		// to store expression values for each sample
		// Samples are expected to be in the same order as the values
		// in the blob
		var values = make([]float32, 0, len(samples))

		//for buf.Len() > 0 {
		for range samples {
			if err := binary.Read(buf, binary.LittleEndian, &f); err != nil {
				return nil, err
			}
			values = append(values, f)
		}

		feature := ResultFeature{Gene: gene, Expr: values}

		if probeId.Valid {
			feature.ProbeId = &probeId.String
		}

		log.Debug().Msgf("got %d values for gene %s", len(values), gene.GeneSymbol)

		ret.Features = append(ret.Features, &feature)
	}

	return &ret, nil
}

func (cache *DatasetCache) FindMicroarrayValues(geneIds []string) (*SearchResults, error) {

	genes, err := cache.FindGenes(geneIds)

	if err != nil {
		return nil, err
	}

	return cache.Expr(ExprTypeRMA, genes)
}
