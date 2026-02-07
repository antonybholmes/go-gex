package gex

import (
	"database/sql"
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
		Genome     string `json:"genome"`
		Technology string `json:"technology"`
		Platform   string `json:"platform"`

		Institution string      `json:"institution"`
		Description string      `json:"description"`
		Samples     []*Sample   `json:"samples"`
		ExprTypes   []*ExprType `json:"exprTypes"`
	}

	DatasetDB struct {
		db   *sql.DB
		dir  string
		path string
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

	// DatasetSQL = `SELECT
	// 	dataset.id,
	// 	dataset.species,
	// 	dataset.technology,
	// 	dataset.platform,
	// 	dataset.institution,
	// 	dataset.name,
	// 	dataset.description
	// 	FROM dataset`

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

)

var (
	ExprTypeRMA = &ExprType{Id: sys.BlankUUID, Name: GexTypeRMA}
)

func NewDatasetDB(dir string, path string) *DatasetDB {
	path = filepath.Join(dir, path)

	return &DatasetDB{dir: dir, path: path, db: sys.Must(sql.Open(sys.Sqlite3DB, path))}
}

func (gdb *DatasetDB) Close() error {
	return gdb.db.Close()
}

// func (dsdb *DatasetDB) Dataset() (*Dataset, error) {

// 	var dataset Dataset

// 	err := dsdb.db.QueryRow(DatasetSQL).Scan(
// 		&dataset.Id,
// 		&dataset.Species,
// 		&dataset.Technology,
// 		&dataset.Platform,
// 		&dataset.Institution,
// 		&dataset.Name,
// 		&dataset.Description)

// 	if err != nil {
// 		return nil, err
// 	}

// 	dataset.Samples, err = dsdb.Samples()

// 	if err != nil {
// 		return nil, err
// 	}

// 	dataset.ExprTypes, err = dsdb.ExprTypes()

// 	if err != nil {
// 		return nil, err
// 	}

// 	return &dataset, nil
// }

// func (dsdb *DatasetDB) ExprTypes() ([]*ExprType, error) {

// 	rows, err := dsdb.db.Query(ExprTypesSQL)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer rows.Close()

// 	exprTypes := make([]*ExprType, 0, 5)

// 	for rows.Next() {
// 		var exprType ExprType

// 		err := rows.Scan(
// 			&exprType.Id,
// 			&exprType.Name)

// 		if err != nil {
// 			return nil, err
// 		}

// 		exprTypes = append(exprTypes, &exprType)
// 	}

// 	return exprTypes, nil
// }

func (gdb *DatasetDB) Metadata() ([]*Metadata, error) {

	rows, err := gdb.db.Query(MetadataSQL)

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

	return metadata, nil
}

func (gdb *DatasetDB) Dir() string {
	return gdb.dir
}

func (gdb *DatasetDB) Samples() ([]*Sample, error) {

	rows, err := gdb.db.Query(SamplesSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	samples := make([]*Sample, 0, DefaultNumSamples)
	sampleMap := make(map[string]*Sample)

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
		sampleMap[sample.Id] = &sample
	}

	var id string
	var sampleId string

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

	rows, err = gdb.db.Query(SampleMetadataSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var nv = NameValueType{}

		err := rows.Scan(&id, &sampleId, &nv.Name, &nv.Value, &nv.Color)

		if err != nil {
			log.Error().Msgf("error scanning sample: %v", err)
			return nil, err
		}

		sampleMap[sampleId].Metadata = append(sampleMap[sampleId].Metadata, &nv)
	}

	return samples, nil
}

// FindGenes looks up genes by their gene symbol, hugo id, ensembl id or refseq id
// since expr values are stored by gene id

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
