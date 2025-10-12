package gex

import (
	"database/sql"
	"path/filepath"

	"github.com/rs/zerolog/log"

	"github.com/antonybholmes/go-sys"
)

type (
	SampleCache struct {
		// dataset that this cache is for
		dataset *Dataset
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
	}

	Sample struct {
		PublicId string          `json:"publicId"`
		Name     string          `json:"name"`
		AltNames []NameValueType `json:"altNames"`
		Metadata []NameValueType `json:"metadata"`
		Id       int             `json:"-"`
	}

	ExprType struct {
		Id       uint   `json:"id"`
		PublicId string `json:"publicId"`
		Name     string `json:"name"`
	}

	GexGene struct {
		Ensembl    string `json:"ensembl"`
		Refseq     string `json:"refseq"`
		Hugo       string `json:"hugo"`
		Mgi        string `json:"mgi"`
		GeneSymbol string `json:"geneSymbol"`
		Id         int    `json:"-"`
	}

	SearchResults struct {
		// we use the simpler value type for platform in search
		// results so that the value types are not repeated in
		// each search. The useful info in a search is just
		// the platform name and id

		//Dataset *Dataset      `json:"dataset"`
		Dataset  string           `json:"dataset"`
		ExprType string           `json:"exprType"`
		Features []*ResultFeature `json:"features"`
	}

	// Either a probe or gene
	ResultFeature struct {
		ProbeId *string  `json:"probeId,omitempty"` // distinguish between null and ""
		Gene    *GexGene `json:"gene"`
		//Platform     *ValueType       `json:"platform"`
		//GexValue *GexValue    `json:"gexType"`
		Expression []float32 `json:"expression"`
	}
)

// keep them in the entered order so we can preserve
// groupings such as N/GC/M which are not alphabetical
const (
	DefaultNumSamples = 500

	SamplesSQL = `SELECT
		samples.id,
		samples.public_id,
		samples.name
		FROM samples
		ORDER BY samples.id`

	SampleAltNamesSQL = `SELECT
		sample_alt_names.id,
		sample_alt_names.sample_id,
		sample_alt_names.name,
		sample_alt_names.value
		FROM sample_alt_names
		ORDER by sample_alt_names.sample_id, sample_alt_names.id`

	SampleMetadataSQL = `SELECT
		sample_metadata.id,
		sample_metadata.sample_id,
		sample_metadata.name,
		sample_metadata.value
		FROM sample_metadata
		ORDER by sample_metadata.sample_id, sample_metadata.id`

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

	GexTypeCounts string = "Counts"
	GexTypeTPM    string = "TPM"
	GexTypeVST    string = "VST"
	GexTypeRMA    string = "RMA"
)

var (
	ExprTypeRMA = &ExprType{Id: 1, PublicId: sys.BlankUUID, Name: GexTypeRMA}
)

func NewSampleCache(dir string, dataset *Dataset) *SampleCache {
	return &SampleCache{dir: dir, dataset: dataset, db: filepath.Join(dir, dataset.Path)}
}

func (cache *SampleCache) Dir() string {
	return cache.dir
}

func (cache *SampleCache) Dataset() *Dataset {
	return cache.dataset
}

func (cache *SampleCache) Samples() ([]*Sample, error) {

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
			&sample.PublicId,
			&sample.Name)

		if err != nil {
			return nil, err
		}

		// initialize alt names and metadata slices
		// to avoid nil slices
		// we can estimate the size to avoid too many allocations
		sample.AltNames = make([]NameValueType, 0, 10)
		sample.Metadata = make([]NameValueType, 0, 10)

		samples = append(samples, &sample)
	}

	var id uint
	var sampleId uint

	// add sample alt names to samples

	rows, err = db.Query(SampleAltNamesSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var nv = NameValueType{}

		err := rows.Scan(&id, &sampleId, &nv.Name, &nv.Value)

		if err != nil {
			return nil, err
		}

		// samples are ordered by sample id starting at 1 so
		// we can use sampleId - 1 as the index
		// otherwise we would need a map
		// which would be less efficient
		index := sampleId - 1

		samples[index].AltNames = append(samples[index].AltNames, nv)
	}

	// add sample metadata to samples

	rows, err = db.Query(SampleMetadataSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var nv = NameValueType{}

		err := rows.Scan(&id, &sampleId, &nv.Name, &nv.Value)

		if err != nil {
			return nil, err
		}

		index := sampleId - 1

		samples[index].Metadata = append(samples[index].Metadata, nv)
	}

	return samples, nil
}

func (cache *SampleCache) ExprTypes() ([]*ExprType, error) {

	db, err := sql.Open(sys.Sqlite3DB, cache.db)

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

// FindGenes looks up genes by their gene symbol, hugo id, ensembl id or refseq id
// since expr values are stored by gene id
func (cache *SampleCache) FindGenes(genes []string) ([]*GexGene, error) {

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
			&gene.Hugo,
			&gene.Mgi,
			&gene.Ensembl,
			&gene.Refseq,
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

func (cache *SampleCache) FindSeqValues(exprType *ExprType,
	geneIds []string) (*SearchResults, error) {

	genes, err := cache.FindGenes(geneIds)

	if err != nil {
		return nil, err
	}

	return cache.Expr(exprType, genes)
}

func (cache *SampleCache) Expr(exprType *ExprType,
	genes []*GexGene) (*SearchResults, error) {

	db, err := sql.Open(sys.Sqlite3DB, cache.db)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := SearchResults{
		Dataset:  cache.dataset.PublicId,
		ExprType: exprType.Name,
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

func (cache *SampleCache) FindMicroarrayValues(
	geneIds []string) (*SearchResults, error) {

	genes, err := cache.FindGenes(geneIds)

	if err != nil {
		return nil, err
	}

	return cache.Expr(ExprTypeRMA, genes)
}
