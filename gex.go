package gex

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-web/auth/sqlite"
)

type (
	GexGene struct {
		Id         string `json:"id"`
		Ensembl    string `json:"ensembl,omitempty"`
		Refseq     string `json:"refseq,omitempty"`
		GeneSymbol string `json:"geneSymbol"`
		Ncbi       string `json:"ncbi,omitempty"`
	}

	Probe struct {
		Key  int      `json:"-"`
		Id   string   `json:"id"`
		Name string   `json:"name"`
		Gene *GexGene `json:"gene,omitempty"`
	}

	Genome struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	}

	Technology struct {
		Name      string     `json:"name"`
		Id        string     `json:"id"`
		ExprTypes []ExprType `json:"exprTypes"`
	}

	GexDB struct {
		db   *sql.DB
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

	GenomesSql = `SELECT
		uuid,
		name
		FROM genomes
		ORDER BY genome`

	TechnologiesSQL = `SELECT
		datasets.platform
		FROM datasets
		JOIN genomes ON datasets.genome_id = genomes.id
		WHERE datasets.uuid = :id
		ORDER BY datasets.platform`

	AllTechnologiesSQL = `SELECT DISTINCT 
		species, technology, platform 
		FROM datasets 
		ORDER BY species, technology, platform`

	BaseDatasetsSQL = `SELECT 
		d.uuid,
		g.name AS genome,
		t.name AS technology,
		d.platform,
		d.institution,
		d.name,
		d.description
		FROM datasets d
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		JOIN genomes g ON d.genome_id = g.id
		JOIN technologies t ON d.technology_id = t.id
		WHERE
			<<PERMISSIONS>>
			AND d.genome_id = :gid 
			AND d.technology_id = :tid`

	DatasetsSQL = BaseDatasetsSQL + ` ORDER BY d.name`

	DatasetFromIdSQL = BaseDatasetsSQL + ` AND d.uuid = :id`

	ExprTypesSQL = `SELECT
		et.id,
		et.name
		FROM expr_types et
		JOIN expression ex ON et.id = ex.expression_type_id
		JOIN datasets d ON ex.dataset_id = d.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE
			<<PERMISSIONS>>
			AND <<DATASETS>>
		ORDER BY et.name`

	GeneSQL = `SELECT 
		g.uuid, 
		g.ensembl,
		g.refseq,
		g.ncbi,
		g.gene_symbol 
		FROM genes g
		WHERE
			g.uuid = :id OR
			g.ensembl = :id OR 
			g.refseq = :id OR
			g.gene_symbol LIKE :id
		LIMIT 1`

	ProbesSQL = `SELECT
		p.id AS probe_id,
		p.uuid AS probe_uuid,
		p.name AS probe_name,
		g.uuid AS gene_uuid, 
		g.ensembl,
		g.refseq,
		g.ncbi,
		g.gene_symbol 
		FROM genes g
		JOIN probes p ON g.id = p.gene_id
		WHERE
			g.uuid = :id OR
			g.ensembl = :id OR 
			g.refseq = :id OR
			g.gene_symbol LIKE :id`

	ExprSQL = `SELECT
		p.uuid,
		f.url,
		e.offset,
		e.length
		FROM expression e
		JOIN probes p ON e.probe_id = p.id
		JOIN files f ON e.file_id = f.id
		WHERE <<PROBES>>`

	GexTypeCounts string = "Counts"
	GexTypeTPM    string = "TPM"
	GexTypeVST    string = "VST"
	GexTypeRMA    string = "RMA"
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

func (gdb *GexDB) Genomes() ([]*Genome, error) {

	genomes := make([]*Genome, 0, 10)

	rows, err := gdb.db.Query(GenomesSql)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var genome Genome

		err := rows.Scan(
			&genome.Id,
			&genome.Name)

		if err != nil {
			return nil, err
		}

		genomes = append(genomes, &genome)
	}

	return genomes, nil
}

func (gdb *GexDB) Technologies(genome string) ([]string, error) {

	platforms := make([]string, 0, 10)

	rows, err := gdb.db.Query(TechnologiesSQL, sql.Named("id", genome))

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

func (gdb *GexDB) Datasets(genome string, technology string, permissions []string, isAdmin bool) ([]*Dataset, error) {

	namedArgs := []any{sql.Named("gid", genome), sql.Named("tid", technology)}

	query := sqlite.MakePermissionsSql(DatasetsSQL, isAdmin, permissions, &namedArgs)

	datasetRows, err := gdb.db.Query(query, namedArgs...)

	if err != nil {
		return nil, err
	}

	defer datasetRows.Close()

	datasets := make([]*Dataset, 0, 10)

	for datasetRows.Next() {
		var dataset Dataset

		err := datasetRows.Scan(
			&dataset.Id,
			&dataset.Genome)

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

// func (gdb *GexDB) DatasetCacheFromId(datasetId string) (*DatasetDB, error) {

// 	var id string
// 	var path string

// 	err := gdb.db.QueryRow(DatasetFromIdSQL, sql.Named("id", datasetId)).Scan(
// 		&id,
// 		&path)

// 	if err != nil {
// 		return nil, err
// 	}

// 	datasetCache := NewDatasetDB(gdb.dir, path)

// 	return datasetCache, nil
// }

func (gdb *GexDB) ExprTypes(datasets []string, isAdmin bool, permissions []string) ([]*ExprType, error) {

	namedArgs := []any{}

	query := sqlite.MakePermissionsSql(DatasetsSQL, isAdmin, permissions, &namedArgs)

	query = MakeInDatasetSql(query, datasets, &namedArgs)

	allExprTypes := make(map[string]*ExprType)

	rows, err := gdb.db.Query(query, namedArgs...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var exprType ExprType

		err := rows.Scan(
			&exprType.Id,
			&exprType.Name)

		if err != nil {
			return nil, err
		}

		if _, exists := allExprTypes[exprType.Id]; !exists {
			allExprTypes[exprType.Id] = &exprType
		}

	}

	ret := make([]*ExprType, 0, len(datasets))

	for _, exprType := range allExprTypes {
		ret = append(ret, exprType)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Name < ret[j].Name
	})

	return ret, nil
}

func (gdb *GexDB) FindGenes(genes []string) ([]*GexGene, error) {

	ret := make([]*GexGene, 0, len(genes))

	for _, g := range genes {
		var gene GexGene
		err := gdb.db.QueryRow(GeneSQL, sql.Named("id", g)).Scan(
			&gene.Id,
			&gene.Ensembl,
			&gene.Refseq,
			&gene.Ncbi,
			&gene.GeneSymbol)

		if err != nil {
			// log that we couldn't find a gene, but continue
			// anyway to find as many as possible
			log.Error().Msgf("gene not found: %s: %v", g, err)

			//return nil, err
			continue
		}

		ret = append(ret, &gene)
	}

	return ret, nil
}

func (gdb *GexDB) FindProbes(genes []string) ([]*Probe, error) {

	ret := make([]*Probe, 0, len(genes))

	for _, g := range genes {
		var probe Probe

		// init the gene
		probe.Gene = &GexGene{}

		err := gdb.db.QueryRow(ProbesSQL, sql.Named("id", g)).Scan(
			&probe.Key,
			&probe.Id,
			&probe.Name,
			&probe.Gene.Id,
			&probe.Gene.Ensembl,
			&probe.Gene.Refseq,
			&probe.Gene.Ncbi,
			&probe.Gene.GeneSymbol,
		)

		if err != nil {
			// log that we couldn't find a gene, but continue
			// anyway to find as many as possible
			log.Error().Msgf("gene not found: %s: %v", g, err)

			//return nil, err
			continue
		}

		ret = append(ret, &probe)
	}

	return ret, nil
}

// func (gdb *GexDB) FindSeqValues(datasetId string,
// 	exprType *ExprType,
// 	geneIds []string) (*SearchResults, error) {

// 	dsdb, err := gdb.DatasetCacheFromId(datasetId)

// 	if err != nil {
// 		log.Error().Msgf("error finding dataset cache from id %s: %v", datasetId, err)
// 		return nil, err
// 	}

// 	defer dsdb.Close()

// 	res, err := dsdb.FindSeqValues(exprType, geneIds)

// 	if err != nil {
// 		return nil, err
// 	}

// 	return res, nil
// }

// func (gdb *GexDB) FindMicroarrayValues(datasetId string,
// 	geneIds []string) (*SearchResults, error) {

// 	dsdb, err := gdb.DatasetCacheFromId(datasetId)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer dsdb.Close()

// 	res, err := dsdb.FindMicroarrayValues(geneIds)

// 	if err != nil {
// 		return nil, err
// 	}

// 	return res, nil
// }

// using binary blobs for expression values
func (gdb *GexDB) Expr(dataset string, exprType *ExprType, probes []*Probe) (*SearchResults, error) {

	ret := SearchResults{
		Dataset:  dataset,
		ExprType: exprType,
		Features: make([]*ResultFeature, 0, len(probes))}

	var id string
	var url string
	var offset int
	var length int

	var f float32

	namedArgs := make([]any, 0, len(probes))

	query := MakeInProbesSql(ExprSQL, probes, &namedArgs)

	for _, gene := range probes {

		err := gdb.db.QueryRow(query, namedArgs...).Scan(
			&id,
			&url,
			&offset,
			&length)

		if err != nil {
			return nil, err
		}

		path := filepath.Join(gdb.dir, url)

		f, err := os.Open(path)

		if err != nil {
			return nil, err
		}

		var total uint32
		binary.Read(io.NewSectionReader(f, offset, 4), binary.LittleEndian, &total)

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

func read(f *os.File, offset int, length int) ([]byte, error) {
	buf := make([]byte, length)
	_, err := f.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (gdb *GexDB) FindSeqValues(exprType *ExprType, geneIds []string) (*SearchResults, error) {

	genes, err := gdb.FindGenes(geneIds)

	if err != nil {
		return nil, err
	}

	return gdb.Expr(exprType, genes)
}

func (gdb *DatasetDB) FindMicroarrayValues(geneIds []string) (*SearchResults, error) {

	genes, err := gdb.FindGenes(geneIds)

	if err != nil {
		return nil, err
	}

	return gdb.Expr(ExprTypeRMA, genes)
}

func MakeInDatasetSql(query string, datasets []string, namedArgs *[]any) string {

	inPlaceholders := make([]string, len(datasets))

	for i, dataset := range datasets {
		ph := fmt.Sprintf("ds%d", i+1)
		inPlaceholders[i] = ":" + ph
		*namedArgs = append(*namedArgs, sql.Named(ph, dataset))
	}

	clause := "d.uuid IN (" + strings.Join(inPlaceholders, ",") + ")"

	return strings.Replace(query, "<<DATASETS>>", clause, 1)

}

func MakeInProbesSql(query string, probes []*Probe, namedArgs *[]any) string {

	inPlaceholders := make([]string, len(probes))

	for i, probe := range probes {
		ph := fmt.Sprintf("p%d", i+1)
		inPlaceholders[i] = ":" + ph
		*namedArgs = append(*namedArgs, sql.Named(ph, probe.Key))
	}

	clause := "e.probe_id IN (" + strings.Join(inPlaceholders, ",") + ")"

	return strings.Replace(query, "<<PROBES>>", clause, 1)

}
