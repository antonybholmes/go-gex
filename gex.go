package gex

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-sys/collections"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-web"
	"github.com/antonybholmes/go-web/auth/sqlite"
)

type (
	GexGene struct {
		GeneId     string `json:"geneId"`
		GeneSymbol string `json:"geneSymbol"`
		Ensembl    string `json:"ensembl,omitempty"`
		Refseq     string `json:"refseq,omitempty"`
		Ncbi       string `json:"ncbi,omitempty"`
		sys.IdEntity
	}

	Probe struct {
		Gene *GexGene `json:"gene,omitempty"`
		sys.Entity
	}

	// Technology struct {
	// 	sys.Entity
	// 	ExprTypes []sys.Entity `json:"exprTypes"`
	// }

	NamedValue struct {
		sys.Entity
		Value string `json:"value"`
		Color string `json:"color,omitempty"`
	}

	// Metadata struct {
	// 	Name  string `json:"name"`
	// 	Value string `json:"value"`
	// 	Color string `json:"color,omitempty"`
	// 	//Description string `json:"description,omitempty"`
	// }

	Dataset struct {
		sys.Entity
		Genome      *sys.Entity `json:"genome"`
		Technology  *sys.Entity `json:"technology"`
		Platform    string      `json:"platform,omitempty"`
		Institution string      `json:"institution"`
		Samples     []*Sample   `json:"samples,omitempty"`
		//Metadata    []string    `json:"metadata,omitempty"`
		ExprTypes []*sys.Entity `json:"exprTypes,omitempty"`
	}

	Sample struct {
		sys.Entity
		//AltNames []NameValueType `json:"altNames"`
		Metadata []*NamedValue `json:"metadata"`
	}

	SearchResults struct {
		// we use the simpler value type for platform in search
		// results so that the value types are not repeated in
		// each search. The useful info in a search is just
		// the platform name and id

		Dataset  *sys.Entity        `json:"dataset"`
		ExprType *sys.Entity        `json:"type"`
		Probes   []*ExpressionProbe `json:"probes"`
	}

	// Either a probe or gene
	ExpressionProbe struct {
		Probe *Probe `json:"probe"` // distinguish between null and ""
		//Gene  *GexGene `json:"gene"`
		//Platform     *ValueType       `json:"platform"`
		//GexValue *GexValue    `json:"gexType"`
		Values []float32 `json:"values"`
	}

	GexDB struct {
		db  *sql.DB
		dir string
	}
)

const (
	DefaultNumSamples = 500
	MaxDatasets       = 10
	MaxProbes         = 200

	GexTypeCounts = "Counts"
	GexTypeTPM    = "TPM"
	GexTypeVST    = "VST"
	GexTypeRMA    = "RMA"

	GenesSql = `SELECT 
		g.public_id, 
		g.gene_id, 
		g.gene_symbol 
		FROM genes g
		ORDER BY g.gene_symbol`

	GenomesSql = `SELECT
		g.id,
		g.public_id,
		g.name
		FROM genomes g
		ORDER BY g.name`

	TechnologiesSQL = `SELECT
		t.id,
		t.public_id,
		t.name
		FROM technologies t
		ORDER BY t.name`

	// AllTechnologiesSQL = `SELECT DISTINCT
	// 	species,
	// 	technology,
	// 	platform
	// 	FROM datasets
	// 	ORDER BY species, technology, platform`

	GenomeTechnologySQL = `SELECT 
		g.id AS genome_id,
		g.public_id AS genome_public_id,
		g.name AS genome_name,
		t.id AS technology_id,
		t.public_id AS technology_public_id,
		t.name AS technology_name
		FROM datasets d
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		JOIN genomes g ON d.genome_id = g.id
		JOIN technologies t ON d.technology_id = t.id
		WHERE
			d.public_id = :id`

	BaseDatasetsSQL = `SELECT 
		d.id,
		d.public_id,
		d.name,
		d.platform,
		d.institution,
		g.id AS genome_id,
		g.public_id AS genome_public_id,
		g.name AS genome_name,
		t.id AS technology_id,
		t.public_id AS technology_public_id,
		t.name AS technology_name,
		s.id AS sample_id,
		s.public_id AS sample_public_id,
		s.name AS sample_name,
		m.name AS metadata_name,
		smd.value AS metadata_value,
		m.color AS metadata_color
		FROM datasets d
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		JOIN genomes g ON d.genome_id = g.id
		JOIN technologies t ON d.technology_id = t.id
		JOIN samples s ON s.dataset_id = d.id
		JOIN sample_metadata smd ON smd.sample_id = s.id
		JOIN metadata m ON smd.metadata_id = m.id
		WHERE
			<<PERMISSIONS>>`

	// DatasetsSQL = BaseDatasetsSQL +
	// 	` AND d.genome_id = :gid
	// 	AND d.technology_id = :tid
	// 	ORDER BY d.name, s.name, m.name`

	DatasetsSQL = BaseDatasetsSQL +
		` AND LOWER(g.name) = :genome 
		AND LOWER(t.name) = :technology
		ORDER BY d.name, s.name, m.name`

	DatasetFromIdSQL = BaseDatasetsSQL + ` AND d.public_id = :id`

	BasicDatasetSQL = `SELECT
		d.id,
		d.public_id,
		d.name
		FROM datasets d
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE
			<<PERMISSIONS>>
			AND d.public_id = :id`

	SamplesSQL = `SELECT
		samples.id,
		samples.name
		FROM samples
		ORDER BY samples.id`

	MetadataSQL = `SELECT
		m.id,
		m.public_id,
		m.name,
		m.color
		FROM metadata m
		ORDER BY m.name`

	// order by sample id and then sample_metadata.id to ensure consistent order of metadata for each sample
	// as it was read from its original source file
	SampleMetadataSQL = `SELECT
		s.id AS sample_id,
		m.id AS metadata_id,
		m.public_id,
		m.name,
		smd.value,
		m.color
		FROM sample_metadata smd
		JOIN metadata m ON smd.metadata_id = m.id
		JOIN samples s ON smd.sample_id = s.id
		ORDER by s.id, smd.id`

	// ExprTypesSQL = `SELECT DISTINCT
	// 	e.id,
	// 	e.public_id,
	// 	e.name
	// 	FROM expr_types e
	// 	JOIN Probes ex ON e.Probes_type_id = ex.Probes_type_id
	// 	JOIN datasets d ON ex.dataset_id = d.id
	// 	JOIN dataset_permissions dp ON d.id = dp.dataset_id
	// 	JOIN permissions p ON dp.permission_id = p.id
	// 	WHERE
	// 		<<PERMISSIONS>>
	// 		AND <<DATASETS>>
	// 	ORDER BY e.name`

	ExprTypesSQL = `SELECT DISTINCT
		e.id,
		e.public_id,
		e.name
		FROM expression_types e
		JOIN expression ex ON e.id = ex.expression_type_id
		JOIN datasets d ON ex.dataset_id = d.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE
			d.id = :id
		ORDER BY e.name`

	ExprTypeSQL = `SELECT
		e.id,
		e.public_id,
		e.name
		FROM expression_types e
		WHERE
			e.public_id = :id
			OR LOWER(e.name) = :id
		LIMIT 1`

	// GeneSQL = `SELECT
	// 	g.public_id,
	// 	g.ensembl,
	// 	g.refseq,
	// 	g.ncbi,
	// 	g.gene_symbol
	// 	FROM genes g
	// 	WHERE
	// 		g.public_id = :id OR
	// 		LOWER(g.ensembl) = :id OR
	// 		LOWER(g.refseq) = :id OR
	// 		LOWER(g.gene_symbol) LIKE :id
	// 	LIMIT 1`

	CreateIdTableSQL = `CREATE TEMP TABLE IF NOT EXISTS ids (
        id TEXT NOT NULL UNIQUE,
        ord INTEGER NOT NULL
    )`

	InsertIdSQL = `INSERT INTO ids (id, ord) VALUES (:id, :ord) ON CONFLICT(id) DO NOTHING`

	ProbesSQL = `SELECT DISTINCT
		t.probe_id,
		t.probe_public_id,
		t.probe_name,
		t.gid, 
		t.gene_public_id, 
		t.gene_id,
		t.gene_symbol,
		t.ensembl,
		t.refseq,
		t.ncbi
		FROM (
			SELECT DISTINCT
			p.id AS probe_id,
			p.public_id AS probe_public_id,
			p.name AS probe_name,
			ge.id AS gid, 
			ge.public_id AS gene_public_id, 
			ge.gene_id AS gene_id,
			ge.gene_symbol AS gene_symbol,
			ge.ensembl,
			ge.refseq,
			ge.ncbi,
			ids.ord
			FROM probes p
			JOIN genomes g ON g.id = p.genome_id
			JOIN technologies t ON t.id = p.technology_id
			JOIN genes ge ON ge.id = p.gene_id
			JOIN ids ON (
				p.public_id = ids.id
				OR LOWER(p.name) LIKE ids.id
				OR ge.public_id = ids.id
				OR LOWER(ge.gene_symbol) LIKE ids.id
				OR LOWER(ge.ensembl) = ids.id
				OR LOWER(ge.refseq) = ids.id
			)
			WHERE
				g.id = :genome
				AND t.id = :technology
		) t
		ORDER BY t.ord`

	// ProbeIdsSQL = `SELECT DISTINCT
	// 	p.id AS probe_id,
	// 	p.public_id AS probe_public_id,
	// 	p.name AS probe_name
	// 	FROM probes p
	// 	JOIN genes g ON g.id = p.gene_id
	// 	JOIN ids i ON (
	// 		p.public_id = i.id
	// 		OR p.name LIKE i.id
	// 		OR g.public_id = i.id
	// 		OR g.gene_symbol LIKE i.id
	// 		OR g.ensembl = i.id
	// 		OR g.refseq = i.id
	// 	)
	// 	ORDER BY i.ord`

	// ExprSQL = `SELECT
	// 	p.id,
	// 	p.public_id,
	// 	p.name,
	// 	f.url,
	// 	e.offset
	// 	FROM Probes e
	// 	JOIN datasets d ON ex.dataset_id = e.id
	// 	JOIN dataset_permissions dp ON d.id = dp.dataset_id
	// 	JOIN probes p ON e.probe_id = p.id
	// 	JOIN files f ON e.file_id = f.id
	// 	WHERE
	// 		<<PERMISSIONS>>
	// 		AND <<PROBES>>
	// 		AND e.Probes_type_id = :expr_type
	// 		AND d.public_id = :dataset
	// 	ORDER BY p.name`

	ExprSQL = `SELECT
		f.url,
		e.offset,
		e.length
		FROM expression e
		JOIN datasets d ON d.id = e.dataset_id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		JOIN files f ON e.file_id = f.id
		WHERE 
			<<PERMISSIONS>>
			AND e.probe_id = :probe
			AND e.expression_type_id = :type
			AND d.public_id = :dataset`
)

func NewGexDB(dir string) *GexDB {

	path := filepath.Join(dir, "gex.db"+sys.SqliteReadOnlySuffix)

	// db, err := sql.Open("sqlite3", path)

	// if err != nil {
	// 	log.Fatal().Msgf("%s", err)
	// }

	// defer db.Close()

	return &GexDB{dir: dir, db: sys.Must(sql.Open(sys.Sqlite3DB, path))}
}

func (gdb *GexDB) Close() error {
	return gdb.db.Close()
}

func (gdb *GexDB) Dir() string {
	return gdb.dir
}

func (gdb *GexDB) Genomes() ([]*sys.Entity, error) {

	genomes := make([]*sys.Entity, 0, 10)

	rows, err := gdb.db.Query(GenomesSql)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var genome sys.Entity

		err := rows.Scan(
			&genome.Id,
			&genome.PublicId,
			&genome.Name)

		if err != nil {
			return nil, err
		}

		genomes = append(genomes, &genome)
	}

	return genomes, nil
}

func (gdb *GexDB) Technologies() ([]*sys.Entity, error) {

	technologies := make([]*sys.Entity, 0, 10)

	rows, err := gdb.db.Query(TechnologiesSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var technology sys.Entity

		err := rows.Scan(
			&technology.Id,
			&technology.PublicId,
			&technology.Name)

		if err != nil {
			return nil, err
		}

		technologies = append(technologies, &technology)
	}

	return technologies, nil
}

// func (gdb *GexDB) Technologies(genome string) ([]string, error) {

// 	platforms := make([]string, 0, 10)

// 	rows, err := gdb.db.Query(TechnologiesSQL, sql.Named("id", genome))

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer rows.Close()

// 	for rows.Next() {
// 		var platform string

// 		err := rows.Scan(
// 			&platform)

// 		if err != nil {
// 			return nil, err
// 		}

// 		platforms = append(platforms, platform)
// 	}

// 	return platforms, nil
// }

// func (gdb *GexDB) AllTechnologies() (map[string]map[string][]string, error) {

// 	technologies := make(map[string]map[string][]string)

// 	rows, err := gdb.db.Query(AllTechnologiesSQL)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer rows.Close()

// 	var species string
// 	var technology string
// 	var platform string

// 	for rows.Next() {

// 		err := rows.Scan(&species,
// 			&technology,
// 			&platform)

// 		if err != nil {
// 			return nil, err
// 		}

// 		if technologies[species] == nil {
// 			technologies[species] = make(map[string][]string)
// 		}

// 		if technologies[species][technology] == nil {
// 			technologies[species][technology] = make([]string, 0, 10)
// 		}

// 		technologies[species][technology] = append(technologies[species][technology], platform)

// 	}

// 	return technologies, nil
// }

func (gdb *GexDB) Datasets(genome string,
	technology string,
	permissions []string,
	isAdmin bool) ([]*Dataset, error) {

	namedArgs := []any{sql.Named("genome", web.FormatParam(genome)),
		sql.Named("technology", web.FormatParam(technology))}

	log.Debug().Msgf("Query: %s, Args: %v", DatasetsSQL, namedArgs)

	query := sqlite.MakePermissionsSql(DatasetsSQL, isAdmin, permissions, &namedArgs)

	rows, err := gdb.db.Query(query, namedArgs...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	datasets := make([]*Dataset, 0, 10)

	var currentDataset *Dataset
	var currentSample *Sample

	for rows.Next() {
		var dataset Dataset
		var genome sys.Entity
		var technology sys.Entity
		var sample Sample
		var metadata NamedValue

		//dataset.Genome = &sys.Entity{}
		//dataset.Technology = &sys.Entity{}
		//dataset.Samples = make([]*Sample, 0, 10)

		err := rows.Scan(
			&dataset.Id,
			&dataset.PublicId,
			&dataset.Name,
			&dataset.Platform,
			&dataset.Institution,
			&genome.Id,
			&genome.PublicId,
			&genome.Name,
			&technology.Id,
			&technology.PublicId,
			&technology.Name,
			&sample.Id,
			&sample.PublicId,
			&sample.Name,
			&metadata.Name,
			&metadata.Value,
			&metadata.Color)

		if err != nil {
			return nil, err
		}

		if currentDataset == nil || currentDataset.Id != dataset.Id {
			currentDataset = &dataset
			currentDataset.Genome = &genome
			currentDataset.Technology = &technology
			currentDataset.Samples = make([]*Sample, 0, 20)

			datasets = append(datasets, &dataset)
		}

		if currentSample == nil || currentSample.Id != sample.Id {
			currentSample = &sample
			currentSample.Metadata = make([]*NamedValue, 0, 20)
			currentDataset.Samples = append(currentDataset.Samples, currentSample)
		}

		currentSample.Metadata = append(currentSample.Metadata, &metadata)
	}

	// Add expr types

	for _, dataset := range datasets {
		dataset.ExprTypes = make([]*sys.Entity, 0, 5)

		query = sqlite.MakePermissionsSql(ExprTypesSQL, isAdmin, permissions, &namedArgs)

		rows, err = gdb.db.Query(ExprTypesSQL, sql.Named("id", dataset.Id))

		for rows.Next() {
			var exprType sys.Entity

			err := rows.Scan(&exprType.Id, &exprType.PublicId, &exprType.Name)

			if err != nil {
				return nil, err
			}

			dataset.ExprTypes = append(dataset.ExprTypes, &exprType)
		}
	}

	return datasets, nil
}

// used for search results where only basic dataset info is needed
func (gdb *GexDB) BasicDataset(datasetId string, permissions []string, isAdmin bool) (*sys.Entity, error) {

	namedArgs := []any{sql.Named("id", datasetId)}

	query := sqlite.MakePermissionsSql(BasicDatasetSQL, isAdmin, permissions, &namedArgs)

	var ret sys.Entity

	err := gdb.db.QueryRow(query, namedArgs...).Scan(
		&ret.Id,
		&ret.PublicId,
		&ret.Name)

	if err != nil {
		return nil, err
	}

	return &ret, nil
}

func (gdb *GexDB) Metadata() ([]*NamedValue, error) {

	rows, err := gdb.db.Query(MetadataSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	metadata := make([]*NamedValue, 0, 20)

	for rows.Next() {
		var m NamedValue

		err := rows.Scan(
			&m.Id,
			&m.PublicId,
			&m.Name,
			&m.Value,
			&m.Color)

		if err != nil {
			return nil, err
		}

		metadata = append(metadata, &m)
	}

	return metadata, nil
}

func (gdb *GexDB) Samples() ([]*Sample, error) {

	rows, err := gdb.db.Query(SamplesSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	samples := make([]*Sample, 0, DefaultNumSamples)
	sampleMap := make(map[int]*Sample)

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
		sample.Metadata = make([]*NamedValue, 0, 10)

		samples = append(samples, &sample)
		sampleMap[sample.Id] = &sample
	}

	// add sample metadata to samples

	rows, err = gdb.db.Query(SampleMetadataSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var sampleId int

	for rows.Next() {
		var m NamedValue

		err := rows.Scan(&sampleId, &m.Id, &m.PublicId, &m.Name, &m.Value, &m.Color)

		if err != nil {
			return nil, err
		}

		sampleMap[sampleId].Metadata = append(sampleMap[sampleId].Metadata, &m)
	}

	return samples, nil
}

func (gdb *GexDB) ExprType(id string) (*sys.Entity, error) {

	var ret sys.Entity

	err := gdb.db.QueryRow(ExprTypeSQL, sql.Named("id", web.FormatParam(id))).Scan(
		&ret.Id,
		&ret.PublicId,
		&ret.Name)

	if err != nil {
		return nil, err
	}

	return &ret, nil
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

// func (gdb *GexDB) ExprTypes(datasetIds []string, isAdmin bool, permissions []string) ([]*sys.Entity, error) {

// 	namedArgs := []any{}

// 	query := sqlite.MakePermissionsSql(ExprTypesSQL, isAdmin, permissions, &namedArgs)

// 	query = MakeInDatasetsSql(query, datasetIds, &namedArgs)

// 	allExprTypes := make(map[string]*sys.Entity)

// 	rows, err := gdb.db.Query(query, namedArgs...)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer rows.Close()

// 	for rows.Next() {
// 		var exprType sys.Entity

// 		err := rows.Scan(
// 			&exprType.PublicId,
// 			&exprType.Name)

// 		if err != nil {
// 			return nil, err
// 		}

// 		if _, exists := allExprTypes[exprType.PublicId]; !exists {
// 			allExprTypes[exprType.PublicId] = &exprType
// 		}
// 	}

// 	ret := make([]*sys.Entity, 0, len(datasetIds))

// 	for _, exprType := range allExprTypes {
// 		ret = append(ret, exprType)
// 	}

// 	sort.Slice(ret, func(i, j int) bool {
// 		return ret[i].Name < ret[j].Name
// 	})

// 	return ret, nil
// }

// func (gdb *GexDB) FindGenes(genes []string) ([]*GexGene, error) {

// 	ret := make([]*GexGene, 0, len(genes))

// 	for _, g := range genes {
// 		var gene GexGene
// 		err := gdb.db.QueryRow(GeneSQL, sql.Named("id", g)).Scan(
// 			&gene.PublicId,
// 			&gene.Ensembl,
// 			&gene.Refseq,
// 			&gene.Ncbi,
// 			&gene.Name)

// 		if err != nil {
// 			// log that we couldn't find a gene, but continue
// 			// anyway to find as many as possible
// 			log.Error().Msgf("gene not found: %s: %v", g, err)

// 			//return nil, err
// 			continue
// 		}

// 		ret = append(ret, &gene)
// 	}

// 	return ret, nil
// }

func (gdb *GexDB) GenomeTechnology(datasetId string) (*sys.Entity, *sys.Entity, error) {

	var genome sys.Entity
	var technology sys.Entity

	err := gdb.db.QueryRow(GenomeTechnologySQL, sql.Named("id", datasetId)).Scan(
		&genome.Id,
		&genome.PublicId,
		&genome.Name,
		&technology.Id,
		&technology.PublicId,
		&technology.Name)

	if err != nil {
		return nil, nil, err
	}

	return &genome, &technology, nil
}

func (gdb *GexDB) FindProbes(genome, technology *sys.Entity, genes []string) ([]*Probe, error) {

	// use a transaction to insert gene ids into a temp table

	tx, err := gdb.db.BeginTx(context.Background(), &sql.TxOptions{
		ReadOnly: false,
	})

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	_, err = tx.Exec(CreateIdTableSQL)

	if err != nil {
		return nil, err
	}

	_, err = tx.Exec(`DELETE FROM ids`)
	if err != nil {
		return nil, err
	}

	stmt, err := tx.Prepare(InsertIdSQL)

	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	for i, id := range genes {
		if _, err := stmt.Exec(sql.Named("id", web.FormatParam(id)), sql.Named("ord", i+1)); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	//
	// Join the ids with the probes table to find
	// matching probes whilst maintaining the gene
	// order
	//

	ret := make([]*Probe, 0, len(genes))

	rows, err := gdb.db.Query(ProbesSQL,
		sql.Named("genome", genome.Id), sql.Named("technology", technology.Id))

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var probe Probe

		// init the gene
		probe.Gene = &GexGene{}

		err := rows.Scan(
			&probe.Id,
			&probe.PublicId,
			&probe.Name,
			&probe.Gene.Id,
			&probe.Gene.PublicId,
			&probe.Gene.GeneId,
			&probe.Gene.GeneSymbol,
			&probe.Gene.Ensembl,
			&probe.Gene.Refseq,
			&probe.Gene.Ncbi,
		)

		if err != nil {
			return nil, err
		}

		ret = append(ret, &probe)
	}

	// for _, g := range ret {
	// 	log.Debug().Msgf("probe %v", *g)
	// }

	return ret, nil
}

// func (gdb *GexDB) FindProbes(genes []string) ([]*Idtype, error) {

// 	ret := make([]*Idtype, 0, len(genes))

// 	patternsSql, args := MakeOrderdPatternsClause(genes)

// 	query := strings.Replace(ProbeIdsSQL, "<<PATTERNS>>", patternsSql, 1)

// 	rows, err := gdb.db.Query(query, args...)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer rows.Close()

// 	for rows.Next() {
// 		var probe Idtype
// 		err := rows.Scan(&probe.Id, &probe.PublicId, &probe.Name)

// 		if err != nil {
// 			return nil, err
// 		}

// 		ret = append(ret, &probe)
// 	}

// 	return ret, nil
// }

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

// using binary blobs for Probes values
func (gdb *GexDB) Expression(datasetId string,
	exprType *sys.Entity,
	probes []*Probe,
	isAdmin bool,
	permissions []string) (*SearchResults, error) {

	//exprType, err := gdb.ExprType(exprTypeId)

	//if err != nil {
	//	return nil, err
	//}

	//probeIds, err := gdb.FindProbes(genes)

	dataset, err := gdb.BasicDataset(datasetId, permissions, isAdmin)

	if err != nil {
		return nil, err
	}

	ret := SearchResults{
		Dataset:  dataset,
		ExprType: exprType,
		Probes:   make([]*ExpressionProbe, 0, len(probes))}

	var url string
	var offset int64
	var length int

	for _, probe := range probes {
		namedArgs := []any{
			sql.Named("dataset", datasetId),
			sql.Named("probe", probe.Id),
			sql.Named("type", exprType.Id)}

		query := sqlite.MakePermissionsSql(ExprSQL, isAdmin, permissions, &namedArgs)

		err := gdb.db.QueryRow(query, namedArgs...).Scan(
			&url,
			&offset,
			&length)

		if err != nil {
			log.Debug().Msgf("here2 %v", err)

			return nil, err
		}

		path := filepath.Join(gdb.dir, url)

		// the offset is the start of a row block which consists
		// of a 4 byte unsigned int of the probe id, which can be
		// matched to the database and then the data
		_, values, err := readGeneBlock(path, offset, length)

		if err != nil {
			return nil, err
		}

		//log.Debug().Msgf("v %v  ", values)

		feature := ExpressionProbe{Probe: probe, Values: values}

		ret.Probes = append(ret.Probes, &feature)
	}

	return &ret, nil
}

// func (gdb *GexDB) FindSeqValues(dataset string,
// 	exprTypeId string,
// 	genes []string,
// 	isAdmin bool,
// 	permissions []string) (*SearchResults, error) {

// 	probes, err := gdb.FindProbes(genes)

// 	if err != nil {
// 		return nil, err
// 	}

// 	return gdb.Probes(dataset, exprTypeId, probes, isAdmin, permissions)
// }

// read a binary file containing float32 values with a given offset and count
// and return the probe id and a float array of the sample values
func readGeneBlock(path string, offset int64, l int) (uint32, []float32, error) {
	// Open the file for reading
	f, err := os.Open(path)

	if err != nil {
		return 0, nil, err
	}

	defer f.Close()

	// Seek to the specified offset
	_, err = f.Seek(offset, 0) // 0 means "from the beginning of the file"

	if err != nil {
		return 0, nil, err
	}

	var probeId uint32

	err = binary.Read(f, binary.LittleEndian, &probeId)

	if err != nil {
		return 0, nil, err
	}

	// Prepare a slice to hold the read values
	data := make([]float32, l)

	// Read the data into the slice
	err = binary.Read(f, binary.LittleEndian, &data)

	if err != nil {
		return 0, nil, err
	}

	return probeId, data, nil
}

// func read(f *os.File, offset int, length int) ([]byte, error) {
// 	buf := make([]byte, length)
// 	_, err := f.ReadAt(buf, int64(offset))
// 	if err != nil {
// 		return nil, err
// 	}
// 	return buf, nil
// }

// func (gdb *GexDB) FindMicroarrayValues(dataset string,
// 	exprTypeId string,
// 	genes []string,
// 	isAdmin bool,
// 	permissions []string) (*SearchResults, error) {

// 	probes, err := gdb.FindProbes(genes)

// 	if err != nil {
// 		return nil, err
// 	}

// 	return gdb.Expr(dataset, exprTypeId, probes, isAdmin, permissions)
// }

func MakeInDatasetsSql(query string, datasets []string, namedArgs *[]any) string {

	datasets = collections.TruncateSlice(datasets, MaxDatasets)

	inPlaceholders := make([]string, len(datasets))

	for i, dataset := range datasets {
		ph := fmt.Sprintf("ds%d", i+1)
		inPlaceholders[i] = ":" + ph
		*namedArgs = append(*namedArgs, sql.Named(ph, dataset))
	}

	clause := "d.public_id IN (" + strings.Join(inPlaceholders, ",") + ")"

	return strings.Replace(query, "<<DATASETS>>", clause, 1)

}

func MakeInProbesSql(query string, probes []int, namedArgs *[]any) string {

	probes = collections.TruncateSlice(probes, MaxProbes)

	inPlaceholders := make([]string, len(probes))

	for i, probe := range probes {
		ph := fmt.Sprintf("p%d", i+1)
		inPlaceholders[i] = ":" + ph
		*namedArgs = append(*namedArgs, sql.Named(ph, probe))
	}

	clause := "e.probe_id IN (" + strings.Join(inPlaceholders, ",") + ")"

	return strings.Replace(query, "<<PROBES>>", clause, 1)

}

// func MakeOrderedPatternsClause(query string, list []string) (string, []any) {
// 	if len(list) == 0 {
// 		return "", nil
// 	}

// 	parts := make([]string, len(list))
// 	params := make([]any, 0, len(list)*2)

// 	for i, s := range list {
// 		s = web.FormatParam(s)
// 		idx := i + 1
// 		patternName := fmt.Sprintf("v%d", idx)
// 		ordName := fmt.Sprintf("vo%d", idx)

// 		// Build one row
// 		parts[i] = fmt.Sprintf("(:%s, :%s)", patternName, ordName)

// 		// Bind values
// 		params = append(params,
// 			sql.Named(patternName, s),
// 			sql.Named(ordName, idx),
// 		)
// 	}

// 	patternsSql := "(VALUES " + strings.Join(parts, ", ") + ") AS v(id, ord)"

// 	return strings.Replace(query, "<<PATTERNS>>", patternsSql, 1), params

// }
