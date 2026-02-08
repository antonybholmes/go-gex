package gex

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-web/auth/sqlite"
)

type (
	Entity struct {
		Id       int    `json:"-"`
		PublicId string `db:"public_id" json:"id"`
		Name     string `json:"name"`
	}

	GexGene struct {
		Entity
		Ensembl string `json:"ensembl,omitempty"`
		Refseq  string `json:"refseq,omitempty"`
		//GeneSymbol string `json:"geneSymbol"`
		Ncbi string `json:"ncbi,omitempty"`
	}

	Probe struct {
		Entity
		Gene *GexGene `json:"gene,omitempty"`
	}

	Technology struct {
		Entity
		ExprTypes []Entity `json:"exprTypes"`
	}

	NamedValue struct {
		Entity
		Value string `json:"value"`
		Color string `json:"color,omitempty"`
	}

	Metadata struct {
		//Id          string `json:"id"`
		Name        string `json:"name"`
		Value       string `json:"value"`
		Color       string `json:"color,omitempty"`
		Description string `json:"description,omitempty"`
	}

	Sample struct {
		Id   string `json:"id"`
		Name string `json:"name"`
		//AltNames []NameValueType `json:"altNames"`
		Metadata []*Metadata `json:"metadata"`
	}

	SearchResults struct {
		// we use the simpler value type for platform in search
		// results so that the value types are not repeated in
		// each search. The useful info in a search is just
		// the platform name and id

		Dataset  *Entity       `json:"dataset"`
		ExprType *Entity       `json:"type"`
		Features []*Expression `json:"features"`
	}

	// Either a probe or gene
	Expression struct {
		Probe *Probe `json:"probe"` // distinguish between null and ""
		//Gene  *GexGene `json:"gene"`
		//Platform     *ValueType       `json:"platform"`
		//GexValue *GexValue    `json:"gexType"`
		Values []float32 `json:"values"`
	}

	Dataset struct {
		Entity
		Genome     string `json:"genome"`
		Technology string `json:"technology"`
		Platform   string `json:"platform"`

		Institution string    `json:"institution"`
		Description string    `json:"description"`
		Samples     []*Sample `json:"samples"`
		ExprTypes   []*Entity `json:"exprTypes"`
	}

	GexDB struct {
		db   *sql.DB
		dir  string
		path string
	}
)

const (
	DefaultNumSamples = 500

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
		t.public_id
		t.name
		FROM technologies t
		JOIN datasets d ON d.technology_id = t.id
		WHERE datasets.public_id = :id
		ORDER BY t.name`

	AllTechnologiesSQL = `SELECT DISTINCT 
		species, 
		technology, 
		platform 
		FROM datasets 
		ORDER BY species, technology, platform`

	BaseDatasetsSQL = `SELECT 
		d.public_id,
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
			<<PERMISSIONS>>`

	DatasetsSQL = BaseDatasetsSQL + ` AND d.genome_id = :gid 
			AND d.technology_id = :tid
			ORDER BY d.name`

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
		m.name,
		m.color
		FROM metadata m
		ORDER BY m.name`

	// order by sample id and then sample_metadata.id to ensure consistent order of metadata for each sample
	// as it was read from its original source file
	SampleMetadataSQL = `SELECT
		s.id,
		m.name,
		smd.value,
		m.color
		FROM sample_metadata smd
		JOIN metadata m ON smd.metadata_id = m.id
		JOIN samples s ON smd.sample_id = s.id
		ORDER by s.id, smd.id`

	ExprTypesSQL = `SELECT
		e.id,
		e.public_id,
		e.name
		FROM expr_types e
		JOIN expression ex ON e.id = ex.expression_type_id
		JOIN datasets d ON ex.dataset_id = d.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE
			<<PERMISSIONS>>
			AND <<DATASETS>>
		ORDER BY e.name`

	ExprTypeSQL = `SELECT
		e.id,
		e.public_id,
		e.name
		FROM expr_types e
		WHERE
			e.public_id = :id
			OR e.name LIKE :id
		LIMIT 1`

	GeneSQL = `SELECT 
		g.public_id, 
		g.ensembl,
		g.refseq,
		g.ncbi,
		g.gene_symbol 
		FROM genes g
		WHERE
			g.public_id = :id OR
			g.ensembl = :id OR 
			g.refseq = :id OR
			g.gene_symbol LIKE :id
		LIMIT 1`

	ProbesSQL = `SELECT DISTINCT
		p.id AS probe_id,
		p.public_id AS probe_public_id,
		p.name AS probe_name,
		g.id AS gene_id, 
		g.public_id AS gene_public_id, 
		g.gene_symbol AS gene_name,
		g.ensembl,
		g.refseq,
		g.ncbi
		FROM probes p
		JOIN genes g ON g.id = p.gene_id
		JOIN <<PATTERNS>> ON (
			p.public_id = v.id
			OR p.name LIKE v.id
			OR g.public_id = v.id
			OR g.gene_symbol LIKE v.id
			OR g.ensembl = v.id
			OR g.refseq = v.id
		)
		ORDER BY v.ord`

	ProbeIdsSQL = `SELECT DISTINCT
		p.id AS probe_id,
		p.public_id AS probe_public_id,
		p.name AS probe_name
		FROM probes p
		JOIN genes g ON g.id = p.gene_id
		JOIN <<PATTERNS>> ON (
			p.public_id = v.id
			OR p.name LIKE v.id
			OR g.public_id = v.id
			OR g.gene_symbol LIKE v.id
			OR g.ensembl = v.id
			OR g.refseq = v.id
		)
		ORDER BY v.ord`

	// ExprSQL = `SELECT
	// 	p.id,
	// 	p.public_id,
	// 	p.name,
	// 	f.url,
	// 	e.offset
	// 	FROM expression e
	// 	JOIN datasets d ON ex.dataset_id = e.id
	// 	JOIN dataset_permissions dp ON d.id = dp.dataset_id
	// 	JOIN probes p ON e.probe_id = p.id
	// 	JOIN files f ON e.file_id = f.id
	// 	WHERE
	// 		<<PERMISSIONS>>
	// 		AND <<PROBES>>
	// 		AND e.expression_type_id = :expr_type
	// 		AND d.public_id = :dataset
	// 	ORDER BY p.name`

	ExprSQL = `SELECT
		f.url,
		e.offset
		FROM expression e
		JOIN datasets d ON ex.dataset_id = e.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN files f ON e.file_id = f.id
		WHERE 
			<<PERMISSIONS>>
			AND e.probe_id = :probe
			AND e.expression_type_id = :expr_type
			AND d.public_id = :dataset`

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

	return &GexDB{dir: dir, path: path, db: sys.Must(sql.Open(sys.Sqlite3DB, path+sys.Sqlite3RO))}
}

func (gdb *GexDB) Close() error {
	return gdb.db.Close()
}

func (gdb *GexDB) Dir() string {
	return gdb.dir
}

func (gdb *GexDB) Genomes() ([]*Entity, error) {

	genomes := make([]*Entity, 0, 10)

	rows, err := gdb.db.Query(GenomesSql)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var genome Entity

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

func (gdb *GexDB) Datasets(genome string,
	technology string,
	permissions []string,
	isAdmin bool) ([]*Dataset, error) {

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
			&dataset.PublicId,
			&dataset.Genome,
			&dataset.Technology,
			&dataset.Platform,
			&dataset.Institution,
			&dataset.Name)

		if err != nil {
			return nil, err
		}

		datasets = append(datasets, &dataset)
	}

	return datasets, nil
}

// used for search results where only basic dataset info is needed
func (gdb *GexDB) BasicDataset(datasetId string, permissions []string, isAdmin bool) (*Entity, error) {

	namedArgs := []any{sql.Named("id", datasetId)}

	query := sqlite.MakePermissionsSql(BasicDatasetSQL, isAdmin, permissions, &namedArgs)

	var ret Entity

	err := gdb.db.QueryRow(query, namedArgs...).Scan(
		&ret.Id,
		&ret.PublicId,
		&ret.Name)

	if err != nil {
		return nil, err
	}

	return &ret, nil
}

func (gdb *GexDB) Metadata() ([]*Metadata, error) {

	rows, err := gdb.db.Query(MetadataSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	metadata := make([]*Metadata, 0, 20)

	for rows.Next() {
		var m Metadata

		err := rows.Scan(
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
		sample.Metadata = make([]*Metadata, 0, 10)

		samples = append(samples, &sample)
		sampleMap[sample.Id] = &sample
	}

	var sampleId string

	// add sample metadata to samples

	rows, err = gdb.db.Query(SampleMetadataSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var m Metadata

		err := rows.Scan(sampleId, &m.Name, &m.Value, &m.Color)

		if err != nil {
			log.Error().Msgf("error scanning sample: %v", err)
			return nil, err
		}

		sampleMap[sampleId].Metadata = append(sampleMap[sampleId].Metadata, &m)
	}

	return samples, nil
}

func (gdb *GexDB) ExprType(id string) (*Entity, error) {

	var ret Entity

	err := gdb.db.QueryRow(ExprTypeSQL, sql.Named("id", id)).Scan(
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

func (gdb *GexDB) ExprTypes(datasetIds []string, isAdmin bool, permissions []string) ([]*Entity, error) {

	namedArgs := []any{}

	query := sqlite.MakePermissionsSql(DatasetsSQL, isAdmin, permissions, &namedArgs)

	query = MakeInDatasetsSql(query, datasetIds, &namedArgs)

	allExprTypes := make(map[string]*Entity)

	rows, err := gdb.db.Query(query, namedArgs...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var exprType Entity

		err := rows.Scan(
			&exprType.PublicId,
			&exprType.Name)

		if err != nil {
			return nil, err
		}

		if _, exists := allExprTypes[exprType.PublicId]; !exists {
			allExprTypes[exprType.PublicId] = &exprType
		}

	}

	ret := make([]*Entity, 0, len(datasetIds))

	for _, exprType := range allExprTypes {
		ret = append(ret, exprType)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Name < ret[j].Name
	})

	return ret, nil
}

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

func (gdb *GexDB) FindProbes(genes []string) ([]*Probe, error) {

	ret := make([]*Probe, 0, len(genes))

	patternsSql, args := MakeOrderdPatternsClause(genes)

	query := strings.Replace(ProbeIdsSQL, "<<PATTERNS>>", patternsSql, 1)

	rows, err := gdb.db.Query(query, args...)

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
			&probe.Gene.Name,
			&probe.Gene.Ensembl,
			&probe.Gene.Refseq,
			&probe.Gene.Ncbi,
		)

		if err != nil {
			return nil, err
		}

		ret = append(ret, &probe)
	}

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

// using binary blobs for expression values
func (gdb *GexDB) Expression(datasetId string,
	exprType *Entity,
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
		Features: make([]*Expression, 0, len(probes))}

	// query = MakeInProbesSql(query, probeIds, &namedArgs)

	// rows, err := gdb.db.Query(query, namedArgs...)

	// if err != nil {
	// 	return nil, err
	// }

	// defer rows.Close()

	// var url string
	// var offset int64

	// for rows.Next() {
	// 	var probe Idtype
	// 	err := rows.Scan(
	// 		&probe.Id,
	// 		&probe.PublicId,
	// 		&probe.Name,
	// 		&url,
	// 		&offset)

	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	path := filepath.Join(gdb.dir, url)

	// 	values, err := readFloat32sWithOffset(path, offset, dataset.Count)

	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	feature := ResultFeature{Probe: &probe, Expr: values}

	// 	ret.Features = append(ret.Features, &feature)
	// }

	var url string
	var offset int64
	var length int

	for _, probe := range probes {
		namedArgs := []any{sql.Named("dataset", datasetId),
			sql.Named("probe", probe.Id),
			sql.Named("expr_type", exprType.Id)}

		query := sqlite.MakePermissionsSql(ExprSQL, isAdmin, permissions, &namedArgs)

		err := gdb.db.QueryRow(query, namedArgs...).Scan(
			&url,
			&offset,
			&length)

		if err != nil {
			return nil, err
		}

		path := filepath.Join(gdb.dir, url)

		values, err := readFloat32Array(path, offset, length)

		if err != nil {
			return nil, err
		}

		feature := Expression{Probe: probe, Values: values}

		ret.Features = append(ret.Features, &feature)
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

// 	return gdb.Expression(dataset, exprTypeId, probes, isAdmin, permissions)
// }

// read a binary file containing float32 values with a given offset and count
// and create a float32 slice from the values
func readFloat32Array(path string, offset int64, l int) ([]float32, error) {
	// Open the file for reading
	f, err := os.Open(path)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	// Seek to the specified offset
	_, err = f.Seek(offset, 0) // 0 means "from the beginning of the file"

	if err != nil {
		return nil, err
	}

	// Prepare a slice to hold the read values
	data := make([]float32, l)

	// Read the data into the slice
	err = binary.Read(f, binary.LittleEndian, &data)

	if err != nil {
		return nil, err
	}

	return data, nil
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

	inPlaceholders := make([]string, len(probes))

	for i, probe := range probes {
		ph := fmt.Sprintf("p%d", i+1)
		inPlaceholders[i] = ":" + ph
		*namedArgs = append(*namedArgs, sql.Named(ph, probe))
	}

	clause := "e.probe_id IN (" + strings.Join(inPlaceholders, ",") + ")"

	return strings.Replace(query, "<<PROBES>>", clause, 1)

}

func MakeOrderdPatternsClause(list []string) (string, []any) {
	if len(list) == 0 {
		return "", nil
	}

	parts := make([]string, len(list))
	params := make([]any, 0, len(list)*2)

	for i, s := range list {
		idx := i + 1
		patternName := fmt.Sprintf("v%d", idx)
		ordName := fmt.Sprintf("vo%d", idx)

		// Build one row
		parts[i] = fmt.Sprintf("(:%s, :%s)", patternName, ordName)

		// Bind values
		params = append(params,
			sql.Named(patternName, s),
			sql.Named(ordName, idx),
		)
	}

	return "(VALUES" + strings.Join(parts, ", ") + ") AS v(id, ord)", params
}
