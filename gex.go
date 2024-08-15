package gex

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/antonybholmes/go-dna"
	"github.com/rs/zerolog/log"
)

const GENES_SQL = "SELECT genes.id, genes.gene_id, genes.gene_symbol FROM genes ORDER BY genes.gene_symbol"

const DATASETS_SQL = "SELECT datasets.id, datasets.name, datasets.institution FROM datasets ORDER BY datasets.name"

// const DATASETS_SQL = `SELECT
// 	name
// 	FROM datasets
// 	ORDER BY datasets.name`

const SAMPLES_SQL = `SELECT
	uuid,
	name, 
	coo, 
	lymphgen, 
	paired_normal_dna, 
	institution, 
	sample_type
	FROM samples 
	ORDER BY samples.name`

const FIND_MUTATIONS_SQL = `SELECT 
	chr, 
	start, 
	end, 
	ref, 
	tum, 
	t_alt_count, 
	t_depth, 
	variant_type,
	vaf,
	sample_uuid
	FROM mutations 
	WHERE chr = ?1 AND start >= ?2 AND end <= ?3 
	ORDER BY chr, start, end, variant_type`

type ExpressionDataIndex struct {
	ProbeIds    []string
	EntrezIds   []string
	GeneSymbols []string
}

type ExpressionData struct {
	Exp    [][]float64
	Header []string
	Index  ExpressionDataIndex
}

type MutationsReq struct {
	Assembly string        `json:"assembly"`
	Location *dna.Location `json:"location"`
	Samples  []string      `json:"samples"`
}

type Dataset struct {
	Id int `json:"id"`

	Name        string    `json:"name"`
	Assembly    string    `json:"assembly"`
	Description string    `json:"description"`
	Samples     []*Sample `json:"samples"`

	//db                *sql.DB
	//findMutationsStmt *sql.Stmt
}

type Sample struct {
	Id      string `json:"id"`
	Name    string `json:"name"`
	Dataset int    `json:"dataset"`
}

// type MutationDBInfo struct {
// 	//Metadata *MutationDBMetadata `json:"metadata"`
// 	//Datasets []*MutationDBDataSet `json:"datasets"`
// 	Id          string `json:"id"`
// 	Uuid        string `json:"uuid"`
// 	PublicId    string `json:"publicId"`
// 	Name        string `json:"name"`
// 	Assembly    string `json:"assembly"`
// 	Description string `json:"description"`

// 	Samples []*MutationDBSample `json:"samples"`
// }

type RNASeqGex struct {
	Dataset int     `json:"dataset"`
	Sample  int     `json:"sample"`
	Gene    int     `json:"gene"`
	Counts  int     `json:"counts"`
	TPM     float32 `json:"tpm"`
	VST     float32 `json:"vst"`
}

func (mutation *Mutation) Clone() *Mutation {
	var ret Mutation = Mutation{Chr: mutation.Chr,
		Start:  mutation.Start,
		End:    mutation.End,
		Ref:    mutation.Ref,
		Tum:    mutation.Tum,
		Alt:    mutation.Alt,
		Depth:  mutation.Depth,
		Type:   mutation.Type,
		Vaf:    mutation.Vaf,
		Sample: mutation.Sample,
	}

	return &ret
}

type DatasetResults struct {
	Dataset string `json:"dataset"`

	Mutations []*Mutation `json:"mutations"`
}

type PileupResults struct {
	Location *dna.Location `json:"location"`
	Datasets []string      `json:"datasets"`
	//Samples   uint                  `json:"samples"`
	Pileup [][]*Mutation `json:"pileup"`
}

type SearchResults struct {
	Location *dna.Location `json:"location"`
	//Info           []*Info           `json:"info"`
	DatasetResults []*DatasetResults `json:"results"`
}

// func MutationDBKey(assembly string, name string) string {
// 	return fmt.Sprintf("%s:%s", assembly, name)
// }

// func NewMutationDBMetaData(assembly string, name string) *MutationDBMetadata {

// 	return &MutationDBMetadata{
// 		Id:          MutationDBKey(assembly, name),
// 		Assembly:    assembly,
// 		Name:        name,
// 		Description: "",
// 	}
// }

// type Dataset struct {
// 	Info    *Info     `json:"info"`
// 	Samples []*Sample `json:"samples"`
// }

func NewDataset(file string) (*Dataset, error) {
	//file := path.Join(dir, "mutations.db")
	db, err := sql.Open("sqlite3", file)

	if err != nil {
		log.Fatal().Msgf("%s", err)
	}

	defer db.Close()

	dataset := &Dataset{
		File:    file,
		Samples: make([]*Sample, 0, 100),
	}

	err = db.QueryRow(GENES_SQL).Scan(&dataset.Uuid,
		&dataset.PublicId,
		&dataset.Name,
		&dataset.Description,
		&dataset.Assembly)

	if err != nil {
		log.Fatal().Msgf("info %s", err)
	}

	//mutationDB.Id = MutationDBKey(mutationDB.Assembly, mutationDB.PublicId)

	// datasetRows, err := db.Query(DATASETS_SQL)

	// if err != nil {
	// 	log.Fatal().Msgf("%s", err)
	// }

	// defer datasetRows.Close()

	// datasets := []*MutationDBDataSet{}

	// for datasetRows.Next() {
	// 	var dataset MutationDBDataSet

	// 	err := datasetRows.Scan(
	// 		&dataset.Name)

	// 	if err != nil {
	// 		log.Fatal().Msgf("%s", err)
	// 	}

	// 	datasets = append(datasets, &dataset)
	// }

	sampleRows, err := db.Query(SAMPLES_SQL)

	if err != nil {
		log.Fatal().Msgf("%s", err)
	}

	defer sampleRows.Close()

	for sampleRows.Next() {
		var sample Sample

		err := sampleRows.Scan(
			&sample.Uuid,
			&sample.Name,
			&sample.COO,
			&sample.Lymphgen,
			&sample.PairedNormalDna,
			&sample.Institution,
			&sample.SampleType)

		sample.Dataset = dataset.Uuid

		if err != nil {
			log.Fatal().Msgf("%s", err)
		}

		dataset.Samples = append(dataset.Samples, &sample)
	}

	// info := &MutationDBInfo{
	// 	Metadata: metadata,
	// 	//Datasets: datasets,
	// 	Samples: samples,
	// }

	return dataset, nil
}

// func (mutationsdb *MutationsDB) AllMutationSets() (*[]MutationSet, error) {

// 	rows, err := mutationsdb.AllMutationSetsStmt.Query()

// 	if err != nil {
// 		return nil, err
// 	}

// 	mutationSets := []MutationSet{}

// 	defer rows.Close()

// 	for rows.Next() {
// 		var mutationSet MutationSet
// 		err := rows.Scan(&mutationSet.Uuid, &mutationSet.Name)

// 		if err != nil {
// 			fmt.Println(err)
// 		}

// 		mutationSets = append(mutationSets, mutationSet)
// 	}

// 	return &mutationSets, nil
// }

func (dataset *Dataset) Search(location *dna.Location) (*DatasetResults, error) {

	db, err := sql.Open("sqlite3", dataset.File) //not clear on what is needed for the user and password

	if err != nil {
		return nil, err
	}

	defer db.Close()

	rows, err := db.Query(FIND_MUTATIONS_SQL, location.Chr, location.Start, location.End)

	if err != nil {
		return nil, err
	}

	mutations, err := rowsToMutations(rows)

	if err != nil {
		return nil, err
	}

	return &DatasetResults{Dataset: dataset.Uuid, Mutations: mutations}, nil
}

func GetPileup(search *SearchResults) (*PileupResults, error) {
	// first lets fix deletions and insertions
	for _, datasetResults := range search.DatasetResults {
		for _, mutation := range datasetResults.Mutations {
			// change for sorting purposes so that ins always comes last
			switch mutation.Type {
			case "INS":
				mutation.Type = "2:INS"
				// modify the output so that is begins with a caret to indicate
				// an insertion
				mutation.Tum = fmt.Sprintf("^%s", mutation.Tum)
			case "DEL":
				mutation.Type = "3:DEL"
			default:
				mutation.Type = "1:SNP"
			}
		}
	}

	// put together by position, type, tum

	pileupMap := make(map[uint]map[string]map[string][]*Mutation)

	for _, datasetResults := range search.DatasetResults {
		for _, mutation := range datasetResults.Mutations {

			mutation.Dataset = datasetResults.Dataset

			switch mutation.Type {
			case "3:DEL":
				for i := range mutation.End - mutation.Start + 1 {
					addToPileupMap(&pileupMap, mutation.Start+i, mutation)
				}
			case "2:INS":
				addToPileupMap(&pileupMap, mutation.Start, mutation)
			default:
				// deal with concatenated snps
				//tum := []rune(mutation.Tum)
				for i, c := range mutation.Tum {
					// clone and change tumor
					mut2 := mutation.Clone()
					mut2.Tum = string(c)
					addToPileupMap(&pileupMap, mut2.Start+uint(i), mut2)
				}
			}
		}
	}

	location := search.Location

	// init pileup
	pileup := make([][]*Mutation, location.Len())

	for i := range location.Len() {
		pileup[i] = []*Mutation{}
	}

	// get sorted start positions
	starts := make([]uint, 0, len(pileupMap))

	for start := range pileupMap {
		starts = append(starts, start)
	}

	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })

	// assemble pileups on each start location
	for _, start := range starts {
		// sort variant types
		variantTypes := make([]string, 0, len(pileupMap[start]))

		for variantType := range pileupMap[start] {
			variantTypes = append(variantTypes, variantType)
		}

		sort.Strings(variantTypes)

		for _, variantType := range variantTypes {
			// sort variant change
			tumors := make([]string, 0, len(pileupMap[start][variantType]))

			for tumor := range pileupMap[start][variantType] {
				tumors = append(tumors, tumor)
			}

			sort.Strings(tumors)

			for _, tumor := range tumors {
				mutations := pileupMap[start][variantType][tumor]

				for _, mutation := range mutations {
					offset := start - location.Start

					pileup[offset] = append(pileup[offset], mutation)
				}

			}

		}
	}

	// extract the datasets on which dataframe we are using
	datasets := make([]string, 0, len(search.DatasetResults))

	for _, results := range search.DatasetResults {
		datasets = append(datasets, results.Dataset)
	}

	log.Debug().Msgf("what the %d", len(datasets))

	return &PileupResults{Location: location, Datasets: datasets, Pileup: pileup}, nil
}

func addToPileupMap(pileupMap *map[uint]map[string]map[string][]*Mutation, start uint, mutation *Mutation) {

	_, ok := (*pileupMap)[start]

	if !ok {
		(*pileupMap)[start] = make(map[string]map[string][]*Mutation)
	}

	_, ok = (*pileupMap)[start][mutation.Type]

	if !ok {
		(*pileupMap)[start][mutation.Type] = make(map[string][]*Mutation)
	}

	_, ok = (*pileupMap)[start][mutation.Type][mutation.Tum]

	if !ok {
		(*pileupMap)[start][mutation.Type][mutation.Tum] = make([]*Mutation, 0, 100)
	}

	(*pileupMap)[start][mutation.Type][mutation.Tum] = append((*pileupMap)[start][mutation.Type][mutation.Tum], mutation)
}

func rowsToMutations(rows *sql.Rows) ([]*Mutation, error) {

	mutations := make([]*Mutation, 0, 100)

	defer rows.Close()

	for rows.Next() {
		var mutation Mutation

		err := rows.Scan(
			&mutation.Chr,
			&mutation.Start,
			&mutation.End,
			&mutation.Ref,
			&mutation.Tum,
			&mutation.Alt,
			&mutation.Depth,
			&mutation.Type,
			&mutation.Vaf,
			&mutation.Sample)

		if err != nil {
			fmt.Println(err)
		}

		mutations = append(mutations, &mutation)
	}

	return mutations, nil
}

type DatasetCache struct {
	dir      string
	cacheMap map[string]*Dataset
}

func NewMutationDBCache(dir string) *DatasetCache {

	cacheMap := make(map[string]*Dataset)

	log.Debug().Msgf("---- mutations ----")

	dbFiles, err := os.ReadDir(dir)

	if err != nil {
		log.Fatal().Msgf("%s", err)

	}

	// init the cache
	//cacheMap[assemblyFile.Name()] = make(map[string]*MutationDB)

	for _, dbFile := range dbFiles {
		if !strings.HasSuffix(dbFile.Name(), ".db") {
			continue
		}

		log.Debug().Msgf("Loading mutations from %s...", dbFile.Name())

		//metadata := NewMutationDBMetaData(assemblyFile.Name(), dbFile.Name())

		path := filepath.Join(dir, dbFile.Name())

		dataset, err := NewDataset(path)

		if err != nil {
			log.Fatal().Msgf("%s", err)
		}

		log.Debug().Msgf("Caching %s", dataset.PublicId)

		cacheMap[dataset.Uuid] = dataset
	}

	log.Debug().Msgf("---- end ----")

	return &DatasetCache{dir, cacheMap}
}

func (cache *DatasetCache) Dir() string {
	return cache.dir
}

func (cache *DatasetCache) List() []*Dataset {

	ret := make([]*Dataset, 0, len(cache.cacheMap))

	ids := make([]string, 0, len(cache.cacheMap))

	for id := range cache.cacheMap {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	for _, id := range ids {
		ret = append(ret, cache.cacheMap[id])
	}

	return ret
}

func (cache *DatasetCache) GetDataset(uuid string) (*Dataset, error) {
	dataset, ok := cache.cacheMap[uuid]

	if !ok {
		return nil, fmt.Errorf("dataset not found")
	}

	return dataset, nil
}

func (cache *DatasetCache) Search(location *dna.Location, uuids []string) (*SearchResults, error) {
	results := SearchResults{Location: location, DatasetResults: make([]*DatasetResults, 0, len(uuids))}

	for _, uuid := range uuids {
		dataset, err := cache.GetDataset(uuid)

		if err != nil {
			return nil, err
		}

		datasetResults, err := dataset.Search(location)

		if err != nil {
			return nil, err
		}

		results.DatasetResults = append(results.DatasetResults, datasetResults)
	}

	return &results, nil
}
