package dna

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/rs/zerolog/log"
)

// var DNA_4BIT_DECODE_MAP = map[byte]byte{
// 	1:  65,
// 	2:  67,
// 	3:  71,
// 	4:  84,
// 	5:  97,
// 	6:  99,
// 	7:  103,
// 	8:  116,
// 	9:  78,
// 	10: 110,
// }

// use an array for speed since
// we only have 16 values and we
// know explicitly what each value
// maps to
var DNA_4BIT_DECODE_MAP = [16]byte{
	0,
	65,
	67,
	71,
	84,
	97,
	99,
	103,
	116,
	78,
	110,
	0,
	0,
	0,
	0,
	0,
}

const BASE_N byte = 78

// This is simple complementary lookup
// map for DNA bases represented as
// ASCII code bytes, e.g. 65 = 'A' maps
// to 84 = 'T'
// var DNA_COMPLEMENT_MAP = map[byte]byte{
// 	0:   0,
// 	65:  84,
// 	67:  71,
// 	71:  67,
// 	84:  65,
// 	97:  116,
// 	99:  103,
// 	103: 99,
// 	116: 97,
// 	78:  78,
// 	110: 10,
// }

func CompBase(b byte) byte {
	switch b {
	case 65:
		return 84
	case 67:
		return 71
	case 71:
		return 67
	case 84:
		return 65
	case 97:
		return 116
	case 99:
		return 103
	case 103:
		return 99
	case 116:
		return 97
	case 78:
		return 78
	case 110:
		return 10
	default:
		return 0
	}
}

// Returns true if byte is a lowercase ascii letter
func IsLower(b byte) bool {
	switch b {
	case 97, 99, 103, 116, 110:
		return true
	default:
		return false
	}
}

// Converts a byte to an uppercase letter, note that
// this is a restrictive alphabet of A,C,G,T,N so
// it is designed only for DNA sequences.
func toUpper(b byte) byte {
	switch b {
	case 65, 97:
		return 65
	case 67, 99:
		return 67
	case 71, 103:
		return 71
	case 84, 116:
		return 84
	case BASE_N, 110:
		return BASE_N
	default:
		return 0
	}
}

// Converts a byte to a lowercase letter
func toLower(b byte) byte {
	switch b {
	case 65, 97:
		return 97
	case 67, 99:
		return 99
	case 71, 103:
		return 103
	case 84, 116:
		return 116
	case BASE_N, 110:
		return 110
	default:
		return 0
	}
}

// Reverses a byte array
func Rev(dna []byte) {
	l := len(dna)
	lastIndex := l - 1

	for i := 0; i < l/2; i++ {
		j := lastIndex - i
		dna[i], dna[j] = dna[j], dna[i]
	}
}

// Compliments a DNA sequence represented as an ascii byte array.
func Comp(dna []byte) {
	for i, b := range dna {
		dna[i] = CompBase(b)
	}
}

// Reverse complement a dna byte sequence in situ.
func RevComp(dna []byte) {
	Rev(dna)
	Comp(dna)

	// l := len(dna)
	// lastIndex := l - 1
	// n := l / 2

	// // reverse the byte order and complement each base
	// for i := 0; i < n; i++ {
	// 	i2 := lastIndex - i
	// 	b := DNA_COMPLEMENT_MAP[dna[i]]
	// 	dna[i] = DNA_COMPLEMENT_MAP[dna[i2]]
	// 	dna[i2] = b
	// }
}

// Determines how repetative sequences are shown. Can
// be either "", "lower", or "n". If "" formatting is
// ignored allowing change case to modify lowercase to
// uppercase. If "lower" lowercase bases are preserved
// even if change of case specified. If "n" lowercase
// bases are converted to N
func changeRepeatMask(dna []byte, repeatMask string) {
	if strings.ToLower(repeatMask) == "n" {
		for i, b := range dna {
			if IsLower(b) {
				dna[i] = BASE_N
			}
		}

	}
}

// Determines how sequences are displayed. Can
// be either "", "lower", or "upper". If "", formatting is
// ignored. If "lower" lowercase bases are converted to lowercase.
// If "upper" bases are converted to uppercase. Repeat mask rules
// overrule case changes.
func changeCase(dna []byte, format string, repeatMask string) {
	if format == "" || repeatMask != "" {
		return
	}

	if strings.Contains(strings.ToLower(format), "u") {
		for i, b := range dna {
			dna[i] = toUpper(b)
		}
	} else {
		for i, b := range dna {
			dna[i] = toLower(b)
		}
	}

}

type DNADBCache struct {
	Dir      string
	CacheMap map[string]*DNADB
}

func NewDNADBCache(dir string) *DNADBCache {
	cacheMap := make(map[string]*DNADB)

	files, err := os.ReadDir(dir)

	log.Debug().Msgf("---- dna ----")

	if err != nil {
		log.Fatal().Msgf("error opening %s", dir)
	}

	for _, file := range files {
		fileInfo, err := os.Stat(filepath.Join(dir, file.Name()))
		if err != nil {
			log.Debug().Msgf("%s", err)
			continue
		}

		log.Debug().Msgf("dna %s %v", file.Name())

		if fileInfo.IsDir() {
			db := NewDNADB(filepath.Join(dir, file.Name()))
			cacheMap[file.Name()] = db
		}
	}

	return &DNADBCache{dir, cacheMap}
}

func (cache *DNADBCache) List() []string {

	ids := make([]string, 0, len(cache.CacheMap))

	for id := range cache.CacheMap {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids
}

func (dnadbcache *DNADBCache) DB(assembly string) (*DNADB, error) {

	//log.Debug().Msgf("key %s", key)

	_, ok := dnadbcache.CacheMap[assembly]

	if !ok {

		dir := filepath.Join(dnadbcache.Dir, assembly)

		_, err := os.Stat(dir)

		if err != nil {
			return nil, fmt.Errorf("%s is not a valid assembly", assembly)
		}

		db := NewDNADB(dir)

		dnadbcache.CacheMap[assembly] = db
	}

	return dnadbcache.CacheMap[assembly], nil
}

type DNADB struct {
	Dir string
}

func NewDNADB(dir string) *DNADB {
	return &DNADB{dir}
}

func (dnadb *DNADB) DNA(location *Location, format string, repeatMask string, rev bool, comp bool) (string, error) {
	s := location.Start - 1
	e := location.End - 1
	l := e - s + 1
	bs := s / 2
	be := e / 2
	bl := be - bs + 1

	d := make([]byte, bl)

	file := filepath.Join(dnadb.Dir, fmt.Sprintf("%s.dna.4bit", strings.ToLower(location.Chr)))

	f, err := os.Open(file)

	if err != nil {
		return "", err
	}

	f.Seek(int64(1+bs), 0)

	_, err = f.Read(d)

	f.Close()

	if err != nil {
		return "", err
	}

	dna := make([]byte, l)

	// which byte we are scanning (each byte contains 2 bases)
	byteIndex := 0
	var v byte

	for i := uint(0); i < l; i++ {
		// Which base we want in the byte
		// If the start position s is even, we want the first
		// 4 bits of the byte, else the lower 4 bits.
		baseIndex := s % 2

		v = d[byteIndex]

		if baseIndex == 0 {
			v = v >> 4
		} else {
			// if we are on the second base of the byte, on the
			// next loop we must proceed to the next byte to get
			// the base
			byteIndex++
		}

		// mask for lower 4 bits since these
		// contain the dna base code
		dna[i] = DNA_4BIT_DECODE_MAP[v&15]

		s++
	}

	if rev {
		Rev(dna)
	}

	if comp {
		Comp(dna)
	}

	changeRepeatMask(dna, repeatMask)

	changeCase(dna, format, repeatMask)

	return string(dna), nil
}
