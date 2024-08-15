package dna

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/antonybholmes/go-basemath"
)

type TSSRegion struct {
	offset5p uint
	offset3p uint
}

func NewTSSRegion(offset5p uint, offset3p uint) *TSSRegion {
	return &TSSRegion{offset5p, offset3p}
}

func (tssRegion *TSSRegion) Offset5P() uint {
	return tssRegion.offset5p
}

func (tssRegion *TSSRegion) Offset3P() uint {
	return tssRegion.offset3p
}

type Location struct {
	Chr   string `json:"chr"`
	Start uint   `json:"start"`
	End   uint   `json:"end"`
}

func NewLocation(chr string, start uint, end uint) *Location {
	chr = strings.ToLower(chr)

	if !strings.Contains(chr, "chr") {
		chr = fmt.Sprintf("chr%s", chr)
	}

	s := basemath.UintMax(1, basemath.UintMin(start, end))

	return &Location{Chr: chr, Start: s, End: basemath.UintMax(s, end)}
}

func (location *Location) String() string {
	return fmt.Sprintf("%s:%d-%d", location.Chr, location.Start, location.End)
}

func (location *Location) MarshalJSON() ([]byte, error) {
	return json.Marshal(location.String())
}

func (location *Location) Mid() uint {
	return (location.Start + location.End) / 2
}

func (location *Location) Len() uint {
	return location.End - location.Start + 1
}

func ParseLocation(location string) (*Location, error) {
	matched, err := regexp.MatchString(`^chr([0-9]+|[xyXY]):\d+-\d+$`, location)

	if !matched || err != nil {
		return nil, fmt.Errorf("%s does not seem like a valid location", location)
	}

	tokens := strings.Split(location, ":")
	chr := tokens[0]
	tokens = strings.Split(tokens[1], "-")

	start, err := strconv.ParseUint(tokens[0], 10, 32)

	if err != nil {
		return nil, fmt.Errorf("%s does not seem like a valid start", tokens[0])
	}

	end, err := strconv.ParseUint(tokens[1], 10, 32)

	if err != nil {
		return nil, fmt.Errorf("%s does not seem like a valid end", tokens[1])
	}

	return NewLocation(chr, uint(start), uint(end)), nil
}

func ParseLocations(locations []string) ([]*Location, error) {
	ret := make([]*Location, 0, len(locations))

	for _, l := range locations {
		loc, err := ParseLocation(l)

		if err != nil {
			return nil, err
		}

		ret = append(ret, loc)
	}

	return ret, nil
}
