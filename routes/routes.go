package routes

import (
	"errors"

	"github.com/antonybholmes/go-gex"
	"github.com/antonybholmes/go-gex/gexdb"

	"github.com/antonybholmes/go-web"
	"github.com/gin-gonic/gin"
)

type GexParams struct {
	Species    string        `json:"species"`
	Technology string        `json:"technology"`
	ExprType   *gex.ExprType `json:"exprType"` // use pointer so we can check for nil
	Genes      []string      `json:"genes"`
	Datasets   []string      `json:"datasets"`
}

func parseParamsFromPost(c *gin.Context) (*GexParams, error) {

	var params GexParams

	err := c.Bind(&params)

	if err != nil {
		return nil, err
	}

	return &params, nil
}

func SpeciesRoute(c *gin.Context) {

	types, err := gexdb.Species()

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", types)
}

func TechnologiesRoute(c *gin.Context) {

	technologies := gexdb.Technologies() //gexdbcache.Technologies()

	web.MakeDataResp(c, "", technologies)
}

func ExprTypesRoute(c *gin.Context) {

	params, err := parseParamsFromPost(c)

	if err != nil {
		c.Error(err)
		return
	}

	exprTypes, err := gexdb.ExprTypes(params.Datasets)

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", exprTypes)
}

func GexDatasetsRoute(c *gin.Context) {

	species := c.Param("species")

	technology := c.Param("technology")

	datasets, err := gexdb.Datasets(species, technology)

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", datasets)
}

func GexGeneExprRoute(c *gin.Context) {
	params, err := parseParamsFromPost(c)

	if err != nil {
		c.Error(err)
		return
	}

	// we enforce the expr type so that if multiple datasets are provided
	// they must all be of the same type so that we do not
	// mix microarray and rna-seq searches together for example
	if params.ExprType == nil {
		web.BadReqResp(c, errors.New("expr type is required"))
		return
	}

	results := make([]*gex.SearchResults, 0, len(params.Datasets))

	// at a minimum the dataset id will contain the technology
	// we can use this to determine which search function to call
	// e.g. "FindMicroarrayValues" or "FindSeqValues", other
	// info could be added later
	if params.ExprType.Name == "Microarray" {
		// microarray
		for _, datasetId := range params.Datasets {
			ret, err := gexdb.FindMicroarrayValues(datasetId, params.Genes)

			if err != nil {
				c.Error(err)
				return
			}

			results = append(results, ret)
		}
	} else {
		for _, datasetId := range params.Datasets {
			ret, err := gexdb.FindSeqValues(datasetId, params.ExprType, params.Genes)

			if err != nil {
				c.Error(err)
				return
			}

			results = append(results, ret)
		}
	}

	web.MakeDataResp(c, "", results)

}
