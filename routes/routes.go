package routes

import (
	"errors"

	"github.com/antonybholmes/go-gex"
	"github.com/antonybholmes/go-gex/gexdb"
	"github.com/antonybholmes/go-web"
	"github.com/antonybholmes/go-web/auth"
	"github.com/antonybholmes/go-web/middleware"
	"github.com/gin-gonic/gin"
)

type GexParams struct {
	Genomes    string   `json:"genomes"`
	Technology string   `json:"technology"`
	ExprType   string   `json:"exprType"` // use pointer so we can check for nil
	Genes      []string `json:"genes"`
	Datasets   []string `json:"datasets"`
}

func parseParamsFromPost(c *gin.Context) (*GexParams, error) {

	var params GexParams

	err := c.Bind(&params)

	if err != nil {
		return nil, err
	}

	return &params, nil
}

func GenomesRoute(c *gin.Context) {

	types, err := gexdb.Genomes()

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", types)
}

// func TechnologiesRoute(c *gin.Context) {

// 	technologies := gexdb.Technologies() //gexdbcache.Technologies()

// 	web.MakeDataResp(c, "", technologies)
// }

func ExprTypesRoute(c *gin.Context) {
	middleware.JwtUserWithPermissionsRoute(c, func(c *gin.Context, isAdmin bool, user *auth.AuthUserJwtClaims) {

		params, err := parseParamsFromPost(c)

		if err != nil {
			c.Error(err)
			return
		}

		exprTypes, err := gexdb.ExprTypes(params.Datasets, isAdmin, user.Permissions)

		if err != nil {
			c.Error(err)
			return
		}

		web.MakeDataResp(c, "", exprTypes)
	})
}

func GexDatasetsRoute(c *gin.Context) {
	middleware.JwtUserWithPermissionsRoute(c, func(c *gin.Context, isAdmin bool, user *auth.AuthUserJwtClaims) {

		genome := c.Param("genome")
		technology := c.Param("technology")

		datasets, err := gexdb.Datasets(genome, technology, isAdmin, user.Permissions)

		if err != nil {
			c.Error(err)
			return
		}

		web.MakeDataResp(c, "", datasets)
	})
}

func GexGeneExprRoute(c *gin.Context) {
	middleware.JwtUserWithPermissionsRoute(c, func(c *gin.Context, isAdmin bool, user *auth.AuthUserJwtClaims) {
		params, err := parseParamsFromPost(c)

		if err != nil {
			c.Error(err)
			return
		}

		// we enforce the expr type so that if multiple datasets are provided
		// they must all be of the same type so that we do not
		// mix microarray and rna-seq searches together for example
		if params.ExprType == "" {
			web.BadReqResp(c, errors.New("expr type is required"))
			return
		}

		results := make([]*gex.SearchResults, 0, len(params.Datasets))

		// at a minimum the dataset id will contain the technology
		// we can use this to determine which search function to call
		// e.g. "FindMicroarrayValues" or "FindSeqValues", other
		// info could be added later
		if params.ExprType == "Microarray" {
			// microarray
			for _, datasetId := range params.Datasets {
				ret, err := gexdb.FindMicroarrayValues(datasetId, params.ExprType, params.Genes, isAdmin, user.Permissions)

				if err != nil {
					c.Error(err)
					return
				}

				results = append(results, ret)
			}
		} else {
			for _, datasetId := range params.Datasets {
				ret, err := gexdb.FindSeqValues(datasetId, params.ExprType, params.Genes, isAdmin, user.Permissions)

				if err != nil {
					c.Error(err)
					return
				}

				results = append(results, ret)
			}
		}

		web.MakeDataResp(c, "", results)
	})
}
