package routes

import (
	"errors"

	"github.com/antonybholmes/go-gex"
	"github.com/antonybholmes/go-gex/gexdb"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-web"
	"github.com/antonybholmes/go-web/auth"
	"github.com/antonybholmes/go-web/middleware"
	"github.com/gin-gonic/gin"
)

type GexParams struct {
	//Genome     string   `json:"genome"`
	//Technology string   `json:"technology"`
	//ExprType   string   `json:"type"` // use pointer so we can check for nil
	Genes    []string `json:"genes"`
	Datasets []string `json:"datasets"`
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

func TechnologiesRoute(c *gin.Context) {

	technologies, err := gexdb.Technologies() //gexdbcache.Technologies()

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", technologies)
}

// func ExprTypesRoute(c *gin.Context) {
// 	middleware.JwtUserWithPermissionsRoute(c, func(c *gin.Context, isAdmin bool, user *auth.AuthUserJwtClaims) {

// 		params, err := parseParamsFromPost(c)

// 		if err != nil {
// 			c.Error(err)
// 			return
// 		}

// 		exprTypes, err := gexdb.ExprTypes(params.Datasets, isAdmin, user.Permissions)

// 		if err != nil {
// 			c.Error(err)
// 			return
// 		}

// 		web.MakeDataResp(c, "", exprTypes)
// 	})
// }

func DatasetsRoute(c *gin.Context) {
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

func GeneExpressionRoute(c *gin.Context) {
	middleware.JwtUserWithPermissionsRoute(c, func(c *gin.Context, isAdmin bool, user *auth.AuthUserJwtClaims) {
		genome := c.Param("genome")
		technology := c.Param("technology")
		t := c.Param("type")

		params, err := parseParamsFromPost(c)

		if err != nil {
			c.Error(err)
			return
		}

		// we enforce the expr type so that if multiple datasets are provided
		// they must all be of the same type so that we do not
		// mix microarray and rna-seq searches together for example
		if t == "" {
			web.BadReqResp(c, errors.New("expr type is required"))
			return
		}

		results := make([]*gex.SearchResults, 0, len(params.Datasets))

		// find the expression type desired
		exprType, err := gexdb.ExprType(t)

		if err != nil {
			web.BadReqResp(c, errors.New("invalid expr type"))
			return
		}

		// match the gens to probes using either probe or gene ids
		probes, err := gexdb.FindProbes(genome, technology, params.Genes)

		if err != nil {
			web.BadReqResp(c, errors.New("invalid genes"))
			return
		}

		// search each dataset and gene in order user specified
		for _, datasetId := range params.Datasets {
			ret, err := gexdb.Expression(datasetId, exprType, probes, isAdmin, user.Permissions)

			// if there is an error accessing a dataset, we skip it and continue with the others
			if err != nil {
				log.Debug().Msgf("not able to access dataset: %s", datasetId)
				continue
			}

			results = append(results, ret)
		}

		// at a minimum the dataset id will contain the technology
		// we can use this to determine which search function to call
		// e.g. "FindMicroarrayValues" or "FindSeqValues", other
		// info could be added later
		// if params.ExprType == "Microarray" {
		// 	// microarray
		// 	for _, datasetId := range params.Datasets {
		// 		ret, err := gexdb.FindMicroarrayValues(datasetId, params.ExprType, params.Genes, isAdmin, user.Permissions)

		// 		if err != nil {
		// 			c.Error(err)
		// 			return
		// 		}

		// 		results = append(results, ret)
		// 	}
		// } else {
		// 	for _, datasetId := range params.Datasets {
		// 		ret, err := gexdb.FindSeqValues(datasetId, params.ExprType, params.Genes, isAdmin, user.Permissions)

		// 		if err != nil {
		// 			c.Error(err)
		// 			return
		// 		}

		// 		results = append(results, ret)
		// 	}
		// }

		web.MakeDataResp(c, "", results)
	})
}
