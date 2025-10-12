package gex

type (
	ExprType struct {
		PublicId string `json:"publicId"`
		Name     string `json:"name"`
		Id       uint   `json:"id"`
	}

	Dataset struct {
		PublicId    string      `json:"publicId"`
		Name        string      `json:"name"`
		Species     string      `json:"species"`
		Technology  string      `json:"technology"`
		Platform    string      `json:"platform"`
		Path        string      `json:"-"`
		Institution string      `json:"institution"`
		Description string      `json:"description"`
		Samples     []*Sample   `json:"samples"`
		ExprTypes   []*ExprType `json:"exprTypes"`
		Id          uint        `json:"id"`
	}
)

const (
	ExprTypesSQL = `SELECT
		expr_types.id,
		expr_types.public_id,
		expr_types.name
		FROM expr_types
		ORDER BY expr_types.id`
)

func NewDataset(id uint,
	publicId string,
	species string,
	technology string,
	platform string,
	institution string,
	name string,
	description string,
	path string) *Dataset {

	return &Dataset{Id: id,
		PublicId:    publicId,
		Species:     species,
		Technology:  technology,
		Platform:    platform,
		Institution: institution,
		Name:        name,
		Description: description,
		Path:        path,
		ExprTypes:   make([]*ExprType, 0, 5),
	}

}
