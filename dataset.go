package gex

type Dataset struct {
	PublicId    string    `json:"publicId"`
	Name        string    `json:"name"`
	Species     string    `json:"species"`
	Technology  string    `json:"technology"`
	Platform    string    `json:"platform"`
	Path        string    `json:"-"`
	Institution string    `json:"institution"`
	Description string    `json:"description"`
	Samples     []*Sample `json:"samples"`
	Id          int       `json:"id"`
}
