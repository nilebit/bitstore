package manageserver

var DefaultCenterName = "DefaultCenter"

type DataCenter struct {
	baseNodeInfo
	DataRack 			  map[string]*DataRack    `json:"Racks"`
}

func NewDataCenter() *DataCenter {
	return &DataCenter{DataRack: map[string]*DataRack{}}
}