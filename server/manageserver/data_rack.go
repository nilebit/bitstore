package manageserver

var DefaultRackName = "DefaultRack"

type DataRack struct {
	baseNodeInfo
	DataNode 	      	  map[string]*DataNode  `json:"DataNodes"`
}

func NewRack() *DataRack {
	return &DataRack{DataNode: map[string]*DataNode{}}
}