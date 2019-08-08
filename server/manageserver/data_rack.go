package manageserver

var DefaultRackName = "DefaultRack"

type DataRack struct {
	baseNodeInfo
	dataNode 	      	  map[string]*DataNode
}

func NewRack() *DataRack {
	return &DataRack{dataNode: map[string]*DataNode{}}
}