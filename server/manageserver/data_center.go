package manageserver

var DefaultCenterName = "DefaultDataCenter"

type DataCenter struct {
	baseNodeInfo
	dataRack 			  map[string]*DataRack
}

func NewDataCenter() *DataCenter {
	return &DataCenter{dataRack: map[string]*DataRack{}}
}