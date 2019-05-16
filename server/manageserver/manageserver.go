package manageserver

type ManageNodeMapper interface {
	RegistRouter()
	CreateDiskOpt()
	StartServer() bool
}

type ManageServer struct {
	port                    int
	metaFolder              string
	volumeSizeLimitMB       uint
	preallocate             int64
	pulseSeconds            int
	defaultReplicaPlacement string
	garbageThreshold        string
}

func NewManageServer() *ManageServer {
	return &ManageServer{}
}
