package command

import (
	"runtime"

	"github.com/golang/glog"
	"github.com/nilebit/bitstore/server/manageserver"
	"github.com/nilebit/bitstore/util"
)

var mn = manageserver.NewManageServer()

// MNModule manage node command
var MNModule = &Command{
	UsageLine: "manage -port=8000",
	Short:     "start a manage node server",
	Long:      `start a manage node server to provide bitstore spaces`,
}

func init() {
	MNModule.Run = runMN
	mn.Advertise = MNModule.Flag.String("advertise-urls", "http://127.0.0.1:8200", "list of URLs to listen on for peer traffic.")
	mn.Cluster = MNModule.Flag.String("cluster-urls", "http://127.0.0.1:8200", "cluster configuration for bootstrapping. example: http://10.0.0.1:8100,http://10.0.0.2:8100")
	mn.VolumeSizeLimitMB = MNModule.Flag.Uint("volumeSizeLimitMB", 30000, "Manage stops directing writes to oversized volumes.")
	mn.MetaFolder = MNModule.Flag.String("mdir", "./data", "data directory to store meta data")
	mn.MaxCPU = MNModule.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
}

// runMN runing manage node
func runMN(cmd *Command, args []string) bool {
	if *mn.MaxCPU < 1 {
		*mn.MaxCPU = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*mn.MaxCPU)

	if err := util.TestFolderWritable(*mn.MetaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", *mn.MetaFolder, err)
	}
	mn.RegistRouter()
	mn.StartServer()

	return true
}
