package command

import (
	"runtime"

	"github.com/golang/glog"
	"github.com/nilebit/bitstore/server/manageserver"
	"github.com/nilebit/bitstore/util"
)

var mn = manageserver.NewManageServer()

var MNModule = &Command{
	UsageLine: "manage -port=8000",
	Short:     "start a manage node server",
	Long:      `start a manage node server to provide bitstore spaces`,
}

func init() {
	MNModule.Run = RunMN
	mn.ID = MNModule.Flag.Int("id", 1, "server id")
	mn.Port = MNModule.Flag.Int("port", 8080, "http listen port")
	mn.Ip = MNModule.Flag.String("ip", "127.0.0.1", "ip or server name")
	mn.Peers = MNModule.Flag.String("peers", "", "all manage nodes in comma separated ip:port list, example: 127.0.0.1:9093,127.0.0.1:9094")
	mn.VolumeSizeLimitMB = MNModule.Flag.Uint("volumeSizeLimitMB", 30*1000, "Manage stops directing writes to oversized volumes.")
	mn.MetaFolder = MNModule.Flag.String("mdir", "./data", "data directory to store meta data")
	mn.MaxCpu = MNModule.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
}

func RunMN(cmd *Command, args []string) bool {
	if *mn.MaxCpu < 1 {
		*mn.MaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*mn.MaxCpu)

	if err := util.TestFolderWritable(*mn.MetaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", *mn.MetaFolder, err)
	}
	mn.RegistRouter()
	mn.StartServer()

	return true
}
