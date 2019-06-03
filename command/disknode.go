package command

import (
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/golang/glog"
	"github.com/nilebit/bitstore/server/diskserver"
)

var DNModule = &Command{
	UsageLine: "disk -port=8001",
	Short:     "start a disk node server",
	Long:      `start a disk node server to provide bitstore spaces`,
}

var (
	dn = diskserver.NewDiskServer()
	Folders *string
	ManageNode *string
	FolderMaxLimits *string
)

func init() {
	DNModule.Run = RunDN
	dn.Port = DNModule.Flag.Int("port", 8080, "http listen port")
	dn.Ip = DNModule.Flag.String("ip", "0.0.0.0", "ip or server name")
	dn.Cluster = DNModule.Flag.String("cluster", "localhost:8000", "cluster server location")
	dn.MaxCpu = DNModule.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	dn.DataCenter = DNModule.Flag.String("dataCenter", "", "current dss server's data center name")
	dn.Rack = DNModule.Flag.String("rack", "", "current volume server's rack name")
	dn.Debug = DNModule.Flag.Bool("debug", false, "open debug")
	ManageNode = DNModule.Flag.String("manage", "localhost:8000", "comma-separated manage node servers. manageNode1[,manageNode2]...")
	Folders = DNModule.Flag.String("dir", os.TempDir(), "directories to store data files. dir[,dir]...")
	FolderMaxLimits = DNModule.Flag.String("max", "1", "maximum numbers of File, count[,count]...")
}

func RunDN(md *Command, args []string) (ret bool) {

	if *dn.MaxCpu < 1 {
		*dn.MaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*dn.MaxCpu)
	dn.ManageNode = strings.Split(*ManageNode, ",")
	dn.Folders = strings.Split(*Folders, ",")

	var tempFolderMaxLimits = strings.Split(*FolderMaxLimits, ",")
	for _, maxString := range tempFolderMaxLimits {
		if max, e := strconv.Atoi(maxString); e == nil {
			dn.FolderMaxLimits = append(dn.FolderMaxLimits, max)
		} else {
			glog.Fatalf("The max specified in -max not a valid number %s", maxString)
		}
	}

	if len(dn.Folders) != len(dn.FolderMaxLimits) {
		glog.Fatalf("%d directories by -dir, but only %d max is set by -max", len(dn.Folders), len(dn.FolderMaxLimits))
	}

	dn.RegistRouter()
	dn.CreateDiskOpt()

	ret = dn.StartServer()

	return ret
}
