package command

import (
	"fmt"
	"runtime"
)

const (
	VERSION = "0.1"
)

var Version = &Command{
	Run:       runVersion,
	UsageLine: "version",
	Short:     "print version",
	Long:      `version prints the bitstore`,
}

func runVersion(cmd *Command, args []string) bool {
	if len(args) != 0 {
		cmd.Usage()
	}

	fmt.Printf("Version %s %s %s\n", VERSION, runtime.GOOS, runtime.GOARCH)
	return true
}
