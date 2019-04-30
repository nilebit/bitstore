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
	Short:     "Print Version",
	Long:      `Version prints the storemain`,
}

func runVersion(cmd *Command, args []string) bool {
	if len(args) != 0 {
		cmd.Usage()
	}

	fmt.Printf("Version %s %s %s\n", VERSION, runtime.GOOS, runtime.GOARCH)
	return true
}
