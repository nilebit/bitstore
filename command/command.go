package command

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

var Commands = []*Command{
	Version,
}

type Command struct {
	Run       func(cmd *Command, args []string) bool
	UsageLine string
	Short     string
	Long      string
	Flag      flag.FlagSet
}

func (m *Command) Name() string {
	name := m.UsageLine
	i := strings.Index(name, " ")
	if i >= 0 {
		name = name[:i]
	}
	return name
}

func (m *Command) Usage() {
	fmt.Fprintf(os.Stderr, "Example: %s\n", m.UsageLine)
	fmt.Fprintf(os.Stderr, "Default Usage:\n")
	m.Flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "Description:\n")
	fmt.Fprintf(os.Stderr, "  %s\n", strings.TrimSpace(m.Long))
	os.Exit(2)
}

func (m *Command) Runnable() bool {
	return m.Run != nil
}
