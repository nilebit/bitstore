package storemain

import (
	"flag"
	"fmt"
	"html/template"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/golang/glog"
	"github.com/huangchunhua/bitstore/command"
	"github.com/huangchunhua/bitstore/logs"
)

var modules = command.Commands

var exitStatus = 0
var exitMu sync.Mutex

func setExitStatus(n int) {
	exitMu.Lock()
	if exitStatus < n {
		exitStatus = n
	}
	exitMu.Unlock()
}

var usageTemplate = `
Bitstore

Usage:

	 storemain module [arguments]

The module are:
{{range .}}{{if .Runnable}}
    {{.Name | printf "%-11s"}} {{.Short}}{{end}}{{end}}

Use "storemain help [command]" for more information about a command.

`

var helpTemplate = `{{if .Runnable}}Usage: storemain {{.UsageLine}}
{{end}}
  {{.Long}}
`

func capitalize(s string) string {
	if s == "" {
		return s
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToTitle(r)) + s[n:]
}

func tmpl(w io.Writer, text string, data interface{}) {
	t := template.New("storemain")
	t.Funcs(template.FuncMap{"trim": strings.TrimSpace, "capitalize": capitalize})
	template.Must(t.Parse(text))
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
}

func printUsage(w io.Writer) {
	tmpl(w, usageTemplate, modules)
}

func usage() {
	printUsage(os.Stderr)
	fmt.Fprintf(os.Stderr, "For Logging, use \"storemain [logging_options] [command]\". The logging options are:\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func help(args []string) {
	if len(args) == 0 {
		printUsage(os.Stdout)
		return
	}
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: storemain help command\n\nToo many arguments given.\n")
		os.Exit(2)
	}

	arg := args[0]

	for _, md := range modules {
		if md.Name() == arg {
			tmpl(os.Stdout, helpTemplate, md)
			return
		}
	}

	fmt.Fprintf(os.Stderr, "Bitstore：Unknown help topic %#q.  Run 'storemain help'.\n", arg)
	os.Exit(2)
}

var atexitFuncs []func()

func exit() {
	for _, f := range atexitFuncs {
		f()
	}
	glog.Flush()
	os.Exit(exitStatus)
}

func Main() {
	rand.Seed(time.Now().UnixNano())

	glog.MaxSize = 1024 * 1024 * 1024
	logs.InitLogs()
	defer logs.FlushLogs()

	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	if args[0] == "help" {
		help(args[1:])
		for _, md := range modules {
			if len(args) >= 2 && md.Name() == args[1] && md.Run != nil {
				fmt.Fprintf(os.Stderr, "Default Parameters:\n")
				md.Flag.PrintDefaults()
			}
		}
		return
	}

	for _, md := range modules {
		if md.Name() == args[0] && md.Run != nil {
			md.Flag.Usage = func() { md.Usage() }
			md.Flag.Parse(args[1:])
			args = md.Flag.Args()
			if !md.Run(md, args) {
				fmt.Fprintf(os.Stderr, "\n")
				md.Flag.Usage()
				fmt.Fprintf(os.Stderr, "Default Parameters:\n")
				md.Flag.PrintDefaults()
			}
			exit()
			return
		}
	}
	fmt.Fprintf(os.Stderr, "Bitstore: unknown module %q\nRun 'storemain help' for usage.\n", args[0])
	setExitStatus(2)
	exit()
}
