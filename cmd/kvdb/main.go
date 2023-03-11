package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/logging"

	. "github.com/streamingfast/cli"
	_ "github.com/streamingfast/kvdb/store/badger"
	_ "github.com/streamingfast/kvdb/store/badger3"
	_ "github.com/streamingfast/kvdb/store/bigkv"
	_ "github.com/streamingfast/kvdb/store/netkv"
	_ "github.com/streamingfast/kvdb/store/tikv"
)

// Commit sha1 value, injected via go build `ldflags` at build time
var commit = ""

// Version value, injected via go build `ldflags` at build time
var version = "dev"

// Date value, injected via go build `ldflags` at build time
var date = ""

var zlog, tracer = logging.RootLogger("kvdb", "github.com/streamingfast/kvdb/cmd/kvdb")

var RootCmd = &cobra.Command{
	Use: "kvdb", Short: "",
}

func init() {
	logging.InstantiateLoggers()
}

func main() {
	Run("substreams-sink-kv", "KVDB Client",
		ConfigureViper("KVDB"),
		ConfigureVersion(),

		Group("read", "KVDB read commands",
			ReadGetCmd,
			ReadScanCmd,

			PersistentFlags(
				func(flags *pflag.FlagSet) {
					flags.String("decoder", "hex", "output decoding. Supported schemes: 'hex', 'ascii', 'proto:<path_to_proto>'")
				},
			),
		),

		PersistentFlags(
			func(flags *pflag.FlagSet) {
				flags.String("dsn", "", "URL to connect to the KV store. Supported schemes: 'badger3', 'badger', 'bigkv', 'tikv', 'netkv'. See https://github.com/streamingfast/kvdb for more details. (ex: 'badger3:///tmp/substreams-sink-kv-db')")
			},
		),
		AfterAllHook(func(cmd *cobra.Command) {
			cmd.PersistentPreRunE = func(_ *cobra.Command, _ []string) error {
				return nil
			}
		}),
	)
}

func ConfigureVersion() CommandOption {
	return CommandOptionFunc(func(cmd *cobra.Command) {
		cmd.Version = versionString(version)
	})
}

func versionString(version string) string {
	var labels []string
	if len(commit) >= 7 {
		labels = append(labels, fmt.Sprintf("Commit %s", commit[0:7]))
	}

	if date != "" {
		labels = append(labels, fmt.Sprintf("Built %s", date))
	}

	if len(labels) == 0 {
		return version
	}

	return fmt.Sprintf("%s (%s)", version, strings.Join(labels, ", "))
}
