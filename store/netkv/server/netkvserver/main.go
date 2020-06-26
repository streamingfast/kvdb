package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	_ "github.com/dfuse-io/kvdb/store/badger"
	netkvserver "github.com/dfuse-io/kvdb/store/netkv/server"
)

// This is a simple, example server for hosting a NetKV, backed (most
// probably by) Badger storage.

var flagBackendDSN = flag.String("backend-dsn", "badger://./netkv", "KVDB storage backing this NetKV instance")
var flagListenAddr = flag.String("listen-addr", ":65211", "gRPC listening address (insecure)")

func main() {
	flag.Parse()

	pwd, _ := os.Getwd()
	backendDSN := strings.Replace(*flagBackendDSN, "//./", fmt.Sprintf("//%s/", pwd), 1)

	srv, err := netkvserver.New(*flagListenAddr, backendDSN)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	fmt.Println("Listening", *flagListenAddr)

	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals

	fmt.Println("Shutting down")

	if err := srv.Close(); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Done")
	}
}
