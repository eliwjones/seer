package main

import (
	"github.com/eliwjones/seer"

	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"time"
)

var (
	secret = "dontkeepsecrets"

	hostIP  = flag.String("ip", "", "REQUIRED! IP address to run UDP, HTTP servers on.")
	udpPort = flag.Int("udp", 9999, "<port> to use for UDP server. Default is: 9999")
	tcpPort = flag.Int("tcp", 9998, "<port> to use for HTTP server.  Default is: 9998")
	seeds   = flag.String("seeds", "", "Comma separated list of udp addresses for other peers.")
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
	runtime.GOMAXPROCS(int(runtime.NumCPU() / 2))
	flag.Parse()

	if *hostIP == "" {
		fmt.Printf("Please pass -ip=W.X.Y.Z or -ip=W:X:Y:Z (for ipv6)\n")
		os.Exit(1)
	}

	ip6Parts := strings.Split(*hostIP, ":")
	if len(ip6Parts) > 1 {
		if !strings.HasPrefix(*hostIP, "[") {
			*hostIP = "[" + *hostIP
		}
		if !strings.HasSuffix(*hostIP, "]") {
			*hostIP = *hostIP + "]"
		}
	}
}

func main() {
	var s = seer.New(*hostIP, *udpPort, *tcpPort, secret, strings.Split(*seeds, ","))
	s.Run()
}
