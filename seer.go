package main

import (
        "encoding/json"
        "errors"
        "flag"
        "fmt"
        "io/ioutil"
        "log"
        "math/rand"
        "net"
        "net/http"
        "os"
        "runtime"
        "strings"
        "time"
)

type Gossip struct {
        SeerAddr    string
        ServiceName string
        ServiceAddr string
        Tombstone   bool
        TS          int64
}

var (
        SeerRoot       string
        SeerOpDir      string
        SeerDataDir    string
        SeerServiceDir string
        SeerHostDir    string
        udpAddress     = flag.String("udp", "localhost:9999", "<host>:<port> to use for UDP server.")
        tcpAddress     = flag.String("tcp", "localhost:9998", "<host>:<port> to use for UDP server.")
        commandList    = map[string]bool{"exit": true, "get": true}
)

func main() {
        runtime.GOMAXPROCS(int(runtime.NumCPU() / 2))
        flag.Parse()

        /* Guarantee folder paths */
        SeerRoot = "seer/" + *udpAddress
        SeerOpDir = SeerRoot + "/gossip/oplog"
        SeerDataDir = SeerRoot + "/gossip/data"
        SeerServiceDir = SeerDataDir + "/service"
        SeerHostDir = SeerDataDir + "/host"
        os.MkdirAll(SeerOpDir, 0777)
        os.MkdirAll(SeerDataDir, 0777)

        /* Single cleanup on start. */
        TombstoneReaper()
        AntiEntropy()

        /* Set up channels and start servers. */
        udpServerChannel := make(chan string)
        serviceServerChannel := make(chan string)

        go UDPServer(udpServerChannel, *udpAddress)
        go ServiceServer(*tcpAddress)

        /* Run background cleanup on 10 second cycle. */
        BackgroundLoop("Janitorial Work", 10, TombstoneReaper, AntiEntropy)

        /* Run background routine for GossipOps() */
        //BackgroundLoop("Gossip Oplog", 1, GossipOps)

        /* Wait for and handle any messages. */
        for {
                select {
                case message := <-udpServerChannel:
                        if message == "exit" {
                                fmt.Printf("\nBYE BYE\n")
                                return
                        } else if message == "get" {
                                fmt.Printf("\nGOT A 'GET' COMMAND!\n")
                        }
                case message := <-serviceServerChannel:
                        fmt.Printf("\nServiceServer message: %s\n", message)
                }
        }
}

func BackgroundLoop(name string, seconds int, fns ...func()) {
        go func() {
                for {
                        time.Sleep(time.Duration(seconds) * time.Second)
                        fmt.Printf("\n[%s] : LOOPING : %s\n", time.Now(), name)
                        for _, f := range fns {
                                f()
                        }
                }
        }()
}

func AntiEntropy() {
        /*
           1. Wrapped by background jobs. (Sleep Q seconds then do.)
           2. Check random host gossip/source (or gossip/services) zip checksum.
           3. Sync if checksum no match.

        */
        fmt.Printf("[AntiEntropy] I grab fill copies of other host dbs occassionally.\n")
}

func TombstoneReaper() {
        /*
           1. Wrapped by background jobs. (Sleep Q seconds then do.)
           2. Remove anything but most recent doc that is older than P seconds.
           3. Remove remaining docs if they are Tombstoned and older than certain time.
        */
        fmt.Printf("[TombstoneReaper] I remove stuff that has been deleted or older timestamped source docs.\n")
}

func ServiceServer(address string) {
        fmt.Printf("[ServiceServer] Can query for service by name.\nI listen on %s", address)
        http.HandleFunc("/", ServiceHandler)
        http.ListenAndServe(address, nil)
}

func ServiceHandler(w http.ResponseWriter, r *http.Request) {
        requestTypeMap := map[string]string{
                "host":    "seer",
                "seer":    "seer",
                "service": "service",
        }
        /* Allow GET by 'service' or 'seeraddr' */
        splitURL := strings.Split(r.URL.Path, "/")
        requestType := splitURL[1]
        requestQuery := splitURL[2]
        if requestTypeMap[requestType] == "" {
                http.Error(w, "Please query for 'seer' or 'service'", 404)
                return
        }
        jsonPayload := getServiceData(requestQuery, requestTypeMap[requestType])
        fmt.Fprintf(w, jsonPayload)
}

func getServiceData(name string, requestType string) string {
        var servicePath string
        if requestType == "service" {
                servicePath = SeerServiceDir + "/" + name
        } else if requestType == "seer" {
                servicePath = SeerHostDir + "/" + name
        }

        serviceHosts, _ := ioutil.ReadDir(servicePath)
        /* read files from servicePath, append to jsonPayload. */
        jsonPayload := ""
        for _, serviceHost := range serviceHosts {
                if serviceHost.IsDir() {
                        continue
                }
                serviceData, err := ioutil.ReadFile(servicePath + "/" + serviceHost.Name())
                if err == nil {
                        jsonPayload += fmt.Sprintf("%s,", string(serviceData))
                } else {
                        jsonPayload += fmt.Sprintf("ERROR: %s\nFILE: %s", err, serviceHost.Name())
                }
        }
        if len(jsonPayload) > 0 {
                jsonPayload = jsonPayload[:len(jsonPayload)-1]
        }
        return "[" + jsonPayload + "]"
}

func UDPServer(ch chan<- string, address string) {
        /*
           1. Listen for gossip.
           2. Possibly handle special messages?
             - Someone heard you were down?
        */
        udpLn, err := net.ListenPacket("udp", address)
        if err != nil {
                log.Fatal(err)
        }
        defer udpLn.Close()
        udpBuff := make([]byte, 128)
        for {
                n, _, err := udpLn.ReadFrom(udpBuff)
                if err != nil {
                        log.Fatal(err)
                }
                message := strings.Trim(string(udpBuff[:n]), "\n")
                if commandList[message] {
                        ch <- message
                } else {
                        go ProcessGossip(message)
                }
        }
}

func ProcessGossip(gossip string) {
        /*
           1. Write Gossip to gossiplog.
           2. Share gossip?
        */

        decodedGossip, err := VerifyGossip(gossip)
        if err != nil || decodedGossip.SeerAddr == "" {
                fmt.Printf("\nBad Gossip!: %s\nErr: %s\n", gossip, err)
                return
        }
        /* Write to oplog. opName limits updates from single host to 10 per nanosecond.. */
        opName := fmt.Sprintf("%d_%s_%d", time.Now().UnixNano(), decodedGossip.SeerAddr, (rand.Int()%10)+10)
        err = LazyWriteFile(SeerOpDir, opName+".op", gossip)
        if err != nil {
                /* Not sure want to panic here.  Move forward at all costs? */
                panic(err)
        }
        /* Save it. */
        PutGossip(gossip, decodedGossip)

        /* Spread the word? Or, use GossipOps()? */
        // GossipGossip(gossip)
}

func VerifyGossip(gossip string) (Gossip, error) {
        /*
           { "SeerAddr" : "remotehost1:9999",
             "ServiceName" : "mongod",
             "ServiceAddr" : "remotehost1:2107",
             "TS" : 123312123123 }
        */
        var g Gossip
        err := json.Unmarshal([]byte(gossip), &g)

        /* Reflection feels stupid, so bruteforcing it. */
        /* A Heartbeat is just {SeerAddr,ts}.. so that's all need check. */
        errorStr := ""
        if g.SeerAddr == "" {
                errorStr += ",SeerAddr"
        }
        if g.TS == 0 {
                errorStr += ",TS"
        }
        if err == nil && errorStr != "" {
                err = errors.New(fmt.Sprintf("[%s] are missing!", errorStr[1:len(errorStr)]))
        }
        return g, err
}

func PutGossip(gossip string, decodedGossip Gossip) {
        /* Proper approach is to verify gossip.TS > current.TS */
        /* Current code has odd side effect of allowing Seer data to exist for host even if we missed initial Seer HELO gossip. */
        /* We leave it to AntiEntropy to patch that up. */
        if decodedGossip.ServiceName == "" {
                decodedGossip.ServiceName = "Seer"
        }
        err := LazyWriteFile(SeerServiceDir+"/"+decodedGossip.ServiceName, decodedGossip.SeerAddr, gossip)
        if err != nil {
                fmt.Printf("Could not PutGossip! Error:\n%s", err)
        }
        err = LazyWriteFile(SeerHostDir+"/"+decodedGossip.SeerAddr, decodedGossip.ServiceName, gossip)
        if err != nil {
                fmt.Printf("Could not PutGossip! Error:\n%s", err)
        }
}

func LazyWriteFile(folderName string, fileName string, data string) error {
        err := ioutil.WriteFile(folderName+"/"+fileName, []byte(data), 0777)
        if err != nil {
                os.MkdirAll(folderName, 0777)
                err = ioutil.WriteFile(folderName+"/"+fileName, []byte(data), 0777)
        }
        return err
}

func GossipOps() {
        /*
           1. Pick N random peers.
           2. Gossip all ops that have occured since last GossipOps() to peers.
           3. Ops examined by file create date and encoded "ts"?
        */
        fmt.Printf("[GossipOps] I grab all (plus some padding?) ops that have appeared since last GossipOps()")
}

func GossipGossip(gossip string) {
        fmt.Printf("[GossipGossip] Gossiping Gossip: %s", gossip)
        /*
           if HostList > M:
             return
           Add self to Gossip HostList.
           for N random Gossipees:
             send Gossip
        */
}
