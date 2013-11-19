package main

import (
        "code.google.com/p/go.net/ipv4"

        "archive/tar"
        "bytes"
        "compress/gzip"
        "encoding/json"
        "errors"
        "flag"
        "fmt"
        "io"
        "io/ioutil"
        "log"
        "math/rand"
        "net"
        "net/http"
        "os"
        "path/filepath"
        "regexp"
        "runtime"
        "strconv"
        "strings"
        "time"
)

type Gossip struct {
        SeerAddr    string
        ServiceName string
        ServiceAddr string
        SeerRequest string
        Tombstone   bool
        TS          int64
}

var (
        SeerRoot        string
        SeerOpDir       string
        SeerDataDir     string
        SeerServiceDir  string
        SeerHostDir     string
        udpAddress      string
        tcpAddress      string
        hostIP          = flag.String("ip", "", "REQUIRED! IP address to communicate with other Seers from.")
        udpPort         = flag.String("udp", "9999", "<port> to use for UDP server. Default is: 9999")
        tcpPort         = flag.String("tcp", "9998", "<port> to use for HTTP server.  Default is: 9998")
        bootstrap       = flag.String("bootstrap", "", "<host>:<udp port> for Seer Host to request seed from. Use -bootstrap=magic to search.")
        listenBroadcast = flag.Bool("listenbroadcast", true, "Can disable listening to broadcast UDP packets.  Useful for testing multiple IPs on same machine.")
        commandList     = map[string]bool{"exit": true, "get": true}
        tsRegexp        = regexp.MustCompile(`[,|{]\s*"TS"\s*:\s*(\d+)\s*[,|}]`)
        gossipSocket    *net.UDPConn
        lastSeedTS      int64
)

func main() {
        runtime.GOMAXPROCS(int(runtime.NumCPU() / 2))
        flag.Parse()

        if *hostIP == "" {
                fmt.Printf("Please pass -ip=W.X.Y.Z\n")
                return
        }

        udpAddress = *hostIP + ":" + *udpPort
        tcpAddress = *hostIP + ":" + *tcpPort

        /* Guarantee folder paths */
        SeerRoot = "seer/" + udpAddress
        SeerOpDir = SeerRoot + "/gossip/oplog"
        SeerDataDir = SeerRoot + "/gossip/data"
        SeerServiceDir = SeerDataDir + "/service"
        SeerHostDir = SeerDataDir + "/host"
        os.MkdirAll(SeerOpDir, 0777)
        os.MkdirAll(SeerDataDir, 0777)

        /* Single cleanup on start. */
        //TombstoneReaper()
        //AntiEntropy()

        /* Set up channels and start servers. */
        udpServerChannel := make(chan string)
        serviceServerChannel := make(chan string)

        go UDPServer(udpServerChannel, *hostIP, *udpPort)
        go ServiceServer(tcpAddress)

        /* Not really sure how want to build this out. */
        createGossipSocket()

        if *bootstrap != "" {
                BootStrap(*bootstrap, tcpAddress)
        }

        /* Run background cleanup on 10 second cycle. */
        //BackgroundLoop("Janitorial Work", 10, TombstoneReaper, AntiEntropy)

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

func createGossipSocket() {
        c, err := net.ListenPacket("udp4", ":0")
        if err != nil {
                fmt.Printf("[createGossipSocket] ERR: %s\n", err)
                os.Exit(1)
        }
        gossipSocket = c.(*net.UDPConn)
}

func SendGossip(gossip string, seerAddr string) {
        seer, err := net.ResolveUDPAddr("udp4", seerAddr)
        if err != nil {
                fmt.Printf("[SendGossip] ERR: %s\n", err)
                return
        }
        fmt.Printf("[SendGossip] gossip: %s\n    TO: %s\n", gossip, seer)
        gossipSocket.WriteToUDP([]byte(gossip), seer)
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

func BootStrap(seeder string, seedee string) {
        /* Sexier */
        if seeder == "magic" {
                /* Broadcast to whoever.  Must still handle handshake so that not all Seers send their data. */
                seeder = "255.255.255.255:9999"
        }
        if strings.HasPrefix(seeder, "255.") {
                /* If Broadcasting, must send udpAddress so they can send back "SeedYou" query. */
                seedee = udpAddress
        }
        message := fmt.Sprintf(`{"SeerAddr":"%s","SeerRequest":"SeedMe"}`, seedee)
        SendGossip(message, seeder)
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
        fmt.Printf("[ServiceServer] Can query for service by name.\nI listen on %s\n", address)
        http.HandleFunc("/", ServiceHandler)
        http.ListenAndServe(address, nil)
}

func errorResponse(w http.ResponseWriter, message string) {
        w.WriteHeader(http.StatusInternalServerError)
        w.Write([]byte(message))
}

func ServiceHandler(w http.ResponseWriter, r *http.Request) {
        requestTypeMap := map[string]string{
                "host":    "seer",
                "seer":    "seer",
                "service": "service",
        }
        /* Allow GET by 'service' or 'seeraddr' */
        splitURL := strings.Split(r.URL.Path, "/")

        switch r.Method {
        case "GET":
                if requestTypeMap[splitURL[1]] == "" {
                        errorResponse(w, "Please query for 'seer' or 'service'")
                        return
                }
                jsonPayload := getServiceData(splitURL[2], requestTypeMap[splitURL[1]])
                fmt.Fprintf(w, jsonPayload)
        case "PUT":
                if splitURL[1] != "seed" {
                        errorResponse(w, "I only accept PUTs for 'seed'")
                }
                path := fmt.Sprintf("/tmp/seer_received_seed_%s_%d.tar.gz", udpAddress, time.Now().Unix())
                file, err := os.Create(path)
                if err != nil {
                        errorResponse(w, err.Error())
                        return
                }
                defer file.Close()
                _, err = io.Copy(file, r.Body)
                if err != nil {
                        errorResponse(w, err.Error())
                        return
                }
                /* Made it here.. now need to do go routine to unzip. */
                go processSeed(path)
        }
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

// Once receive UDP notification that seerDestinationAddr needs seed
// tar gz 'host' and 'service' dirs and PUT to seerDestinationAddr.
func sendSeed(seerDestinationAddr string) {
        tarredGossipFile := fmt.Sprintf("/tmp/seer_generated_seed_%s_%s.tar.gz", udpAddress, seerDestinationAddr)
        err := createTarGz(tarredGossipFile, SeerServiceDir, SeerHostDir)
        if err != nil {
                fmt.Printf("[sendSeed] Failed to create tar.gz. ERR: %s\n", err)
                return
        }
        /* Open file */
        rbody, _ := os.Open(tarredGossipFile)
        /* Put file */
        request, err := http.NewRequest("PUT", "http://"+seerDestinationAddr+"/seed", rbody)
        if err != nil {
                fmt.Printf("[sendSeed] ERROR MAKING REQUEST: %s\n", err)
                return
        }
        stat, err := rbody.Stat()
        if err != nil {
                fmt.Printf("[sendSeed] ERROR WITH STAT(): %s\n", err)
                return
        } else {
                request.ContentLength = stat.Size()
        }
        response, err := http.DefaultClient.Do(request)
        fmt.Printf("[sendSeed] RESPONSE: %s\nERR: %s\n", response, err)
}

// Zips up seer/<hostname>/gossip/data/ (aka SeerDataDir)
// General usage is targeting 'host' and 'service' subfolders.
// Removes SeerDataDir root for "easier" untarring into destination.
// TODO: make less stupid.
func createTarGz(tarpath string, folders ...string) error {
        tarfile, err := os.Create(tarpath)
        if err != nil {
                return err
        }
        defer tarfile.Close()
        gw, err := gzip.NewWriterLevel(tarfile, gzip.BestCompression)
        if err != nil {
                return err
        }
        defer gw.Close()
        tw := tar.NewWriter(gw)
        defer tw.Close()
        for _, folder := range folders {
                filepath.Walk(folder, func(path string, fileinfo os.FileInfo, err error) error {
                        if err != nil {
                                fmt.Printf("[createTarGz] Err: %s\n", err)
                                return err
                        }
                        if fileinfo.IsDir() {
                                return nil
                        }
                        header, err := tar.FileInfoHeader(fileinfo, path)
                        header.Name = path[len(SeerDataDir)+1:]
                        err = tw.WriteHeader(header)
                        if err != nil {
                                fmt.Printf("Failed to write Tar Header. Err: %s\n", err)
                                return err
                        }
                        /* Add file. */
                        file, err := os.Open(path)
                        if err != nil {
                                fmt.Printf("Failed to open path: [%s] Err: %s\n", path, err)
                                return err
                        }
                        defer file.Close()
                        _, err = io.Copy(tw, file)
                        if err != nil {
                                fmt.Printf("Failed to copy file into tar: [%s] Err: %s\n", path, err)
                                return err
                        }
                        return nil
                })
        }

        return err
}

func processSeed(targzpath string) {
        lastSeedTS = time.Now().UnixNano()
        fmt.Printf("[processSeed] lastSeedTS: %s\n", lastSeedTS)
        targzfile, err := os.Open(targzpath)
        if err != nil {
                lastSeedTS = 0
                return
        }
        defer targzfile.Close()
        gzr, err := gzip.NewReader(targzfile)
        if err != nil {
                lastSeedTS = 0
                return
        }
        defer gzr.Close()
        tr := tar.NewReader(gzr)
        for {
                hdr, err := tr.Next()
                if err == io.EOF {
                        break
                }
                if err != nil {
                        fmt.Printf("[processSeed] tr ERR: %s\n", err)
                        break
                }
                buf := bytes.NewBuffer(nil)
                io.Copy(buf, tr)
                folder := filepath.Dir(SeerDataDir + "/" + hdr.Name)
                filename := filepath.Base(folder)
                err = LazyWriteFile(folder, filename, buf.Bytes())
                if err != nil {
                        fmt.Printf("[processSeed] ERRR: %s", err)
                }
        }
        return
}

func UDPServer(ch chan<- string, ipAddress string, port string) {
        /*
           "Have" to hack this since want to receive broadcast packets.. yet.. they don't appear to show up
           if net.ListenPacket() gets called on specific IP address?
           Really annoying since prevents listening on same port using different IP addresses on same machine.

           Thus, I have added more hack.
        */
        var err error
        var c net.PacketConn

        if *listenBroadcast {
                c, err = net.ListenPacket("udp", ":"+port)
        } else {
                c, err = net.ListenPacket("udp", udpAddress)
        }
        if err != nil {
                log.Fatal(err)
        }
        defer c.Close()
        udpLn := ipv4.NewPacketConn(c)
        err = udpLn.SetControlMessage(ipv4.FlagDst, true)
        if err != nil {
                log.Fatal(err)
        }
        udpBuff := make([]byte, 256)
        for {
                n, cm, _, err := udpLn.ReadFrom(udpBuff)
                if err != nil {
                        log.Fatal(err)
                }
                message := strings.Trim(string(udpBuff[:n]), "\n")
                if commandList[message] {
                        ch <- message
                } else {
                        go ProcessGossip(message, cm.Src.String(), cm.Dst.String())
                }
        }
}

func ProcessGossip(gossip string, sourceIp string, destinationIp string) {
        /*
           1. Write Gossip to gossiplog.
           2. Share gossip?
        */

        decodedGossip, err := VerifyGossip(gossip)
        /* Handle any broadcasted commands first, since basic gossip is not allowed to be broadcast. */
        if decodedGossip.SeerRequest == "SeedMe" && strings.HasPrefix(destinationIp, "255.") {
                /* If receive broadcast, send back "SeedYou" query. */
                if udpAddress == decodedGossip.SeerAddr {
                        /* No need to seed oneself. */
                        /* Might be sexier to have SendGossip() block gossip to self? */
                        return
                }
                message := fmt.Sprintf(`{"SeerAddr":"%s","SeerRequest":"SeedYou"}`, udpAddress)
                SendGossip(message, decodedGossip.SeerAddr)
                return
        } else if decodedGossip.SeerRequest == "SeedMe" {
                /* Received direct "SeedMe" request.  Send seed. */
                sendSeed(decodedGossip.SeerAddr)
                return
        } else if decodedGossip.SeerRequest == "SeedYou" {
                /* Seer thinks I want a Seed.. do I? */
                if time.Now().UnixNano() >= lastSeedTS+1000000000*30 {
                        fmt.Printf("[SeedYou] Requesting Seed!\n  NOW - THEN = %s", time.Now().UnixNano()-lastSeedTS)
                        message := fmt.Sprintf(`{"SeerAddr":"%s","SeerRequest":"SeedMe"}`, tcpAddress)
                        SendGossip(message, decodedGossip.SeerAddr)
                } else {
                        fmt.Printf("[SeedYou] No need to seed!\n  NOW - THEN = %s", time.Now().UnixNano()-lastSeedTS)
                }
                return
        }
        if err != nil || decodedGossip.SeerAddr == "" || destinationIp != *hostIP {
                fmt.Printf("\nBad Gossip!: %s\nErr: %s\nDestination: %s\n", gossip, err, destinationIp)
                return
        }
        /* Write to oplog. opName limits updates from single host to 10 per nanosecond.. */
        opName := fmt.Sprintf("%d_%s_%d", time.Now().UnixNano(), decodedGossip.SeerAddr, (rand.Int()%10)+10)
        err = LazyWriteFile(SeerOpDir, opName+".op", []byte(gossip))
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
        /* Only checking SeerServiceDir for current TS. */
        /* Current code has odd side effect of allowing Seer data to exist for host even if we missed initial Seer HELO gossip. */
        /* We leave it to AntiEntropy to patch that up. */
        if decodedGossip.ServiceName == "" {
                decodedGossip.ServiceName = "Seer"
        }
        if !FreshGossip(SeerServiceDir+"/"+decodedGossip.ServiceName+"/"+decodedGossip.SeerAddr, decodedGossip.TS) {
                fmt.Printf("[Old Assed Gossip] I ain't writing that.\n")
                return
        }
        err := LazyWriteFile(SeerServiceDir+"/"+decodedGossip.ServiceName, decodedGossip.SeerAddr, []byte(gossip))
        if err != nil {
                fmt.Printf("Could not PutGossip to: [%s]! Error:\n%s", SeerServiceDir+"/"+decodedGossip.ServiceName, err)
        }
        err = LazyWriteFile(SeerHostDir+"/"+decodedGossip.SeerAddr, decodedGossip.ServiceName, []byte(gossip))
        if err != nil {
                fmt.Printf("Could not PutGossip to: [%s]! Error:\n%s", SeerHostDir+"/"+decodedGossip.SeerAddr, err)
        }
}

func FreshGossip(filePath string, newTS int64) bool {
        serviceData, err := ioutil.ReadFile(filePath)
        if err != nil {
                return true
        }
        currentTS, err := ExtractTSFromJSON(string(serviceData))
        return newTS > currentTS
}

func ExtractTSFromJSON(gossip string) (int64, error) {
        /* Looks for "TS" in JSON like:
           {"TS":<numbers>,...}
           {...,"TS":<numbers>}
           {...,"TS":<numbers>,...}
        */
        ts := tsRegexp.FindStringSubmatch(gossip)
        if len(ts) == 2 {
                return strconv.ParseInt(ts[1], 10, 64)
        }
        return 0, errors.New("Could not find TS.")
}

func LazyWriteFile(folderName string, fileName string, data []byte) error {
        err := ioutil.WriteFile(folderName+"/"+fileName, data, 0777)
        if err != nil {
                os.MkdirAll(folderName, 0777)
                err = ioutil.WriteFile(folderName+"/"+fileName, data, 0777)
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
