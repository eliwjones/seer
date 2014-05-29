package seer

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
)

var (
	udpSendSocket  *net.UDPConn
	recentMessages = map[string]int64{}

	// Global vars for easier testing.  Overwritten by seer.New(params...)
	hostIP      string
	secret      string
	tcpPort     int
	tcpAddress  string
	udpPort     int
	udpAddress  string
	database    string
	datadir     string
	metadatadir string
	seeded      bool
)

type seer struct {
	Seeds []string
}

type metaMessage struct {
	Timestamp         int64
	Tombstone         bool `json:",omitempty"`
	Source            string
	Message           map[string]interface{} `json:",omitempty"`
	MessageKey        string                 `json:",omitempty"`
	IndexedProperties []string               /* 'host' ('nodeId'?), 'service' */
	Control           string                 `json:",omitempty"`
}

// New version of it.
func New(hostip string, udpport int, tcpport int, s string, seeds []string) *seer {
	hostIP = hostip
	tcpPort = tcpport
	udpPort = udpport
	udpAddress = fmt.Sprintf("%s:%d", hostIP, udpPort)
	tcpAddress = fmt.Sprintf("%s:%d", hostIP, tcpPort)
	secret = s
	database = "seer_data" + "/" + udpAddress
	datadir = database + "/data"
	metadatadir = database + "/metadata"

	return &seer{Seeds: seeds}
}

// Run it.
func (s *seer) Run() {
	signaler := make(chan os.Signal, 1)
	signal.Notify(signaler, os.Interrupt, os.Kill)

	createUDPSendSocket()

	messager := make(chan string, 100)
	go udpServer(udpAddress, messager)
	go HttpServer(fmt.Sprintf(":%d", tcpPort))

	msg := <-messager
	if msg != "udpready" {
		fmt.Println("First message from messager should have been 'udpready'. Received: ", msg)
		os.Exit(1)
	}

	port := fmt.Sprintf("%d", udpPort)
	mm := joinMessage(udpAddress, hostIP, port)

	// Join self.
	persistMessage(mm)

	mm.Control = "seedee"
	m := encode(mm, udpAddress)
	for _, peer := range s.Seeds {
		if len(peer) < 3 {
			continue
		}
		sendMessage(m, peer)
	}

	BackgroundLoop("Tombstone Reaper", 24*60*60, tombstoneReaper)

	for {
		select {
		case m := <-messager:
			fmt.Println("[message]: ", m)
		case mySignal := <-signaler:
			fmt.Println("[signal]: ", mySignal)
			// Leaving group.
			lm := leaveMessage(udpAddress, hostIP, port)
			gossip(lm)

			os.Exit(1)
		}
	}
}

func processMessage(m string) {
	fmt.Printf("[message]: %s\n", m)
	/* Send */

	var mm metaMessage
	json.Unmarshal([]byte(m), &mm)
	if !auth(mm) {
		fmt.Printf("Auth Failed: %s\n", m)
		return
	}

	if mm.Control != "" {
		mm = processControlMessage(mm)
	}
	if mm.Message == nil {
		fmt.Printf("I do not process empty mw.Messages: %s\n", m)
		return
	}

	// Guess should stuff in syncronization for this... or use channel.
	recencyKey := fmt.Sprintf("%s_%s_%s", mm.Message["Name"], mm.Message["Type"], mm.Message["Port"])
	fmt.Printf("recencyKey: %s, mw: %v\n", recencyKey, mm)
	if recentMessages[recencyKey] >= mm.Timestamp {
		fmt.Printf("OLD MESSAGE!!mm.Timestamp < recentMessages Timestamp:\n%d\n%d\n", mm.Timestamp, recentMessages[recencyKey])
		return
	}
	fmt.Printf("NEW MESSAGE!!mw.Timestamp > recentMessages Timestamp:\n%d\n%d\n", mm.Timestamp, recentMessages[recencyKey])
	recentMessages[recencyKey] = mm.Timestamp

	persistMessage(mm)

	gossip(mm)
}

func processControlMessage(mm metaMessage) metaMessage {
	switch mm.Control {
	case "seedee":
		randWait := rand.Intn(1000)
		time.Sleep(time.Duration(randWait) * time.Millisecond)

		seedu := metaMessage{}
		seedu.Timestamp = MS(Now())
		seedu.Control = "seedu"

		sendMessage(encode(seedu, udpAddress), mm.Source)

		// Do not want 'seedu's from all peers.
		// But would like to gossip 'join' to rest of peers.
		mm.Control = ""
	case "seedu":
		if seeded {
			break
		}
		seedme := metaMessage{}
		seedme.Timestamp = MS(Now())
		seedme.Control = "seedme"

		sendMessage(encode(seedme, udpAddress), mm.Source)
	case "seedme":
		// Proper way is to directly send the file contents.
		peers := getPeers()
		for _, peer := range peers {
			var seed metaMessage
			json.Unmarshal(get("host", peer), &seed)
			if seed.Message == nil {
				fmt.Println("seed.Message is nil! skipping.")
				continue
			}
			seed.Control = "seed"
			sendMessage(encode(seed, udpAddress), mm.Source)
		}
	case "seed":
		// If I receive one 'seed' message, simply presume I will be seeded with rest.
		// Anti-entropy can handle the rest.
		seeded = true
	}

	return mm
}

func persistMessage(mm metaMessage) {
	// Most likely will want extra-paranoid timestamp check in here.
	m, _ := json.Marshal(mm)
	for _, key := range mm.IndexedProperties {
		index := mm.Message[key].(string)
		if index == "" {
			// Don't want to write empty string.
			continue
		}
		key = strings.ToLower(key)
		lazyWriteFile(datadir+"/"+key, index, m)
	}
}

func auth(mm metaMessage) bool {
	key := messageKey(mm)
	return key == mm.MessageKey
}

func messageKey(mm metaMessage) string {
	mm.MessageKey = ""
	m, _ := json.Marshal(mm)
	h := hmac.New(sha256.New, []byte(secret))
	h.Write(m)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func encode(mm metaMessage, source string) []byte {
	mm.Source = source
	mm.MessageKey = messageKey(mm)
	marshaledMessage, _ := json.Marshal(mm)
	return marshaledMessage
}

func joinMessage(source string, ip string, port string) metaMessage {
	mm := metaMessage{}
	mm.Message = map[string]interface{}{}
	mm.Timestamp = MS(Now())
	mm.Source = source
	mm.Message["Name"] = ip
	mm.Message["Port"] = port
	mm.Message["Host"] = fmt.Sprintf("%s:%s", mm.Message["Name"], mm.Message["Port"])
	mm.IndexedProperties = []string{"Host"}
	return mm
}

func leaveMessage(source string, ip string, port string) metaMessage {
	mm := joinMessage(source, ip, port)
	mm.Tombstone = true
	return mm
}

func gossip(mm metaMessage) {
	m := encode(mm, udpAddress)
	// Choose N hosts to send to.
	peers := getPeers()

	gossipHosts := chooseNFromArrayNonDeterministically(5, peers)
	for _, peer := range gossipHosts {
		sendMessage(m, peer)
	}
}

func get(path string, key string) []byte {
	value, _ := ioutil.ReadFile(datadir + "/" + path + "/" + key)
	if strings.Contains(string(value), `Tombstone":true`) {
		return nil
	}
	return value
}

var getPeers = func() []string {
	d, _ := os.Open(datadir + "/host")
	hosts, _ := d.Readdirnames(-1)
	return hosts
}

func getKeyPaths() []string {
	keypaths := []string{}
	indexedProps, _ := ioutil.ReadDir(datadir)
	for _, indexedProp := range indexedProps {
		if !indexedProp.IsDir() {
			// Looking for directories holding indexed items.
			continue
		}
		path := datadir + "/" + indexedProp.Name()
		keys, _ := ioutil.ReadDir(path)
		for _, key := range keys {
			if key.IsDir() {
				// Looking for files here.
				continue
			}
			keypaths = append(keypaths, path+"/"+key.Name())
		}
	}
	return keypaths
}

func extractHostIPandPort(udpaddress string) (string, string, error) {
	parts := strings.Split(udpaddress, ":")
	if len(parts) != 2 {
		return "", "", errors.New(fmt.Sprintf("Address should be of format <hostip>:<port>. Got: %s", udpaddress))
	}
	hostip := parts[0]
	port := parts[1]
	if hostip == "" || port == "" {
		return hostip, port, errors.New(fmt.Sprintf("Hostip and Port should be non-empty. Hostip: %s, Port: %s", hostip, port))
	}
	return hostip, port, nil
}

/*******************************************************************************
    Background Workers
*******************************************************************************/

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

func tombstoneReaper() {
	keypaths := getKeyPaths()
	for _, keypath := range keypaths {
		value, err := ioutil.ReadFile(keypath)
		if err != nil {
			fmt.Printf("keypath: %s, err: %s\n", keypath, err)
			continue
		}
		var mm metaMessage
		json.Unmarshal(value, &mm)
		// If Tombstoned over 24 hours, remove it.
		if mm.Tombstone && (MS(Now())-mm.Timestamp) > 24*60*60*1000 {
			os.RemoveAll(keypath)
		}
	}
}

/*******************************************************************************
    UDP Send
*******************************************************************************/

func createUDPSendSocket() {
	c, err := net.ListenPacket("udp", ":0")
	if err != nil {
		fmt.Printf("[createUDPSendSocket] ERR: %s\n", err)
		os.Exit(1)
	}
	udpSendSocket = c.(*net.UDPConn)
}

var sendMessage = func(message []byte, destination string) {
	time.Sleep(25 * time.Millisecond)
	udpAddr, err := net.ResolveUDPAddr("udp", destination)
	fmt.Printf("peer: %v, udp: %v, err: %s\n", destination, udpAddr, err)
	udpSendSocket.WriteToUDP(message, udpAddr)
}

/*******************************************************************************
    UDP Server
*******************************************************************************/

func udpServer(address string, messager chan string) {
	var err error
	var c net.PacketConn

	c, err = net.ListenPacket("udp", address)
	if err != nil {
		fmt.Printf("ListenPacket failed! %v, udpAddress: %s\n", err, address)
		log.Fatal(err)
	}
	defer c.Close()
	messager <- "udpready"
	/* Assuming that MTU can't be below 576, which implies one has 508 bytes for payload after overhead. */
	udpBuff := make([]byte, 508)
	for {
		n, _, err := c.ReadFrom(udpBuff)
		if err != nil {
			log.Fatal(err)
		}
		message := strings.Trim(string(udpBuff[:n]), "\n")
		if message == "exit" {
			return
		}
		go processMessage(message)
	}
}

/*******************************************************************************
    HTTP Server
********************************************************************************/

func HttpServer(address string) {
	http.HandleFunc("/", HttpHandler)
	http.ListenAndServe(address, nil)
}

func HttpHandler(w http.ResponseWriter, r *http.Request) {
	// Only allow HTTP on localhost. When wish to allow external GETs, do more work.
	if !(strings.HasPrefix(r.Host, "127.0.0.1") ||
		strings.HasPrefix(r.Host, "[::1]")) {
		return
	}
	response := ""
	splitURL := strings.Split(r.URL.Path, "/")
	switch r.Method {
	case "GET":
		if len(splitURL) == 2 && splitURL[1] == "" {
			fmt.Fprintf(w, fmt.Sprintf("You should request something..\nHOST: %s, PATH: %s, METHOD: %s\n", r.Host, r.URL.Path, r.Method))
			return
		}
		path := splitURL[1]
		if len(splitURL) == 3 {
			// Get single item.
			key := splitURL[2]
			response = string(get(path, key))
		} else {
			dirlist, _ := os.Open(datadir + "/" + path)
			keys, _ := dirlist.Readdirnames(-1)
			items := make([]string, 0, len(keys))
			for _, key := range keys {
				item := string(get(path, key))
				if item != "" {
					items = append(items, item)
				}
			}
			response = fmt.Sprintf(`[%s]`, strings.Join(items, ","))
		}
		fmt.Fprintf(w, response)
	case "PUT":
		b := bytes.NewBuffer(nil)
		_, err := io.Copy(b, r.Body)
		if err != nil {
			fmt.Fprintf(w, fmt.Sprintf("ERR: %s\n", err))
		}
		mm := metaMessage{}
		json.Unmarshal(b.Bytes(), &mm.Message)
		json.Unmarshal([]byte(r.Header.Get("X-IndexedProperties")), &mm.IndexedProperties)
		if mm.IndexedProperties == nil {
			fmt.Fprintf(w, "Please pass json encoded list to 'X-IndexedProperties' header!\n")
			return
		}
		for _, prop := range mm.IndexedProperties {
			if mm.Message[prop] == nil {
				fmt.Fprintf(w, fmt.Sprintf("There is no property named '%s' for Message: %s\n", prop, string(b.Bytes())))
				return
			}
			if mm.Message[prop] == "" {
				fmt.Fprintf(w, fmt.Sprintf("Property named '%s' is empty for Message: %s\n", prop, string(b.Bytes())))
				return
			}
		}
		mm.Timestamp = MS(Now())
		encodedMessage := encode(mm, udpAddress)
		// For now, electing to use udp server as main entry point.
		sendMessage(encodedMessage, udpAddress)
		fmt.Fprintf(w, fmt.Sprintf("PUT: %s.\n", encodedMessage))
	}
}

/*******************************************************************************
    Utilities
*******************************************************************************/

func lazyWriteFile(folderName string, fileName string, data []byte) error {
	err := ioutil.WriteFile(folderName+"/"+fileName, data, 0777)
	if err != nil {
		os.MkdirAll(folderName, 0777)
		err = ioutil.WriteFile(folderName+"/"+fileName, data, 0777)
	}
	if err != nil {
		fmt.Printf("[LazyWriteFile] Could not WriteFile: %s\nErr: %s\n", folderName+"/"+fileName, err)
	}
	return err
}

func chooseNFromArrayNonDeterministically(n int, array []string) []string {
	chosenItems := make([]string, 0, n)
	tracker := map[string]bool{}
	counter := 0
	tries := 0
	m := len(array)
	for counter < n {
		tries++
		if tries > maxInt(m, 4*n) {
			/* Do not wish to try forever. */
			break
		}
		item := array[rand.Intn(m)]
		if tracker[item] {
			continue
		}
		chosenItems = append(chosenItems, item)
		tracker[item] = true
		counter++
	}
	return chosenItems
}

func maxInt(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

// MS should be in external package.
func MS(time time.Time) int64 {
	return time.UnixNano() / 1000000
}

// Now() is voodoo to make mocking time easier in tests.
var Now = func() time.Time { return time.Now() }
