package seer

import (
	"encoding/json"
	"os"
	"testing"
)

var (
	hostmock = map[string][]byte{}
)

func getMetaMessage(generatekey bool) metaMessage {
	mm := metaMessage{}
	mm.Message = map[string]any{}
	mm.Timestamp = 1234567890
	mm.Source = udpAddress
	mm.Message["Type"] = "join"
	mm.Message["Name"] = "node9999"
	mm.Message["Port"] = "8888"
	if generatekey {
		mm.MessageKey = messageKey(mm)
	}
	return mm
}

func initializeMocks() {
	hostmock = map[string][]byte{}
	sendMessage = func(message []byte, destination string) {
		hostmock[destination] = message
	}
	getPeers = func() []string {
		return []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "0"}
	}
}

func Test_auth(t *testing.T) {
	mm := getMetaMessage(false)

	if auth(mm) {
		t.Errorf("How can a metaMessage auth with no key?")
	}
	mm.MessageKey = messageKey(mm)

	if !auth(mm) {
		t.Errorf("My key works now so why cannot I auth?")
	}
}

func Test_messageKey(t *testing.T) {
	// Init new store since messageKey() needs secret.
	New("127.0.0.1", 9999, 9998, "dontkeepsecrets", []string{"127.0.0.1:8888"})

	mm := getMetaMessage(false)
	key := messageKey(mm)
	// Sort of a circular test.
	if key != `nHfROSxQKJbg1hn3KaHzJu4OQ9FRTwmVLHvX0pVougE=` {
		t.Errorf("%s\n", key)
	}
}

func Test_joinMessage(t *testing.T) {
	datadir = "test_seer_data"
	defer os.RemoveAll(datadir)

	peers := getPeers()
	if len(peers) != 0 {
		t.Errorf("I should have no peers, yet there are %d", len(peers))
	}
	mm := joinMessage("127.0.0.1:1111", "127.0.0.1", "2222")
	persistMessage(mm)
	peers = getPeers()
	if len(peers) != 1 {
		t.Errorf("I should have 1 peer, yet there are %d", len(peers))
	}
	mm = joinMessage("127.0.0.1:2222", "127.0.0.1", "3333")
	persistMessage(mm)
	peers = getPeers()
	if len(peers) != 2 {
		t.Errorf("I should have 2 peers, yet there are %d", len(peers))
	}
}

func Test_gossip(t *testing.T) {
	initializeMocks()
	if len(hostmock) != 0 {
		t.Errorf("What have you done?")
	}
	mm := getMetaMessage(true)
	gossip(mm)

	if len(hostmock) != 5 {
		t.Errorf("%d hosts should have been sent to!", len(hostmock))
	}
	mmm, _ := json.Marshal(mm)
	for peer, message := range hostmock {
		if string(message) != string(mmm) {
			t.Errorf("Peer: %s seems to have wrong message:\n%v\n%v.", peer, message, mmm)
		}
	}
}

func Test_tombstoneReaper(t *testing.T) {
	datadir = "test_seer_data"
	defer os.RemoveAll(datadir)

	mm := joinMessage("1.1.1.1:1", "1.1.1.1", "1")
	persistMessage(mm)
	mm = leaveMessage("2.2.2.2:2", "2.2.2.2", "2")
	mm.Timestamp = MS(Now()) - 25*60*60*1000
	persistMessage(mm)

	keypaths := getKeyPaths()
	if len(keypaths) != 2 {
		t.Errorf("Should be 2 keypaths!")
	}

	tombstoneReaper()
	keypaths = getKeyPaths()
	if len(keypaths) != 1 {
		t.Errorf("Should be 1 keypath left!")
	}

	mm = leaveMessage("2.2.2.2:2", "2.2.2.2", "2")
	persistMessage(mm)

	tombstoneReaper()
	keypaths = getKeyPaths()
	if len(keypaths) != 2 {
		t.Errorf("Tombstoned item should not have been reaped!")
	}
}
