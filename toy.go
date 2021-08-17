package sentinel

import (
	"bytes"
	"fmt"
	"sync"
)

// ToyKeva simulator used for testing purpose
type ToyKeva struct {
	mu        *sync.Mutex
	alive     bool
	role      string
	id        string
	slaves    []toySlave
	sentinels map[string]toySentinel
}

func (keva *ToyKeva) info() string {
	keva.mu.Lock()
	defer keva.mu.Unlock()
	var ret = bytes.Buffer{}
	ret.WriteString(fmt.Sprintf("role:%s\n", keva.role))
	ret.WriteString(fmt.Sprintf("run_id:%s\n", keva.id))
	for idx, sl := range keva.slaves {
		ret.WriteString(fmt.Sprintf("slave%d:ip=%s,port=%s,state=%s,offset=%d,lag=%d\n",
			idx, sl.addr, sl.state, sl.port, sl.offset, sl.lag,
		))
	}
	return ret.String()
}

type toySentinel struct {
	addr        string
	port        string
	runID       string
	epoch       int
	masterEpoch int
	masterName  string
	masterAddr  string
	masterPort  string
}

type toySlave struct {
	addr   string
	port   string
	state  string
	offset int
	lag    int
}

func (keva *ToyKeva) isAlive() bool {
	keva.mu.Lock()
	defer keva.mu.Unlock()
	return keva.alive
}

func (keva *ToyKeva) addSentinel(intro Intro) {
	keva.mu.Lock()
	defer keva.mu.Unlock()

	s := toySentinel{
		addr:        intro.Addr,
		port:        intro.Port,
		epoch:       intro.Epoch,
		masterEpoch: intro.MasterEpoch,
		masterName:  intro.MasterName,
		masterPort:  intro.MasterPort,
		masterAddr:  intro.MasterAddr,
	}
	keva.sentinels[intro.RunID] = s
}

type toyClient struct {
	link      *ToyKeva
	connected bool
	mu        *sync.Mutex
	//Info() (string, error)
	//Ping() (string, error)
	//SentinelIntroduce(Intro) error
}

func NewToyKeva() *ToyKeva {
	return &ToyKeva{}
}

func NewToyKevaClient(keva *ToyKeva) *toyClient {
	return &toyClient{
		link: keva,
	}
}

func (cl *toyClient) Info() (string, error) {
	return cl.link.info(), nil
}

// simulate network partition
func (cl *toyClient) isConnected() bool {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.connected
}

func (cl *toyClient) Ping() (string, error) {
	if !cl.link.isAlive() || !cl.isConnected() {
		return "", fmt.Errorf("dead keva")
	}
	return "pong", nil
}

func (cl *toyClient) ExchangeSentinel(intro Intro) (ExchangeSentinelResponse, error) {
	cl.link.mu.Lock()
	var ret []SentinelIntroResponse
	for _, s := range cl.link.sentinels {
		ret = append(ret, SentinelIntroResponse{
			Addr:        s.addr,
			Port:        s.port,
			RunID:       s.runID,
			MasterName:  s.masterName,
			MasterPort:  s.masterPort,
			MasterAddr:  s.masterAddr,
			Epoch:       s.epoch,
			MasterEpoch: s.masterEpoch,
		})
	}
	cl.link.mu.Unlock()

	cl.link.addSentinel(intro)
	return ExchangeSentinelResponse{Sentinels: ret}, nil
}
