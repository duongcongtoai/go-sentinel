package sentinel

import (
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	file := filepath.Join("test", "config", "sentinel.yaml")
	numInstances := 3
	master := NewToyKeva()
	sentinels := []*Sentinel{}
	links := make([]*toyClient, numInstances)
	testLock := &sync.Mutex{}
	master.turnToMaster()
	basePort := 2000
	for i := 0; i < numInstances; i++ {
		s, err := NewFromConfig(file)
		s.conf.Port = strconv.Itoa(basePort + i)
		assert.NoError(t, err)

		s.clientFactory = func(addr string) (internalClient, error) {
			cl := NewToyKevaClient(master)
			testLock.Lock()
			links[i] = cl
			testLock.Unlock()
			return cl, nil
		}
		err = s.Start()
		assert.NoError(t, err)
		sentinels = append(sentinels, s)
		defer s.Shutdown()
	}
	// sleep for 2 second to ensure all sentinels have pubsub and recognized each other
	time.Sleep(2 * time.Second)
	masterAddr := "localhost:6767"

	for _, s := range sentinels {
		s.mu.Lock()
		masterI, ok := s.masterInstances[masterAddr]
		assert.True(t, ok)
		s.mu.Unlock()
		masterI.mu.Lock()
		assert.Equal(t, numInstances-1, len(masterI.sentinels))
		masterI.mu.Unlock()
	}
}

func TestSimpleSdown(t *testing.T) {
	file := filepath.Join("test", "config", "sentinel.yaml")
	numInstances := 3
	master := NewToyKeva()
	sentinels := []*Sentinel{}
	links := make([]*toyClient, numInstances)
	testLock := &sync.Mutex{}
	master.turnToMaster()
	basePort := 2000
	sampleConf := Config{}
	for i := 0; i < numInstances; i++ {
		s, err := NewFromConfig(file)
		s.conf.Port = strconv.Itoa(basePort + i)
		sampleConf = s.conf
		assert.NoError(t, err)

		s.clientFactory = func(addr string) (internalClient, error) {
			cl := NewToyKevaClient(master)
			testLock.Lock()
			links[i] = cl
			testLock.Unlock()
			return cl, nil
		}
		err = s.Start()
		assert.NoError(t, err)
		sentinels = append(sentinels, s)
	}
	// sleep for 2 second to ensure all sentinels have pubsub and recognized each other
	time.Sleep(2 * time.Second)
	masterAddr := "localhost:6767"

	for _, s := range sentinels {
		s.mu.Lock()
		masterI, ok := s.masterInstances[masterAddr]
		assert.True(t, ok)
		s.mu.Unlock()
		masterI.mu.Lock()
		assert.Equal(t, numInstances-1, len(masterI.sentinels))
		masterI.mu.Unlock()
	}

	disconnectedIdx := 0
	links[disconnectedIdx].disconnect()

	time.Sleep(sampleConf.Masters[0].DownAfter)
	// 1 more second for sure
	time.Sleep(1 * time.Second)

	// check if a given sentinel is in sdown state, and holds for a long time
	checkMasterState(t, masterAddr, sentinels[disconnectedIdx], masterStateSubjDown)
	// others still see master is up
	for idx := range sentinels {
		if idx == disconnectedIdx {
			continue
		}
		checkMasterState(t, masterAddr, sentinels[idx], masterStateUp)
	}

}

func checkMasterState(t *testing.T, masterAddr string, s *Sentinel, state masterInstanceState) {
	s.mu.Lock()
	m := s.masterInstances[masterAddr]
	s.mu.Unlock()
	assert.Equal(t, state, m.getState())
}
