package sentinel

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// func (s *Sentinel) StartTest() error {

// }

func TestInit(t *testing.T) {
	file := filepath.Join("test", "config", "sentinel.yaml")
	s, err := NewFromConfig(file)
	assert.NoError(t, err)

	keva := NewToyKeva()
	keva.turnToMaster()
	s.clientFactory = func(addr string) (internalClient, error) {
		return NewToyKevaClient(keva), nil
	}

	err = s.Start()
	assert.NoError(t, err)
	//given an example config, start 3 sentinel with it
}
