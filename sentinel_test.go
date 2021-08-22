package sentinel

import (
	"fmt"
)

func (s *Sentinel) StartTest() error {
	if len(s.conf.Masters) != 1 {
		return fmt.Errorf("only support monitoring 1 master for now")
	}
	m := s.conf.Masters[0]
	cl := NewToyKevaClient(NewToyKeva())

	//read master from config, contact that master to get its slave, then contact it slave and sta
	infoStr, err := cl.Info()
	if err != nil {
		return err
	}
	switchedRole, err := s.parseInfoMaster(m.Addr, infoStr)
	if err != nil {
		return err
	}
	if switchedRole {
		return fmt.Errorf("reported address of master %s is not currently in master role", m.Name)
	}
	mInstance := s.masterInstances[m.Name]
	// mInstance.infoClient = &internalClientImpl{cl}
	// mInstance.pingClient = &internalClientImpl{cl2}

	go s.masterRoutine(mInstance)
	go s.serveRPC()
	return nil
}
