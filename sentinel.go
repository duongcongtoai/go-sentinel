package sentinel

import (
	"fmt"
	"sync"
	"time"

	"github.com/spf13/viper"

	kevago "github.com/tuhuynh27/keva/go-client"
)

type Config struct {
	MyID         string
	Binds        []string //TODO: understand how to bind multiple network interfaces
	Port         string
	Masters      []MasterMonitor //TODO: support multiple master
	CurrentEpoch int
}

type MasterMonitor struct {
	Name            string
	Addr            string
	Quorum          int
	DownAfter       time.Duration
	FailoverTimeout time.Duration
	ConfigEpoch     int //epoch of master received from hello message
	LeaderEpoch     int //last leader epoch
	KnownReplicas   []KnownReplica
	KnownSentinels  []KnownSentinel
}
type KnownSentinel struct {
	ID   string
	Addr string
}

type KnownReplica struct {
	Addr string
}

type Sentinel struct {
	conf            Config
	masterInstances map[string]*masterInstance
	instancesLock   sync.Mutex
	currentEpoch    int
}

func NewFromConfig(filepath string) (*Sentinel, error) {
	viper.AddConfigPath(filepath)
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}
	var conf Config
	err = viper.Unmarshal(&conf)
	if err != nil {
		return nil, err
	}
	return &Sentinel{
		conf: conf,
	}, nil
}

func (m *masterInstance) killed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isKilled

}

func (s *Sentinel) masterPingRoutine(m *masterInstance) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for !m.killed() {
		select {
		case <-ticker.C:
			err := m.pingClient.Ping()
			if err != nil {
				if time.Now().Sub(m.lastSuccessfulPing) > m.sentinelConf.DownAfter {
					//notify if it is down
					if m.getState() == masterStateUp {
						m.mu.Lock()
						m.state = masterStateSubjDown
						m.mu.Unlock()
						select {
						case m.subjDownNotify <- struct{}{}:
						default:
						}
					}
				}
				continue
			} else {
				m.mu.Lock()
				state := m.state
				m.lastSuccessfulPing = time.Now()
				m.mu.Unlock()
				if state == masterStateSubjDown || state == masterStateObjDown {
					m.mu.Lock()
					m.state = masterStateUp
					m.mu.Unlock()
					//select {
					//case m.reviveNotify <- struct{}{}:
					//default:
					//}
					//revive
				}

			}
		}

	}
}

func (m *masterInstance) getState() masterInstanceState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

func (s *Sentinel) masterRoutine(m *masterInstance) {
	go s.masterPingRoutine(m)
	infoTicker := time.NewTicker(10 * time.Second)
	for !m.killed() {
		switch m.getState() {
		case masterStateUp:
			select {
			case <-m.shutdownChan:
				return
			case <-m.subjDownNotify:
				//notify allowing break select loop early, rather than waiting for 10s
				infoTicker.Stop()

			case <-infoTicker.C:
				info, err := m.infoClient.Info()
				if err != nil {
					//TODO continue for now
					continue
				}
				roleSwitched, err := s.parseInfoMaster(m.name, info)
				if err != nil {
					// continue for now
				}
				if roleSwitched {
					panic("unimlemented")
					//TODO
					// s.parseInfoSlave()
				}
			}

		case masterStateSubjDown:
		SdownLoop:
			for {
				time.Sleep(1 * time.Second)
				switch m.getState() {
				case masterStateSubjDown:
					// check if quorum as met
					s.askSentinelsIfMasterIsDown(m)
				case masterStateObjDown:
					panic("no other process should set masterStateObjDown")
				case masterStateUp:
					break SdownLoop
				}
			}
		case masterStateObjDown:
			//BIG TODO
		}
	}
}

func (s *Sentinel) askSentinelsIfMasterIsDown(m *masterInstance) {
	m.mu.Lock()
	for _, sentinel := range m.sentinels {
		go func() {
			sentinel.mu.Lock()
			lastReply := sentinel.lastMasterDownReply
			sentinel.mu.Unlock()
			if time.Now().Sub(lastReply) < 1*time.Second {
				return
			}
			if m.getState() == masterStateSubjDown {
				reply, err := sentinel.client.IsMasterDownByAddr(IsMasterDownByAddrArgs{
					Addr:         m.ip,
					Port:         m.port,
					CurrentEpoch: s.currentEpoch,
					SelfID:       "", // do not request for vote yet
				})
				if err != nil {
					//skip for now
				} else {
					sentinel.mu.Lock()
					sentinel.lastMasterDownReply = time.Now()
					sentinel.masterDown = reply.MasterDown
					sentinel.mu.Unlock()
				}

			} else {
				return
			}

		}()
	}
	m.mu.Unlock()

}

func (s *Sentinel) Start() error {
	if len(s.conf.Masters) != 1 {
		return fmt.Errorf("only support monitoring 1 master for now")
	}
	m := s.conf.Masters[0]
	cl, err := kevago.NewInternalClient(m.Addr)
	if err != nil {
		return err
	}
	cl2, err := kevago.NewInternalClient(m.Addr)
	if err != nil {
		return err
	}

	//read master from config, contact that master to get its slave, then contact it slave and sta
	infoStr, err := cl.Info()
	if err != nil {
		return err
	}
	switchedRole, err := s.parseInfoMaster(m.Name, infoStr)
	if err != nil {
		return err
	}
	if switchedRole {
		return fmt.Errorf("reported address of master %s is not currently in master role", m.Name)
	}
	mInstance := s.masterInstances[m.Name]
	mInstance.infoClient = cl
	mInstance.pingClient = cl2

	go s.masterRoutine(mInstance)
	return nil
}

type masterInstance struct {
	sentinelConf MasterMonitor
	isKilled     bool
	name         string
	mu           sync.Mutex
	runID        string
	slaves       map[string]*slaveInstance
	sentinels    []*sentinelInstance
	ip           string
	port         string
	shutdownChan chan struct{}
	pingClient   internalClient
	// infoClient   *kevago.InternalClient
	infoClient internalClient

	state          masterInstanceState
	subjDownNotify chan struct{}
	reviveNotify   chan struct{}

	lastSuccessfulPing time.Time
}

type internalClient interface {
	Info() (string, error)
	Ping() error
}
type sentinelInstance struct {
	mu                  sync.Mutex
	masterDown          bool
	client              sentinelClient
	lastMasterDownReply time.Time

	// these 2 must alwasy change together
	leaderEpoch int
	leaderID    string

	sdown bool
}

type masterInstanceState int

var (
	masterStateUp       masterInstanceState = 1
	masterStateSubjDown masterInstanceState = 2
	masterStateObjDown  masterInstanceState = 3
)

type slaveInstance struct {
	mu              sync.Mutex
	masterDownSince time.Duration
	masterHost      string
	masterPort      string
	masterUp        bool
	addr            string
	slavePriority   int //TODO
	replOffset      int
	lastRefresh     time.Time
	reportedRole    instanceRole
	reportedMaster  *masterInstance

	//each slave has a goroutine that ping by interval
	pingShutdownChan chan struct{}
	//each slave has a goroutine that check info every 10s
	infoShutdownChan chan struct{}
	//notify goroutines that master is down, to change info interval from 10 to 1s like Redis
	masterDownNotify chan struct{}
}

type instanceRole int

var (
	instanceRoleMaster instanceRole = 1
	instanceRoleSlave  instanceRole = 2
)

type sentinelClient interface {
	IsMasterDownByAddr(IsMasterDownByAddrArgs) (IsMasterDownByAddrReply, error)
}

type IsMasterDownByAddrArgs struct {
	Addr         string
	Port         string
	CurrentEpoch int
	SelfID       string
}
type IsMasterDownByAddrReply struct {
	MasterDown    bool
	VotedLeaderID string
	LeaderEpoch   int
}
