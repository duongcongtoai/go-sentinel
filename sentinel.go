package sentinel

import (
	"context"
	"fmt"
	"math/rand"
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
	mu              *sync.Mutex
	quorum          int
	conf            Config
	masterInstances map[string]*masterInstance
	instancesLock   sync.Mutex
	currentEpoch    int
	runID           string
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
		<-ticker.C
		_, err := m.pingClient.Ping()
		if err != nil {
			if time.Since(m.lastSuccessfulPing) > m.sentinelConf.DownAfter {
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

func (m *masterInstance) getFailOverState() failOverState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failOverState
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
					s.checkObjDown(m)
					if m.getState() == masterStateObjDown {
						break SdownLoop
					}
				case masterStateObjDown:
					panic("no other process should set masterStateObjDown")
				case masterStateUp:
					break SdownLoop
				}
			}
		case masterStateObjDown:
			//timeout := time.Second*10 + time.Duration(rand.Intn(1000))*time.Millisecond // randomize timeout a bit
			failOverStartAt := time.Now().Add(time.Duration(rand.Intn(1000)))
			// to avoid 2 leaders share the same vote during leader election
			//timer := time.NewTimer(timeout) //failOver timeout
			ctx, cancel := context.WithCancel(context.Background())
			// set failOver state back to none, so it go and promote it selve in regress
			s.mu.Lock()
			s.currentEpoch += 1
			sentinelEpoch := s.currentEpoch
			s.mu.Unlock()
			m.mu.Lock()
			m.failOverState = failOverWaitLeaderElection
			m.failOverEpoch = sentinelEpoch
			m.mu.Unlock()
			go s.askOtherSentinelsEach1Sec(ctx, m)
		failOverFSM:
			for {
				switch m.getFailOverState() {
				case failOverWaitLeaderElection:
					//check if is leader yet
					if isLeader := s.checkIsLeader(m); isLeader {
						continue
					}
					time.Sleep(1 * time.Second)
					//abort if failover take too long
					if time.Since(failOverStartAt) > 10*time.Second {
						//abort
						m.mu.Lock()
						m.failOverState = failOverNone
						m.mu.Unlock()
						cancel()
						break failOverFSM
					}
				case failOverSelectSlave:
					//TODO
				case failOverPromoteSlave:
					//TODO
				case failOverReconfSlave:
					//TODO
				}
			}

			//BIG TODO
		}
	}
}

func (s *Sentinel) checkIsLeader(m *masterInstance) bool {
	return false
	//dict *counters;
	//dictIterator *di;
	//dictEntry *de;
	//unsigned int voters = 0, voters_quorum;
	//char *myvote;
	//char *winner = NULL;
	//uint64_t leader_epoch;
	//uint64_t max_votes = 0;
	//
	//serverAssert(master->flags & (SRI_O_DOWN|SRI_FAILOVER_IN_PROGRESS));
	//counters = dictCreate(&leaderVotesDictType,NULL);
	//
	//voters = dictSize(master->sentinels)+1; /* All the other sentinels and me.*/
	//
	///* Count other sentinels votes */
	//di = dictGetIterator(master->sentinels);
	//while((de = dictNext(di)) != NULL) {
	//	sentinelRedisInstance *ri = dictGetVal(de);
	//	if (ri->leader != NULL && ri->leader_epoch == sentinel.current_epoch)
	//		sentinelLeaderIncr(counters,ri->leader);
	//}
	//dictReleaseIterator(di);
	//
	///* Check what's the winner. For the winner to win, it needs two conditions:
	// * 1) Absolute majority between voters (50% + 1).
	// * 2) And anyway at least master->quorum votes. */
	//di = dictGetIterator(counters);
	//while((de = dictNext(di)) != NULL) {
	//	uint64_t votes = dictGetUnsignedIntegerVal(de);
	//
	//	if (votes > max_votes) {
	//		max_votes = votes;
	//		winner = dictGetKey(de);
	//	}
	//}
	//dictReleaseIterator(di);
	//
	///* Count this Sentinel vote:
	// * if this Sentinel did not voted yet, either vote for the most
	// * common voted sentinel, or for itself if no vote exists at all. */
	//if (winner)
	//	myvote = sentinelVoteLeader(master,epoch,winner,&leader_epoch);
	//else
	//myvote = sentinelVoteLeader(master,epoch,sentinel.myid,&leader_epoch);
	//
	//if (myvote && leader_epoch == epoch) {
	//	uint64_t votes = sentinelLeaderIncr(counters,myvote);
	//
	//	if (votes > max_votes) {
	//		max_votes = votes;
	//		winner = myvote;
	//	}
	//}
	//
	//voters_quorum = voters/2+1;
	//if (winner && (max_votes < voters_quorum || max_votes < master->quorum))
	//	winner = NULL;
	//
	//winner = winner ? sdsnew(winner) : NULL;
	//sdsfree(myvote);
	//dictRelease(counters);
	//return winner;
}

func (s *Sentinel) checkObjDown(m *masterInstance) {
	m.mu.Lock()
	defer m.mu.Unlock()
	quorum := s.quorum
	if len(m.sentinels) <= quorum {
		panic(fmt.Sprintf("quorum too large (%d) or too few sentinel instances (%d)", quorum, len(m.sentinels)))
		// TODO: log warning only
	}

	down := 1
	for _, sentinel := range m.sentinels {
		if sentinel.sdown {
			down++
		}
	}
	if down >= quorum {
		m.state = masterStateObjDown
	}
}

func (s *Sentinel) askOtherSentinelsEach1Sec(ctx context.Context, m *masterInstance) {
	m.mu.Lock()
	masterIp := m.ip
	masterPort := m.port

	for idx := range m.sentinels {
		sentinel := m.sentinels[idx]
		go func() {
			for m.getState() == masterStateSubjDown {
				select {
				case <-ctx.Done():
					return
				default:
					s.mu.Lock()
					currentEpoch := s.currentEpoch //epoch may change during failover
					selfID := s.runID              // locked as well for sure
					s.mu.Unlock()

					sentinel.mu.Lock()
					lastReply := sentinel.lastMasterDownReply
					sentinel.mu.Unlock()
					if time.Since(lastReply) < 1*time.Second {
						return
					}
					reply, err := sentinel.client.IsMasterDownByAddr(IsMasterDownByAddrArgs{
						Addr:         masterIp,
						Port:         masterPort,
						CurrentEpoch: currentEpoch,
						SelfID:       selfID,
					})
					if err != nil {
						//skip for now
					} else {
						sentinel.mu.Lock()
						sentinel.lastMasterDownReply = time.Now()
						sentinel.masterDown = reply.MasterDown
						if reply.VotedLeaderID != "" {
							sentinel.leaderEpoch = reply.LeaderEpoch
							sentinel.leaderID = reply.VotedLeaderID
						}

						sentinel.mu.Unlock()
					}
				}
			}

		}()
	}
	m.mu.Unlock()

}

func (s *Sentinel) askSentinelsIfMasterIsDown(m *masterInstance) {

	s.mu.Lock()
	currentEpoch := s.currentEpoch
	s.mu.Unlock()

	m.mu.Lock()
	masterIp := m.ip
	masterPort := m.port

	for _, sentinel := range m.sentinels {
		go func(sInstance *sentinelInstance) {
			sInstance.mu.Lock()
			lastReply := sInstance.lastMasterDownReply
			sInstance.mu.Unlock()
			if time.Since(lastReply) < 1*time.Second {
				return
			}
			if m.getState() == masterStateSubjDown {
				reply, err := sInstance.client.IsMasterDownByAddr(IsMasterDownByAddrArgs{
					Addr:         masterIp,
					Port:         masterPort,
					CurrentEpoch: currentEpoch,
					SelfID:       "",
				})
				if err != nil {
					//skip for now
				} else {
					sInstance.mu.Lock()
					sInstance.lastMasterDownReply = time.Now()
					sInstance.masterDown = reply.MasterDown
					if reply.VotedLeaderID != "" {
						sInstance.leaderEpoch = reply.LeaderEpoch
						sInstance.leaderID = reply.VotedLeaderID
					}

					sInstance.mu.Unlock()
				}
			} else {
				return
			}

		}(sentinel)
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
	// cl2, err := kevago.NewInternalClient(m.Addr)
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
	// mInstance.infoClient = &internalClientImpl{cl}
	// mInstance.pingClient = &internalClientImpl{cl2}

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
	infoClient   internalClient

	state          masterInstanceState
	subjDownNotify chan struct{}
	reviveNotify   chan struct{}

	lastSuccessfulPing time.Time

	failOverState     failOverState
	failOverEpoch     int
	failOverStartTime time.Time
}

type failOverState int

var (
	failOverNone               failOverState = 0
	failOverWaitLeaderElection failOverState = 1
	failOverSelectSlave        failOverState = 2
	failOverPromoteSlave       failOverState = 3
	failOverReconfSlave        failOverState = 4
)

type internalClient interface {
	Info() (string, error)
	Ping() (string, error)
	SubscribeHelloChan() HelloChan
}

type HelloChan interface {
	Close() error
	Publish(string) error
	Receive() (string, error)
}

type internalClientImpl struct {
	*kevago.InternalClient
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
