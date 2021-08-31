package sentinel

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

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
		_, err := m.client.Ping()
		if err != nil {
			if time.Since(m.lastSuccessfulPing) > m.sentinelConf.DownAfter {
				//notify if it is down
				if m.getState() == masterStateUp {
					m.mu.Lock()
					m.state = masterStateSubjDown
					logger.Warnf("master %s is subjectively down", m.name)
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

func (m *masterInstance) getFailOverStartTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failOverStartTime
}

func (m *masterInstance) getFailOverEpoch() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failOverEpoch
}

func (m *masterInstance) getFailOverState() failOverState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failOverState
}
func (m *masterInstance) getAddr() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return fmt.Sprintf("%s:%s", m.ip, m.port)
}

func (m *masterInstance) getState() masterInstanceState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// for now, only say hello through master, redis also say hello through slaves
func (s *Sentinel) sayHelloRoutine(m *masterInstance, helloChan HelloChan) {
	for !m.killed() {
		time.Sleep(1 * time.Second)
		m.mu.Lock()
		masterName := m.name
		masterIP := m.ip
		masterPort := m.port
		masterConfigEpoch := m.configEpoch
		m.mu.Unlock()
		info := fmt.Sprintf("%s,%s,%s,%d,%s,%s,%s,%d",
			s.conf.Binds[0], s.conf.Port, s.selfID(),
			s.getCurrentEpoch(), masterName, masterIP, masterPort, masterConfigEpoch,
		)
		err := helloChan.Publish(info)
		if err != nil {
			logger.Errorf("helloChan.Publish: %s", err)
		}
	}
}

func (s *Sentinel) subscribeHello(m *masterInstance) {
	helloChan := m.client.SubscribeHelloChan()

	go s.sayHelloRoutine(m, helloChan)
	for !m.killed() {
		newmsg, err := helloChan.Receive()
		if err != nil {
			logger.Errorf("helloChan.Receive: %s", err)
			continue
			//skip for now
		}
		parts := strings.Split(newmsg, ",")
		if len(parts) != 8 {
			logger.Errorf("helloChan.Receive: invalid format for newmsg: %s", newmsg)
			continue
		}
		runid := parts[2]
		m.mu.Lock()
		_, ok := m.sentinels[runid]
		if !ok {
			client, err := newRPCClient(parts[0], parts[1])
			if err != nil {
				logger.Errorf("newRPCClient: cannot create new client to other sentinel with info: %s", newmsg)
				m.mu.Unlock()
				continue
			}
			si := &sentinelInstance{
				mu:     sync.Mutex{},
				sdown:  false,
				client: client,
				runID:  runid,
			}

			m.sentinels[runid] = si
			logger.Infof("subscribeHello: connected to new sentinel: %s", newmsg)
		}
		m.mu.Unlock()
		//ignore if exist for now
	}
}

func (s *Sentinel) masterRoutine(m *masterInstance) {
	go s.masterPingRoutine(m)
	go s.subscribeHello(m)
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
				info, err := m.client.Info()
				if err != nil {
					//TODO continue for now
					continue
				}
				roleSwitched, err := s.parseInfoMaster(m.getAddr(), info)
				if err != nil {
					logger.Errorf("parseInfoMaster: %v", err)
					continue
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
				time.Sleep(1 * time.Second)
			}
		case masterStateObjDown:
			ctx, cancel := context.WithCancel(context.Background())
			go s.askOtherSentinelsEach1Sec(ctx, m)
			for m.getState() == masterStateObjDown {
				// this code only has to wait in case failover timeout reached and it needs to wait 1 more failover timeout duration
				// before trying failover again
				secondsLeft := 2*m.sentinelConf.FailoverTimeout - time.Since(m.getFailOverStartTime())
				if secondsLeft <= 0 {
					locked(s.mu, func() {
						s.currentEpoch += 1
						locked(&m.mu, func() {
							m.failOverState = failOverWaitLeaderElection
							m.failOverStartTime = time.Now().Add(time.Duration(rand.Intn(SENTINEL_MAX_DESYNC)))
							m.failOverEpoch = s.currentEpoch
						})
					})
					break
				}
				logger.Debugf("sleeping for %s", secondsLeft.String())
				<-time.NewTimer(secondsLeft).C
			}
			// If any logic changing state of master to something != masterStateObjDown, this code below will be broken
		failOverFSM:
			for {
				switch m.getFailOverState() {
				case failOverWaitLeaderElection:
					//check if is leader yet
					leader := s.checkWhoIsLeader(m)
					isLeader := leader != "" && leader == s.selfID()
					if !isLeader {
						time.Sleep(1 * time.Second)
						//abort if failover take too long
						if time.Since(m.getFailOverStartTime()) > m.sentinelConf.FailoverTimeout {
							m.mu.Lock()
							m.failOverState = failOverNone
							m.mu.Unlock()
							cancel()
							break failOverFSM
						}
					}
					// do not call cancel(), keep receiving update from other sentinel
					m.mu.Lock()
					m.failOverState = failOverSelectSlave
					m.mu.Unlock()

				case failOverSelectSlave:
					//TODO
				case failOverPromoteSlave:
					//TODO
				case failOverReconfSlave:
					//TODO
				}
			}
		}
	}
}

func (s *Sentinel) checkWhoIsLeader(m *masterInstance) string {
	leaders := map[string]int{}
	currentEpoch := s.getCurrentEpoch()
	m.mu.Lock()
	for _, sen := range m.sentinels {
		sen.mu.Lock()
		if sen.leaderID != "" && sen.leaderEpoch == currentEpoch {
			leaders[sen.runID] = leaders[sen.runID] + 1
		}
		sen.mu.Unlock()
	}
	m.mu.Unlock()
	maxVote := 0
	var winner string
	for runID, vote := range leaders {
		if vote > maxVote {
			maxVote = vote
			winner = runID
		}
	}
	epoch := m.getFailOverEpoch()
	var leaderEpoch int
	var votedByMe string
	if winner != "" {
		// vote for the most winned candidate
		leaderEpoch, votedByMe = s.voteLeader(m, epoch, winner)
	} else {
		leaderEpoch, votedByMe = s.voteLeader(m, epoch, s.selfID())
	}
	if votedByMe != "" && leaderEpoch == epoch {
		leaders[votedByMe] = leaders[votedByMe] + 1
		if leaders[votedByMe] > maxVote {
			maxVote = leaders[votedByMe]
			winner = votedByMe
		}
	}
	quorum := len(m.sentinels)/2 + 1
	if winner != "" && (maxVote < quorum || maxVote < m.sentinelConf.Quorum) {
		winner = ""
	}
	return winner
}

func (s *Sentinel) checkObjDown(m *masterInstance) {
	m.mu.Lock()
	defer m.mu.Unlock()
	quorum := m.sentinelConf.Quorum
	if len(m.sentinels)+1 < quorum {
		panic(fmt.Sprintf("quorum too large (%d) or too few sentinel instances (%d)", quorum, len(m.sentinels)))
		// TODO: log warning only
	}

	down := 1
	for _, sentinel := range m.sentinels {
		sentinel.mu.Lock()
		if sentinel.sdown {
			down++
		}
		sentinel.mu.Unlock()
	}
	if down >= quorum {
		m.state = masterStateObjDown
	}
}

func (s *Sentinel) askOtherSentinelsEach1Sec(ctx context.Context, m *masterInstance) {
	m.mu.Lock()
	masterName := m.name
	masterIp := m.ip
	masterPort := m.port

	for idx := range m.sentinels {
		sentinel := m.sentinels[idx]
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for m.getState() >= masterStateSubjDown {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					sentinel.mu.Lock()
					lastReply := sentinel.lastMasterDownReply
					sentinel.mu.Unlock()
					if time.Since(lastReply) < 1*time.Second {
						return
					}
					s.mu.Lock()
					currentEpoch := s.currentEpoch //epoch may change during failover
					selfID := s.runID              // locked as well for sure
					s.mu.Unlock()

					// do not ask for vote if has not started failover
					if m.getFailOverState() == failOverNone {
						selfID = ""
					}
					reply, err := sentinel.client.IsMasterDownByAddr(IsMasterDownByAddrArgs{
						Name:         masterName,
						IP:           masterIp,
						Port:         masterPort,
						CurrentEpoch: currentEpoch,
						SelfID:       selfID,
					})
					if err != nil {
						logger.Errorf("sentinel.client.IsMasterDownByAddr: %s", err)
						//skip for now
					} else {
						sentinel.mu.Lock()
						sentinel.lastMasterDownReply = time.Now()
						sentinel.sdown = reply.MasterDown
						if reply.VotedLeaderID != "" {
							if sentinel.leaderID != reply.VotedLeaderID {
								logger.Infof("sentinel.client.IsMasterDownByAddr: sentinel %s voted for %s",
									sentinel.runID,
									reply.VotedLeaderID,
								)
							}
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
	masterName := m.name
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
					Name:         masterName,
					IP:           masterIp,
					Port:         masterPort,
					CurrentEpoch: currentEpoch,
					SelfID:       "",
				})
				if err != nil {
					//skip for now
				} else {
					sInstance.mu.Lock()
					sInstance.lastMasterDownReply = time.Now()
					sInstance.sdown = reply.MasterDown
					sInstance.mu.Unlock()
				}
			} else {
				return
			}

		}(sentinel)
	}
	m.mu.Unlock()

}

type masterInstance struct {
	sentinelConf MasterMonitor
	isKilled     bool
	name         string
	mu           sync.Mutex
	runID        string
	slaves       map[string]*slaveInstance
	sentinels    map[string]*sentinelInstance
	ip           string
	port         string
	shutdownChan chan struct{}
	client       internalClient
	// infoClient   internalClient

	state          masterInstanceState
	subjDownNotify chan struct{}

	lastSuccessfulPing time.Time

	failOverState     failOverState
	failOverEpoch     int
	failOverStartTime time.Time

	leaderEpoch int
	leaderID    string
	// failOverStartTime time.Time

	configEpoch int
}

type failOverState int

var (
	failOverNone               failOverState = 0
	failOverWaitLeaderElection failOverState = 1
	failOverSelectSlave        failOverState = 2
	failOverPromoteSlave       failOverState = 3
	failOverReconfSlave        failOverState = 4
)
