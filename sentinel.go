package sentinel

import (
	"fmt"
	"sync"
	"time"

	"github.com/spf13/viper"
	kevago "github.com/tuhuynh27/keva/go-client"
	"go.uber.org/zap"
)

var (
	logger *zap.SugaredLogger
)

func init() {
	dlogger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	logger = dlogger.Sugar()
}

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
	go s.serveRPC()
	return nil
}

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
	masterName      string
	killed          bool
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
	client           internalClient
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
