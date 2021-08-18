package sentinel

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

// use RPC for simplicity first
type rpcClient struct {
	*rpc.Client
}

// TODO
func newRPCClient(addr string, port string) (*rpcClient, error) {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%s", addr, port))
	if err != nil {
		return nil, err
	}
	return &rpcClient{
		Client: client,
	}, nil
}

func (c *rpcClient) IsMasterDownByAddr(req IsMasterDownByAddrArgs) (IsMasterDownByAddrReply, error) {
	var reply IsMasterDownByAddrReply
	err := c.Client.Call("Sentinel.IsMasterDownByAddr", &req, &reply)
	return reply, err
}

func (s *Sentinel) IsMasterDownByAddr(req *IsMasterDownByAddrArgs, reply *IsMasterDownByAddrReply) error {
	return nil
}

func (s *Sentinel) serveRPC() {
	rpc.Register(s)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%s", s.conf.Port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}
