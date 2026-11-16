package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	proto "replicated_auction/grpc/private"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type Server struct {
	id         uint32
	port       string
	connection *grpc.ClientConn
}

type LeaderService struct {
	proto.UnimplementedLeaderServiceServer

	timestamp uint64
	mutex     sync.Mutex
	my_id     uint32
	leader_id *uint32
	servers   map[uint32]Server
}

var kacp = keepalive.ClientParameters{
	Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
	Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
	PermitWithoutStream: true,             // send pings even without active streams
}

var kaep = keepalive.EnforcementPolicy{
	MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
	PermitWithoutStream: true,            // Allow pings even when there are no active streams
}

var kasp = keepalive.ServerParameters{
	MaxConnectionIdle:     15 * time.Second, // If a client is idle for 15 seconds, send a GOAWAY
	MaxConnectionAge:      30 * time.Second, // If any connection is alive for more than 30 seconds, send a GOAWAY
	MaxConnectionAgeGrace: 5 * time.Second,  // Allow 5 seconds for pending RPCs to complete before forcibly closing connections
	Time:                  5 * time.Second,  // Ping the client if it is idle for 5 seconds to ensure the connection is still active
	Timeout:               1 * time.Second,  // Wait 1 second for the ping ack before assuming the connection is dead
}

func try_connect_leader(port string) *grpc.ClientConn {
	conn, connErr := grpc.NewClient(
		fmt.Sprintf("localhost:%s", strings.TrimSpace(port)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kacp),
	)
	if connErr != nil {
		return nil
	}

	log.Printf("Connected to %s\n", conn.CanonicalTarget())

	return conn
}

func (self *LeaderService) map_servers() []*proto.HiIExistResponse_Servers {
	arr := make([]*proto.HiIExistResponse_Servers, len(self.servers))
	for _, s := range self.servers {
		arr = append(arr, &proto.HiIExistResponse_Servers{
			Id:   s.id,
			Port: s.port,
		})
	}

	return arr
}

func (self *LeaderService) servers_above_me() []*Server {
	arr := make([]*Server, len(self.servers)/2)
	for _, s := range self.servers {
		client := proto.NewLeaderServiceClient(s.connection)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		self.timestamp += 1
		res, err := client.Echo(ctx, &proto.EchoRequest{})
		if err != nil || res == nil { // check that it successfully got echo
			delete(self.servers, s.id)
			continue
		}

		if s.id > self.my_id {
			arr = append(arr, &s)
		}
	}

	return arr
}

func NewLeaderService(id uint32) *LeaderService {
	service := &LeaderService{
		my_id: id,
	}

	go func() {
		for {
			if service.leader_id != nil {
				client := proto.NewLeaderServiceClient(service.servers[*service.leader_id].connection)
				res, err := client.Echo(context.Background(), &proto.EchoRequest{})
				if err != nil || res == nil {
					service.CallElection(
						context.Background(),
						&proto.CallElectionRequest{MyId: service.my_id, Timestamp: service.timestamp},
					)
				}
			}
			time.Sleep(10 * time.Second)
		}
	}()

	return service
}

func (self *LeaderService) Echo(ctx context.Context, in *proto.EchoRequest) (*proto.EchoResponse, error) {
	return &proto.EchoResponse{}, nil
}

func (self *LeaderService) HiIExist(ctx context.Context, in *proto.HiIExistRequest) (*proto.HiIExistResponse, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.timestamp = max(in.Timestamp)
	self.timestamp += 1 // got message

	other_servers := self.map_servers()

	conn := try_connect_leader(in.Port)
	if conn == nil {
		return nil, errors.New("could not connect to you")
	}

	self.servers[in.Id] = Server{
		id:         in.Id,
		port:       in.Port,
		connection: conn,
	}

	self.timestamp += 1 // send message
	return &proto.HiIExistResponse{Timestamp: self.timestamp, Servers: other_servers}, nil
}

func (self *LeaderService) CallElection(ctx context.Context, in *proto.CallElectionRequest) (*proto.CallElectionResponse, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.timestamp = max(in.Timestamp)
	self.timestamp += 1 // got message

	for _, server := range self.servers_above_me() {
		client := proto.NewLeaderServiceClient(server.connection)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		self.timestamp += 1
		res, err := client.CallElection(ctx, &proto.CallElectionRequest{
			MyId:      self.my_id,
			Timestamp: self.timestamp,
		})
		if err != nil || res == nil { // failed, therefore we ignore :)
			delete(self.servers, server.id)
			continue
		}
		self.timestamp = max(self.timestamp, res.Timestamp)
		self.timestamp += 1

		if res.LeaderId != nil {
			self.timestamp += 1
			res.Timestamp = self.timestamp
			return res, nil
		}
	}

	// no leader above me, i must be leader then

	for _, server := range self.servers {
		client := proto.NewLeaderServiceClient(server.connection)
		self.timestamp += 1
		go client.IAmLeader(context.Background(), &proto.IAmLeaderRequest{
			MyId:      self.my_id,
			Timestamp: self.timestamp,
		})
	}

	self.timestamp += 1
	return &proto.CallElectionResponse{
		LeaderId:  &self.my_id,
		Timestamp: self.timestamp,
	}, nil
}

func (self *LeaderService) IAmLeader(ctx context.Context, in *proto.IAmLeaderRequest) (*proto.IAmLeaderResponse, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.timestamp = max(self.timestamp, in.Timestamp)
	self.timestamp += 1

	self.leader_id = &in.MyId

	self.timestamp += 1
	return &proto.IAmLeaderResponse{Timestamp: self.timestamp}, nil
}
