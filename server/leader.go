package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	proto "replicated_auction/grpc/private"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

var LEADER_FILE_NAME = "LEADER_PORT.txt"

type Server struct {
	id         uint32
	port       string
	connection *grpc.ClientConn
}

type LeaderService struct {
	proto.UnimplementedLeaderServiceServer

	auction_service *AuctionService

	timestamp uint64
	mutex     sync.Mutex
	my_id     uint32
	my_port   uint32
	my_auction_port uint32
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

func try_connect_leader_service_with_retry(port string) *grpc.ClientConn {
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
	i := 0
	for _, s := range self.servers {
		arr[i] = &proto.HiIExistResponse_Servers{
			Id:   s.id,
			Port: s.port,
		}
		i++
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

func (self *LeaderService) updateServers(bidderId uint32, bid uint32, endUnixtime uint32, timestamp uint64) uint64 {
	for _, server := range self.servers {
		client := proto.NewLeaderServiceClient(server.connection)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		timestamp += 1
		res, err := client.DataUpdated(ctx, &proto.DataUpdatedRequest{
			BidderId:    bidderId,
			Bid:         bid,
			EndUnixtime: endUnixtime,
			Timestamp:   timestamp,
		})
		if err != nil || res == nil {
			// assume server died :)
			delete(self.servers, server.id)
			continue
		}
		timestamp = max(timestamp, res.Timestamp)
		timestamp += 1
	}
	return timestamp
}

func NewLeaderService(id uint32, port uint32, auction_port uint32) *LeaderService {
	service := &LeaderService{
		my_id:   id,
		my_port: port,
		my_auction_port: auction_port,
		servers: make(map[uint32]Server),
	}

	go func() {
		for {
			if service.leader_id != nil && *service.leader_id != service.my_id {
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
	log.Printf("someone exists! they're %d at %s\n", in.Id, in.Port)
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.timestamp = max(in.Timestamp)
	self.timestamp += 1 // got message

	other_servers := self.map_servers()

	conn := try_connect_leader_service_with_retry(in.Port)
	if conn == nil {
		return nil, errors.New("could not connect to you")
	}

	self.servers[in.Id] = Server{
		id:         in.Id,
		port:       in.Port,
		connection: conn,
	}

	self.timestamp += 1 // send message
	return &proto.HiIExistResponse{
		Timestamp: self.timestamp,
		Servers: other_servers,
		MyId: self.my_id,
	}, nil
}

func (self *LeaderService) CallElection(ctx context.Context, in *proto.CallElectionRequest) (*proto.CallElectionResponse, error) {
	log.Printf("received call election from %d\n", in.MyId)
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.timestamp = max(in.Timestamp)
	self.timestamp += 1 // got message

	for _, server := range self.servers_above_me() {
		log.Printf("call election to %d\n", server.id)
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
			log.Printf("they found a leader! new leader id: %d\n", res.LeaderId)
			self.timestamp += 1
			res.Timestamp = self.timestamp
			return res, nil
		}
	}
	
	log.Printf("I AM LEADER NOW. i shall tell everyone!\n")

	// no leader above me, i must be leader then

	for _, server := range self.servers {
		client := proto.NewLeaderServiceClient(server.connection)
		self.timestamp += 1
		go client.IAmLeader(context.Background(), &proto.IAmLeaderRequest{
			MyId:      self.my_id,
			Timestamp: self.timestamp,
		})
	}

	
	log.Printf("telling clients where to look now\n")
	pwd, pwdErr := os.Getwd()
	if pwdErr != nil {
		log.Fatalln("Could not get current working directory, please try again later")
	}
	file := path.Join(pwd, LEADER_FILE_NAME)
	os.WriteFile(file, []byte(fmt.Sprintf("%d", self.my_auction_port)), 0644) // 0644 (unix permission) = owner can read & write, everyone else can read

	self.timestamp += 1
	self.leader_id = &self.my_id
	return &proto.CallElectionResponse{
		LeaderId:  &self.my_id,
		Timestamp: self.timestamp,
	}, nil
}

func (self *LeaderService) IAmLeader(ctx context.Context, in *proto.IAmLeaderRequest) (*proto.IAmLeaderResponse, error) {
	log.Printf("received i am leader message from %d\n", in.MyId)
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.timestamp = max(self.timestamp, in.Timestamp)
	self.timestamp += 1

	self.leader_id = &in.MyId

	self.timestamp += 1
	return &proto.IAmLeaderResponse{Timestamp: self.timestamp}, nil
}

func (self *LeaderService) DataUpdated(ctx context.Context, in *proto.DataUpdatedRequest) (*proto.DataUpdatedResponse, error) {
	log.Printf("received data update from leader\n")
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.timestamp = max(self.timestamp, in.Timestamp)
	self.timestamp += 1

	if self.auction_service == nil {
		self.timestamp += 1
		return nil, errors.New("Missing auction service, help pls")
	}

	self.auction_service.bid(in.BidderId, in.Bid)
	self.auction_service.end_unixtime = in.EndUnixtime

	self.timestamp += 1
	return &proto.DataUpdatedResponse{
		Timestamp: self.timestamp,
	}, nil
}
