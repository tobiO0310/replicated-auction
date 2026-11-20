package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"replicated_auction/grpc/private"
	"replicated_auction/grpc/public"
	"time"

	"google.golang.org/grpc"
)

func leader_file() string {
	var FILE_NAME = "LEADER_PORT.txt"

	pwd, pwdErr := os.Getwd()
	if pwdErr != nil {
		log.Fatalln("Could not get current working directory, please try again later")
	}
	return path.Join(pwd, FILE_NAME)
}

var my_id = flag.Uint64("id", 0, "the id of this server")
var auction_port = flag.Uint64("auction_port", 0, "the port the auction clients use")
var leader_port = flag.Uint64("port", 0, "the port of this server")
var try_connect_port = flag.Uint64("try_connect", 0, "a server to start connection to, 0 = none")

func listen(port *uint64) net.Listener {
	if port == nil {
		log.Fatalf("Nil port given")
	}
	lis, netErr := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if netErr != nil {
		log.Fatalf("[SERVER] failed to listen: %v\n", netErr)
	} else {
		log.Printf("[SERVER]\tserver is listening on rpc://localhost:%d\n\t\t\t\tPress Ctrl+C to exit.\n", *port)
	}
	return lis
}

func connect_conn(conn *grpc.ClientConn, port string, leader_service *LeaderService) []*private.HiIExistResponse_Servers {
	client := private.NewLeaderServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	leader_service.timestamp += 1
	res, err := client.HiIExist(ctx, &private.HiIExistRequest{
		Id:        uint32(*my_id),
		Port:      fmt.Sprintf("%d", *leader_port),
		Timestamp: leader_service.timestamp,
	})
	if err != nil || res == nil {
		log.Printf("failed to connect2: %s", err)
		return nil
	}
	leader_service.timestamp = max(leader_service.timestamp, res.Timestamp)
	leader_service.timestamp += 1

	leader_service.servers[res.MyId] = Server{
		id:         res.MyId,
		port:       port,
		connection: conn,
	}

	return res.Servers
}

func main() {
	flag.Parse()

	leader_service := NewLeaderService(uint32(*my_id), uint32(*leader_port), uint32(*auction_port))

	auction_service := &AuctionService{
		leader_service: leader_service,
		known_bidders:  make([]Bidder, 0),
	}
	leader_service.auction_service = auction_service

	go func() {
		// leader service
		lis := listen(leader_port)
		defer lis.Close()

		grpcServer := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))
		private.RegisterLeaderServiceServer(grpcServer, leader_service)
		grpcServer.Serve(lis)
	}()

	go func() {
		if try_connect_port == nil || *try_connect_port == 0 {
			// I'm the only guy, LEADER_PORT needs me
			leader_service.CallElection(context.Background(), &private.CallElectionRequest{
				MyId:      uint32(*my_id),
				Timestamp: leader_service.timestamp,
			})
			return
		}
		actual_try_connect_port := fmt.Sprintf("%d", *try_connect_port)

		time.Sleep(150 * time.Millisecond) // let own server startup
		conn := try_connect_leader_service(actual_try_connect_port)

		leader_service.mutex.Lock()
		if res := connect_conn(conn, actual_try_connect_port, leader_service); res != nil {
			for _, server := range res {
				serverConn := try_connect_leader_service(server.Port)

				if serverConn == nil {
					continue // failure the other server hasn't caught yet?
				}

				connect_conn(serverConn, server.Port, leader_service)
			}
		}
		leader_service.mutex.Unlock()

		leader_service.CallElection(context.Background(), &private.CallElectionRequest{
			MyId:      uint32(*my_id),
			Timestamp: leader_service.timestamp,
		})
	}()

	// auction service

	lis := listen(auction_port)
	defer lis.Close()
	grpcServer := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))
	public.RegisterAuctionServiceServer(grpcServer, auction_service)
	grpcServer.Serve(lis)
}
