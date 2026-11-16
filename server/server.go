package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"replicated_auction/grpc/private"

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
var port = flag.Uint64("port", 0, "the port of this server")

func conn_is_ready(conn *grpc.ClientConn) bool {
	return conn.GetState().String() != "READY"
}
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

func main() {

	lis := listen(port)
	defer lis.Close()

	service := NewLeaderService(uint32(*my_id))

	

	grpcServer := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))
	private.RegisterLeaderServiceServer(grpcServer, service)
	grpcServer.Serve(lis)
}
