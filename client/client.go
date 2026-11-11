package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	proto "replicated_auction/grpc/public"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var FILE_NAME = "LEADER_PORT.txt"

func conn_is_ready(conn *grpc.ClientConn) bool {
	return conn.GetState().String() != "READY"
}

func try_connect() *grpc.ClientConn {
	pwd, pwdErr := os.Getwd();
	if pwdErr != nil {
		log.Fatalln("Could not get current working directory, please try again later")
	}
	file := path.Join(pwd, FILE_NAME)
	log.Printf("Trying to read file %s\n", file)
	content, fileErr := os.ReadFile(file);

	if fileErr != nil {
		log.Fatalln("Failed to read leader file, please try again later")
	}

	conn, connErr := grpc.NewClient(
		fmt.Sprintf("localhost:%s", strings.TrimSpace(string(content))),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	for connErr != nil || !conn_is_ready(conn) {
		log.Printf("Trying to read file %s\n", file)
		content, fileErr = os.ReadFile(file);

		if fileErr != nil {
			log.Fatalln("Failed to read leader file, please try again later")
		}

		conn, connErr = grpc.NewClient(
			fmt.Sprintf("localhost:%s", strings.TrimSpace(string(content))),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}

	log.Printf("Connected to %s\n", conn.CanonicalTarget())

	return conn
}

func main() {
	conn := try_connect();

	client := proto.NewAuctionServiceClient(conn)

	
	res, resErr := client.MakeBid(context.Background(), &proto.MakeBidRequest{
		Bid: 0,
	});
	for resErr != nil || res == nil {
		log.Printf("Connection failed, retrying connection...")
		conn = try_connect();
		res, resErr = client.MakeBid(context.Background(), &proto.MakeBidRequest{
			Bid: 0,
		});
	}
	id := res.YourId;
	// yay

	// todo: implement random bidding behavior :=)
	
	conn.Close()
}
