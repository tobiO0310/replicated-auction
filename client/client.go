package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	proto "replicated_auction/grpc/public"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var FILE_NAME = "LEADER_PORT.txt"

var my_id uint32
var last_bid uint32
var timestamp uint64

var client proto.AuctionServiceClient
var conn *grpc.ClientConn

func conn_is_ready(conn *grpc.ClientConn) bool {
	return conn.GetState().String() != "READY"
}

func try_connect() *grpc.ClientConn {
	pwd, pwdErr := os.Getwd()
	if pwdErr != nil {
		log.Fatalln("Could not get current working directory, please try again later")
	}
	file := path.Join(pwd, FILE_NAME)
	log.Printf("Trying to read file %s\n", file)
	content, fileErr := os.ReadFile(file)

	if fileErr != nil {
		log.Fatalln("Failed to read leader file, please try again later")
	}

	conn, connErr := grpc.NewClient(
		fmt.Sprintf("localhost:%s", strings.TrimSpace(string(content))),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	for connErr != nil || !conn_is_ready(conn) {
		log.Printf("Trying to read file %s\n", file)
		content, fileErr = os.ReadFile(file)

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

func bid(bid uint32) bool {
	//assumes you've set your id before using
	// returns false if auction is over
	if bid != 0 {
		log.Printf("trying to bid %d", bid)
		timestamp += 1
		res, resErr := client.MakeBid(context.Background(), &proto.MakeBidRequest{
			Bid:       bid,
			MyId:      &my_id,
			Timestamp: timestamp,
		})

		if resErr != nil {
			if strings.Contains(resErr.Error(), "wrong server buddy") {
				log.Printf("connection error")
				for resErr != nil || res == nil {
					log.Printf("Connection failed, retrying connection...")
					conn = try_connect()
					timestamp += 1
					res, resErr = client.MakeBid(context.Background(), &proto.MakeBidRequest{
						Bid:       bid,
						MyId:      &my_id,
						Timestamp: timestamp,
					})
				}
			} else if strings.Contains(resErr.Error(), "Auction is over") {
				log.Printf("timer finished")
				return false
			} else {
				log.Printf("some other error")
				return true
			}
		}

		timestamp = max(timestamp, res.Timestamp)
		timestamp += 1
		log.Printf("I just bid %d", bid)
	}
	return true
}

func setId() {
	res, resErr := client.MakeBid(context.Background(), &proto.MakeBidRequest{
		Bid:       0,
		Timestamp: timestamp,
	})
	//todo: handle errors that aren't to do with connection
	for resErr != nil || res == nil {
		log.Printf("Connection failed, retrying connection...")
		conn = try_connect()
		timestamp += 1
		res, resErr = client.MakeBid(context.Background(), &proto.MakeBidRequest{
			Bid:       0,
			Timestamp: timestamp,
		})
	}
	my_id = res.YourId
	log.Printf("new id is: %d", my_id)
}

func result() (*proto.GetResultResponse, bool) {
	//if bool true then response type is highestbid, if false then winner
	timestamp += 1
	res, resErr := client.GetResult(context.Background(), &proto.GetResultRequest{
		Timestamp: timestamp,
	})
	for resErr != nil || res == nil {
		log.Printf("Connection failed, retrying connection...")
		conn = try_connect()
		timestamp += 1
		res, resErr = client.GetResult(context.Background(), &proto.GetResultRequest{
			Timestamp: timestamp,
		})
	}

	timestamp = max(timestamp, res.Timestamp)
	timestamp += 1

	highest, ok := res.Result.(*proto.GetResultResponse_HighestBid)

	if ok {
		log.Printf("Auction is still going, current highest bid is: %d\n", highest.HighestBid)
	} else {
		winner := res.Result.(*proto.GetResultResponse_Winner_)
		log.Printf("Auction is over! Winner has id %d and won with a bid of %d", winner.Winner.BidderId, winner.Winner.Bid)
	}

	return res, ok
}

func main() {
	conn = try_connect()

	client = proto.NewAuctionServiceClient(conn)

	setId()
	log.Print("Set my Id!")

	keepBidding := true
	var res *proto.GetResultResponse
	var new_bid uint32

	for keepBidding {
		res, keepBidding = result()
		if keepBidding {
			highest := res.Result.(*proto.GetResultResponse_HighestBid)
			curr_bid := highest.HighestBid
			if last_bid < curr_bid || curr_bid == 0 {
				new_bid = curr_bid + 1
				keepBidding = bid(new_bid)
				if keepBidding {
					last_bid = new_bid
				}
			}
		}
	}

	conn.Close()
}
