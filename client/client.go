package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path"
	proto "replicated_auction/grpc/public"
	"strconv"
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
	state := conn.GetState().String()
	return state == "READY" || state == "IDLE"
}

func is_auction_over(error error) bool {
	return error != nil && strings.Contains(error.Error(), "Auction is over");
}

func auction_is_over_test(error error) {
	if is_auction_over(error) {
		log.Printf("Auction is over, server is refusing connections\n")
		os.Exit(0)
	}
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

func bid(bid uint32) {
	//assumes you've set your id before using
	// returns false if auction is over
	if bid != 0 {
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
				log.Printf("[CLIENT] Auction is over")
			} else if strings.Contains(resErr.Error(), "400") {
				log.Printf("[CLIENT] Bid of %d too low", bid)
			} else {
				log.Printf("[CLIENT] Something went wrong")
			}
		}

		timestamp = max(timestamp, res.Timestamp)
		timestamp += 1
		log.Printf("[CLIENT] Bid of %d successful", bid)
	}
}

func setId() {
	res, resErr := client.MakeBid(context.Background(), &proto.MakeBidRequest{
		Bid:       0,
		Timestamp: timestamp,
	})
	//todo: handle errors that aren't to do with connection
	for resErr != nil || res == nil {
		auction_is_over_test(resErr)
		log.Printf("Connection failed, retrying connection...")
		conn = try_connect()
		timestamp += 1
		res, resErr = client.MakeBid(context.Background(), &proto.MakeBidRequest{
			Bid:       0,
			Timestamp: timestamp,
		})
	}
	my_id = res.YourId
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

	reader := bufio.NewReader(os.Stdin)
	log.Print("[CLIENT] Ready to bid? Y/N")
	ans, _ := reader.ReadString('\n')
	ans = strings.TrimSpace(ans)
	if ans == "Y" || ans == "y" {
		setId()
		log.Printf("[CLIENT] Your id is '%d'", my_id)
	} else {
		log.Printf("[CLIENT] Bye then")
		conn.Close()
		os.Exit(0)
	}

	log.Printf("[CLIENT] Four possible commands:")
	log.Printf(" - bid")
	log.Printf(" - result")
	log.Printf(" - exit")
	log.Printf(" - help")

	for true {
		ans, _ = reader.ReadString('\n')
		ans = strings.TrimSpace(ans)

		if ans == "bid" {
			log.Printf("[CLIENT] How much would you like to bid? Bid 0 to exit")
			numAns, _ := reader.ReadString('\n')
			numAns = strings.TrimSpace(numAns)
			num, err := strconv.Atoi(numAns)
			for err != nil {
				log.Printf("[CLIENT] Invalid Response, please provide a number")
				numAns, _ = reader.ReadString('\n')
				numAns = strings.TrimSpace(numAns)
				num, err = strconv.Atoi(numAns)
			}
			if num != 0 {
				bid(uint32(num))
			} else {
				continue
			}
		} else if ans == "result" {
			result()
		} else if ans == "help" {
			log.Printf("[CLIENT] Four possible commands:")
			log.Printf(" - bid")
			log.Printf(" - result")
			log.Printf(" - exit")
			log.Printf(" - help")
		} else if ans == "exit" {
			log.Printf("[CLIENT] Exiting program")
			conn.Close()
			os.Exit(0)
		} else {
			log.Printf("[CLIENT] Please provide a valid command. (Type \"help\" for commands)")
		}
	}

}
