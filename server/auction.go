package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	proto "replicated_auction/grpc/public"
	"sync"
	"time"
)

var AUCTION_TIME uint32 = 10 // seconds

type Bidder struct {
	id       uint32
	last_bid uint32
}

type AuctionService struct {
	proto.UnimplementedAuctionServiceServer

	leader_service *LeaderService

	mutex             sync.Mutex
	known_bidders     []Bidder
	current_winner_id uint32
	timestamp         uint64 // lamport
	end_unixtime      uint32 // physical :)
}

func (self *AuctionService) IsLeader() bool {
	return self.leader_service != nil &&
		self.leader_service.leader_id != nil &&
		self.leader_service.my_id == *self.leader_service.leader_id
}

func (self *AuctionService) bid(bidder_id uint32, bid uint32) bool {
	for len(self.known_bidders) <= int(bidder_id) {
		// make sure the id has been created on this server
		new_id := uint32(len(self.known_bidders))
		self.known_bidders = append(self.known_bidders, Bidder{
			id:       new_id,
			last_bid: 0,
		})
	}
	winner_bid := self.known_bidders[self.current_winner_id].last_bid

	if bid <= winner_bid {
		return false
	}

	self.current_winner_id = bidder_id
	self.known_bidders[bidder_id].last_bid = bid
	return true
}

func (self *AuctionService) MakeBid(ctx context.Context, in *proto.MakeBidRequest) (*proto.MakeBidResponse, error) {
	if in.MyId == nil && in.Bid == 0 {
		log.Printf("A new challenger approaches!")
	} else if in.MyId != nil && in.Bid != 0 {
		log.Printf("Someone's bidding??? %d bids %d!\n", *in.MyId, in.Bid)
	} else {
		log.Printf("%d is trying to bid 0 but already has an account. sigh", *in.MyId)
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.timestamp = max(self.timestamp, in.Timestamp)
	self.timestamp += 1

	if !self.IsLeader() {
		// leader service is nil or i am not leader
		port := "N/A"
		if self.leader_service != nil && self.leader_service.leader_id != nil {
			port = self.leader_service.servers[*self.leader_service.leader_id].port
		}
		self.timestamp += 1
		return nil, fmt.Errorf("wrong server buddy, connect to localhost:%s", port)
	}

	if self.end_unixtime == 0 {
		// start timer :)
		self.end_unixtime = uint32(time.Now().Unix()) + AUCTION_TIME
		log.Printf("Starting timer")
	} else if self.end_unixtime <= uint32(time.Now().Unix()) {
		// over now >:P
		self.timestamp += 1
		log.Printf("Timer is done.")

		return nil, errors.New("Auction is over, buddy, you're too slow")
	}

	if in.MyId != nil {
		// user gave id, validate id exists
		if int(*in.MyId) > len(self.known_bidders) {
			// not created user, whatchu doing???
			self.timestamp += 1
			return nil, errors.New("404 New Era")
		}
		if !self.bid(*in.MyId, in.Bid) {
			// invalid bid
			self.timestamp += 1
			return nil, errors.New("400 make better bid buddy")
		}
		self.leader_service.updateServers(*in.MyId, in.Bid, self.end_unixtime, self.timestamp)

		self.timestamp += 1
		return &proto.MakeBidResponse{
			YourId:    *in.MyId,
			Timestamp: self.timestamp,
		}, nil
	} else if in.Bid == 0 {
		// user no give id, but wants id :)
		new_id := uint32(len(self.known_bidders))
		self.known_bidders = append(self.known_bidders, Bidder{
			id:       new_id,
			last_bid: 0,
		})
		self.timestamp += 1
		self.leader_service.updateServers(new_id, 0, self.end_unixtime, self.timestamp)

		self.timestamp += 1
		return &proto.MakeBidResponse{
			YourId:    new_id,
			Timestamp: self.timestamp,
		}, nil
	} else {
		// what
		self.timestamp += 1
		return nil, errors.New("418 I'm a teapot. I cannot make bids (coffee) without an id (beans). Please input 0 (beans restock) to get id.") // get an id first buddy, i can't help otherwise
	}
}

func (self *AuctionService) GetResult(ctx context.Context, in *proto.GetResultRequest) (*proto.GetResultResponse, error) {
	log.Printf("someone wanna snitch on the winner... let's see if there are any")
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.timestamp = max(self.timestamp, in.Timestamp)
	self.timestamp += 1

	if !self.IsLeader() {
		// leader service is nil or i am not leader
		port := "N/A"
		if self.leader_service != nil && self.leader_service.leader_id != nil {
			port = self.leader_service.servers[*self.leader_service.leader_id].port
		}
		self.timestamp += 1
		return nil, fmt.Errorf("wrong server buddy, connect to localhost:%s", port)
	}

	if self.end_unixtime == 0 {
		// start timer :)
		self.end_unixtime = uint32(time.Now().Unix()) + AUCTION_TIME
	} else if self.end_unixtime <= uint32(time.Now().Unix()) {
		// over now >:P
		self.timestamp += 1
		return &proto.GetResultResponse{
			Result: &proto.GetResultResponse_Winner_{
				Winner: &proto.GetResultResponse_Winner{
					BidderId: self.current_winner_id,
					Bid:      self.known_bidders[self.current_winner_id].last_bid,
				},
			},
		}, nil
	}

	if len(self.known_bidders) == 0 {
		// no-one has bid anything yet, bid a single money unit to win!
		self.timestamp += 1
		return &proto.GetResultResponse{
			Result: &proto.GetResultResponse_HighestBid{
				HighestBid: 0,
			},
		}, nil
	}

	// not over yet
	self.timestamp += 1
	return &proto.GetResultResponse{
		Result: &proto.GetResultResponse_HighestBid{
			HighestBid: self.known_bidders[self.current_winner_id].last_bid,
		},
	}, nil
}
