package main

import (
	public "replicated_auction/grpc/public"
)

type AuctionService struct {
	public.UnimplementedAuctionServiceServer
}
