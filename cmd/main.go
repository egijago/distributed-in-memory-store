package main

import (
	dto "if3230-tubes-williamjeffersonclinton/internal/dto"
	raft "if3230-tubes-williamjeffersonclinton/internal/service"
	"net/rpc"
	"os"
)

func main() {
	// lis, err := net.Listen("tcp", ":50051")
	// if err != nil {
	// 	log.Fatalf("failed to listen: %v", err)
	// }
	// s := grpc.NewServer()
	// server.New(s)
	// log.Printf("server listening at %v", lis.Addr())
	// if err := s.Serve(lis); err != nil {
	// 	log.Fatalf("failed to serve: %v", err)
	// }

	args := os.Args

	rpc.HandleHTTP()
	if len(args) <= 2 {
		raft.NewRaftNode(nil, dto.Address{IP: "localhost", Port: args[1]}, nil)
	} else {
		raft.NewRaftNode(nil, dto.Address{IP: "localhost", Port: args[1]}, &dto.Address{IP: "localhost", Port: args[2]})
	}

	// Sleep to keep the main function running
	select {}
}
