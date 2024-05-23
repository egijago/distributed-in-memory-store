package main

import (
	raft "if3230-tubes-williamjeffersonclinton/internal/service"
	dto "if3230-tubes-williamjeffersonclinton/internal/dto"
	"net/rpc"
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

	// var wg sync.WaitGroup

    // wg.Add(1)
    // go func() {
    //     defer wg.Done()
    //     err := raft.NewRaftNode(nil, dto.Address{"localhost", "8000"}, nil)
    //     if err != nil {
    //         log.Fatalf("Failed to start Raft node on port 8000: %+v", err)
    //     }
    // }()	

    // // Start the second Raft node, specifying the address of the first node
    // wg.Add(1)
    // go func() {
    //     defer wg.Done()
    //     err := raft.NewRaftNode(nil, dto.Address{"localhost", "8001"}, &dto.Address{"localhost", "8000"})
    //     if err != nil {
    //         log.Fatalf("Failed to start Raft node on port 8001: %+v", err)
    //     }
    // }()

	rpc.HandleHTTP()
	raft.NewRaftNode(nil, dto.Address{"localhost", "8000"}, nil)
	raft.NewRaftNode(nil, dto.Address{"localhost", "8001"}, &dto.Address{"localhost", "8000"})
	// raft.NewRaftNode(nil, raft.Address{"localhost", "8002"}, &raft.Address{"localhost", "8000"})

	// node1.InitializeAsLeader()

	// Sleep to keep the main function running
    select {}
}
