package main

import (
	dto "if3230-tubes-williamjeffersonclinton/internal/dto"
	raft "if3230-tubes-williamjeffersonclinton/internal/service"
	// "net/rpc"
	"fmt"
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

	// rpc.HandleHTTP()
	// if len(args) <= 2 {
	// 	raft.NewRaftNode(nil, dto.Address{IP: "localhost", Port: args[1]}, nil)
	// } else {
	// 	raft.NewRaftNode(nil, dto.Address{IP: "localhost", Port: args[1]}, &dto.Address{IP: "localhost", Port: args[2]})
	// }

	if len(args) < 2 {
		fmt.Println("Usage: go run main.go <port>")
		os.Exit(1)
	}

	port := args[1]
	node := raft.NewRaftNode(nil, dto.Address{IP: "localhost", Port: args[1]}, nil)

	// Append some log entries (for testing purposes)
	node.AppendEntries(
		dto.LogEntry{Index: 1, Term: 1, Data: "ping"},
		dto.LogEntry{Index: 2, Term: 1, Data: "ping"},
	)

	// Display the log entries
	for _, entry := range node.Log {
		fmt.Printf("Index: %d, Term: %d, Data: %s\n", entry.Index, entry.Term, entry.Data)
	}

	fmt.Printf("Raft node started on port %s\n", port)
	select {}
}
