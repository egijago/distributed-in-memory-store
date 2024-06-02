package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"

	"github.com/improbable-eng/grpc-web/go/grpcweb"
	entity "github.com/melvinkj/raft-consensus/internal/domain/entities"
	repository "github.com/melvinkj/raft-consensus/internal/domain/repositories"
	"github.com/melvinkj/raft-consensus/internal/domain/services"
	pb "github.com/melvinkj/raft-consensus/internal/pb/proto"

	"github.com/melvinkj/raft-consensus/internal/interfaces/grpc/handlers"
	"google.golang.org/grpc"
)

func requestLogger(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	log.Printf("Received request: %s", info.FullMethod)
	resp, err := handler(ctx, req)

	return resp, err
}

func main() {

	/* Parse argument(s) */
	grpcPort := flag.String("grpc_port", "", "gRPC port")
	masterPort := flag.String("master_port", "", "Master port")
	grpcWebPort := flag.String("grpc_web_port", "", "gRPC-web port")
	flag.Parse()

	if *grpcPort == "" {
		log.Fatal("grpc_port is required")
	}
	if *grpcWebPort == "" {
		log.Fatal("grpc_web_port is required")
	}

	/* Setup TCP listener on gRPC port*/
	lis, err := net.Listen("tcp", ":"+*grpcPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	kvRepository := repository.NewKeyValueRepository()
	logRepository := repository.NewLogRepository()
	var raftService *services.RaftService = nil
	if *masterPort == "" {
		raftService = services.NewRaftLeaderService(*entity.NewAddress("localhost", *grpcPort), kvRepository, logRepository)
	} else {
		raftService = services.NewRaftFollowerService(*entity.NewAddress("localhost", *grpcPort), kvRepository, logRepository, entity.Address{Host: "localhost", Port: *masterPort})
	}
	clientService := services.NewClientService(kvRepository, logRepository)

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(requestLogger),
	)
	grpcHandler := handlers.NewGrpcHandler(raftService, clientService)

	pb.RegisterRaftServiceServer(grpcServer, grpcHandler)
	pb.RegisterClientServiceServer(grpcServer, grpcHandler)

	log.Println("gRPC server started + on port " + *grpcPort + "...")
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve gRPC: %v", err)
		}
	}()

	grpcWebServer := grpcweb.WrapServer(
		grpcServer,
		grpcweb.WithOriginFunc(func(origin string) bool { return true }),
	)

	httpServer := &http.Server{
		Addr:    ":" + *grpcWebPort,
		Handler: grpcWebServer,
	}

	log.Println("gRPC-Web server started on port " + *grpcWebPort + "...")
	if err := httpServer.ListenAndServe(); err != nil {
		log.Fatalf("failed to serve gRPC-Web: %v", err)
	}

}
