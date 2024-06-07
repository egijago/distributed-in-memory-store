package main

import (
	"if3230-tubes-williamjeffersonclinton/internal/domain/entities"
	repository "if3230-tubes-williamjeffersonclinton/internal/domain/repositories"
	"if3230-tubes-williamjeffersonclinton/internal/domain/services"
	"if3230-tubes-williamjeffersonclinton/internal/interfaces/grpc/handlers"
	pb "if3230-tubes-williamjeffersonclinton/internal/pb/proto"

	"github.com/improbable-eng/grpc-web/go/grpcweb"

	"context"
	"flag"
	"log"
	"net"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

func requestLogger(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	p, ok := peer.FromContext(ctx)
	if ok {
		addr := p.Addr
		log.Printf("Received request: %s from %s with local addr %s", info.FullMethod, addr.String(), p.LocalAddr.String())
	} else {
		log.Printf("Received request: %s", info.FullMethod)
	}

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

	state := entities.NewState(
		*entities.NewAddress("localhost", *grpcPort),
		3000,
		3000,
	)

	if *masterPort == "" {
		self := entities.NewRaftClient(*entities.NewAddress("localhost", *grpcPort))
		state.Members = append(state.Members, *self)
		raftService = services.NewRaftLeaderService(*state, kvRepository, logRepository)
	} else {
		raftService = services.NewRaftFollowerService(*state, kvRepository, logRepository, entities.Address{Host: "localhost", Port: *masterPort})
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
