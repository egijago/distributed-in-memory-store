package main

import (
	"encoding/json"
	"time"

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
		log.Printf("Received request: %s from %s", info.FullMethod, addr.String())
	} else {
		log.Printf("Received request: %s", info.FullMethod)
	}

	resp, err := handler(ctx, req)
	return resp, err
}

func stateLogger(state entities.StateInterface) {
	jsonState, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		log.Fatalf("failed to marshal state: %v", err)
	}
	log.Println("[STATE]", string(jsonState))
}

func main() {
	/* Parse argument(s) */
	host := flag.String("host", "localhost", "Hotname")
	grpcPort := flag.String("grpc_port", "", "gRPC port")
	grpcWebPort := flag.String("grpc_web_port", "", "gRPC-web port")
	masterHost := flag.String("master_host", "localhost", "Master host")
	masterPort := flag.String("master_port", "", "Master port")

	electionTimeout := flag.Int("et", 0, "Election timeout")
	heartbeatInterval := flag.Int("hi", 0, "Heartbeat interval")
	heartbeatRequestTimeout := flag.Int("hrto", 0, "Heartbeat request timeout")
	heartbeatTimeout := flag.Int("ht", 0, "Heartbeat timeout")
	heartbeatTimeoutSlack := flag.Int("hts", 0, "Heartbeat timeout slack")
	quorumScale := flag.Float64("qs", 0.5, "Quorum scale (0.0 - 1.0)")
	twoPCTimeout := flag.Int("tpct", 1000, "Two phase commit timeout")

	verbose := flag.Bool("verbose", false, "Verbose mode")

	flag.Parse()

	if *heartbeatRequestTimeout == 0 {
		*heartbeatRequestTimeout = *heartbeatInterval / 2
	}

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
		*entities.NewAddress(*host, *grpcPort),
		*electionTimeout,
		*heartbeatInterval,
		*heartbeatRequestTimeout,
		*heartbeatTimeout,
		*heartbeatTimeoutSlack,
		*twoPCTimeout,
	)

	if *verbose {
		log.Println("Verbose mode enabled")
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		done := make(chan bool)

		go func() {
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					stateLogger(state)
				}
			}
		}()
	}

	if *masterPort == "" {
		raftService = services.NewRaftLeaderService(state, kvRepository, logRepository, *quorumScale)
	} else {
		raftService = services.NewRaftFollowerService(state, kvRepository, logRepository, entities.Address{Host: *masterHost, Port: *masterPort}, *quorumScale)
	}
	clientService := services.NewClientService(state, kvRepository, logRepository, raftService)

	a, b := raftService.Get()

	log.Println(a)
	log.Println(b)

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
