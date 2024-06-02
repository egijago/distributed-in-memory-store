.PHONY: help proto node1 node2 node3

.DEFAULT_GOAL := help

help:
	@echo Usage:
	@echo "make node GRPC_PORT=<grpc_port> GRPC_WEB_PORT=<grpc_web_port> [MASTER_PORT=<master_port>]"
	@echo e.g.
	@echo To start a leader: "make node GRPC_PORT=8000 GRPC_WEB_PORT=8001"
	@echo To start a follower: "make node GRPC_PORT=8002 GRPC_WEB_PORT=8003 MASTER_PORT=8000"

proto: 
	protoc --go_out=internal/pb --go-grpc_out=internal/pb proto/*.proto --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative 

# proto-client: // JANGAN DI RUN
# 	protoc -I=. proto/client.proto --js_out=import_style=commonjs:./client/generated --grpc-web_out=import_style=commonjs,mode=grpcwebtext:./client/generated

run: 
	go run main.go

build:
	go build -o bin/main cmd/main.go

run-server: 
	cmd/server/main 

node:
	go run cmd/main.go --grpc_port=$(GRPC_PORT) --grpc_web_port=$(GRPC_WEB_PORT) $(if $(MASTER_PORT),--master_port=$(MASTER_PORT))

# For development purpose nodes
node1:
	go run cmd/main.go --grpc_port=8000 --grpc_web_port=8001

node2:
	go run cmd/main.go --grpc_port=8002 --grpc_web_port=8003 --master_port=8000

node3:
	go run cmd/main.go --grpc_port=8004 --grpc_web_port=8005 --master_port=8000
