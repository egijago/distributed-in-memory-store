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

proto-client:
	protoc -I=. proto/*.proto --js_out=import_style=commonjs:./client/generated --grpc-web_out=import_style=commonjs,mode=grpcwebtext:./client/generated

run: 
	go run main.go

build:
	go build -o bin/main cmd/main.go

run-server: 
	cmd/server/main 

node:
	go run cmd/main.go --grpc_port=$(GRPC_PORT) --grpc_web_port=$(GRPC_WEB_PORT) $(if $(MASTER_PORT),--master_port=$(MASTER_PORT))

node1:
	go run cmd/main.go --grpc_port=8000 --grpc_web_port=7000 --et=1000 --hi=2000 --hrto=2000 --ht=4000 --hts=2000 -verbose

node2:
	go run cmd/main.go --grpc_port="8001" --grpc_web_port="7001" --master_port="8000" --et=1000 --hi=2000 --hrto=2000 --ht=4000 --hts=2000 -verbose

node3:
	go run cmd/main.go --grpc_port="8002" --grpc_web_port="7002" --master_port="8000" --et=1000 --hi=2000 --hrto=2000 --ht=4000 --hts=2000 -verbose

node4:
	go run cmd/main.go --grpc_port="8003" --grpc_web_port="7003" --master_port="8000" --et=1000 --hi=2000 --hrto=2000 --ht=4000 --hts=2000 -verbose
