proto: 
	protoc --go_out=. --go-grpc_out=. pkg/proto/*.proto

.PHONY:build

build:
	go build -o ./build/main ./cmd

run-server:
	./build/main

run:
	go run cmd/main.go