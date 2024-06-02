.PHONY: proto

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
