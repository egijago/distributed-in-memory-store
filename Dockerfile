FROM golang:bullseye

WORKDIR /app

COPY . .

RUN go mod download

RUN go build -o main ./cmd/main.go

ENTRYPOINT ["./main"]
