FROM golang:latest

WORKDIR /build

COPY go.mod go.sum ./

RUN go mod download

COPY /build/main .

EXPOSE 50051

CMD ["./main"]
