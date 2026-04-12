FROM golang:1.23

WORKDIR /app

COPY go.mod ./
RUN go mod download
COPY . .
RUN go build -o proxy-service .

EXPOSE 8000

CMD ["./proxy-service"]
