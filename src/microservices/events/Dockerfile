FROM golang:1.23

WORKDIR /app

COPY go.mod ./
RUN go mod download
COPY . .
RUN go build -o events-service .

EXPOSE 8082

CMD ["./events-service"]
