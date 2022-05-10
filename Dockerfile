FROM golang:1.16.0-alpine3.13

ENV CGO_ENABLED=0
ENV GOOS=linux
      
COPY . .

RUN go build .

EXPOSE 8080
CMD ["go run main.go"]