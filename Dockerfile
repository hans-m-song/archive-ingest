FROM golang:1-bullseye

WORKDIR /archive-ingest

COPY go.mod go.sum makefile ./
RUN go mod download

COPY main.go ./
COPY announcer ./announcer
COPY ingest ./ingest
COPY parse ./parse
COPY util ./util

RUN go build
ENTRYPOINT ./archive-ingest
