name = archive-ingest

build:
	go build -o $(name) main.go

docker:
	docker build -t $(name) .

discover:
	docker run --rm -it -v data:/data --net host $(name) discover /data

clean:
	rm -f $(name)
