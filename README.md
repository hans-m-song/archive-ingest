# Archive Ingest

## Components

### Discover

Scans a directory for archives of a specific format, parses filenames and announces them to a queue

### Ingester

Listens to a queue and adds data to a postgres database as it is received

## Usage

### Docker

```bash
docker build -t archive-ingest .

# discover a directory
docker run --rm -it --net host archive-ingest discover /data

# ingest into a database
docker run --rm -it --net host archive-ingest ingest
```

### Directly

```bash
go build -o archive-ingest main.go


# discover a directory
./archive-ingest discover /data

# ingest into a database
./archive-ingest ingest
```
