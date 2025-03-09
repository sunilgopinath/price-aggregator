# price-aggregator

## Getting started

```bash
$> git clone git@github.com:sunilgopinath/price-aggregator.git
$> cd price-aggregator
```

```bash
docker compose up -d
```

This will start postgres, kafka

In terminal 1

```bash
$> go run cmd/alerts/main.go
```

In terminal 2

```bash
$> go run cmd/price_processing/main.go
```

In terminal 3

```bash
go run cmd/ingestion/main.go
```