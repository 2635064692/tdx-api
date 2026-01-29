# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Environment

**Container**: `go-dev-container`
- Current directory mounted to `/app/tdx-api/` inside container

**Workflow**:
- **Local (host)**: Code search, analysis, and editing
- **Container**: Build, test, and verification

### Container Commands
```bash
# Attach to container
docker exec -it go-dev-container /bin/bash

# In container, work at /app/tdx-api/
cd /app/tdx-api/

# Build and run
cd web && go run .

# Run tests
go test ./...

# Build binary
go build -o tdx-api .
```

## Project Overview

TDX-API is a stock data query system based on the Tongda Xin (TDX) protocol. It provides:
- Real-time market data (K-line, minute, five-level quotes)
- RESTful API (32 endpoints)
- Web visualization interface
- Docker containerized deployment

## Commands

### Docker Deployment (Recommended)
```bash
docker-compose up -d
# Access at http://localhost:8080
```

### Source Code Running
```bash
# Go 1.22+ required

# Download dependencies
go mod download

# Run web server (must use go run .)
cd web && go run .

# Access at http://localhost:8080
```

### Docker Build
```bash
docker build -t tdx-stock-web .
docker-compose up -d
docker-compose logs -f
```

## Architecture

```
tdx-api/
├── client.go          # TDX client core with connection pool management
├── protocol/          # TDX protocol implementation (binary frame encoding)
├── pool.go            # Channel-based connection pool
├── manage.go          # Manager with pool, cron, codes, workday
├── workday.go         # Trading calendar management
├── codes.go           # Stock code management
├── dial.go            # Dial strategies (range, random, hosts)
├── hosts.go           # Server addresses configuration
│
├── protocol/
│   ├── frame.go       # Binary frame encode/decode, zlib compression
│   ├── const.go       # Protocol constants
│   ├── types.go       # Type definitions
│   └── model_*.go     # Data models (Quote, Kline, Trade, Minute)
│
├── extend/            # Extended features
│   ├── pull-kline.go  # K-line data pull tasks
│   ├── pull-trade.go  # Trade data pull tasks
│   ├── spider-ths.go  # Tonghuashun data crawler
│   └── codes-*.go     # Code services
│
├── web/               # Web application
│   ├── server.go      # HTTP server, 32 API endpoints registration
│   ├── server_api_extended.go  # Extended API handlers
│   ├── tasks.go       # Task management (pull, list, cancel)
│   └── static/        # Frontend (HTML/CSS/JS + ECharts)
│
└── example/           # 34 usage examples
```

### Key Modules

| File | Purpose |
|------|---------|
| **client.go** | Client core, Dial* functions, message handler, SendFrame with wait/timeout |
| **pool.go** | Channel-based connection pool (Get/Put/Do/Go methods) |
| **manage.go** | Manager struct combining Pool, Cron scheduler, Codes, Workday |
| **protocol/frame.go** | Binary protocol: 0x0C prefix, zlib decompression, frame encode/decode |
| **dial.go** | Dial strategies: RangeDial, RandomDial, HostDial, TCPDial |

### TDX Protocol

- **Frame Header**: `0x0C` fixed prefix
- **Response Header**: `0xB1CB7400`
- **Compression**: zlib deflate for response data
- **Heartbeat**: 30-second ping to keep connection alive
- **Message Flow**: Request frame → Response frame → Decompress → Parse

### Connection Management

```go
// Dial strategies (dial.go)
DialDefault()           // Auto-select fastest server from Hosts
DialHostsRange()        // Try hosts in order, stop on first success
DialHostsRandom()       // Random server selection
DialHosts()             // Round-robin with retry

// Pool (pool.go)
NewPool(dialFunc, number)  // Channel-based pool
Get()                       // Acquire connection
Put()                       // Return connection
Do(fn)                      // Execute with auto get/put
Go(fn)                      // Execute async with auto get/put
```

### API Endpoints (32 total)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/quote` | GET | Five-level quotes |
| `/api/kline` | GET | K-line data |
| `/api/minute` | GET | Minute data |
| `/api/trade` | GET | Trade records |
| `/api/search` | GET | Stock search |
| `/api/stock-info` | GET | Comprehensive info |
| `/api/codes` | GET | Code list |
| `/api/batch-quote` | POST | Batch quotes |
| `/api/kline-history` | GET | Historical K-line |
| `/api/index` | GET | Index data |
| `/api/market-stats` | GET | Market statistics |
| `/api/tasks/pull-kline` | POST | Create K-line pull task |
| `/api/tasks/pull-trade` | POST | Create trade pull task |
| `/api/tasks` | GET | List tasks |
| `/api/health` | GET | Health check |

### TDX Servers

System auto-connects to fastest server from:
- 124.71.187.122 (Shanghai)
- 122.51.120.217 (Shanghai)
- 121.36.54.217 (Beijing)
- 124.71.85.110 (Guangzhou)

### Scheduled Tasks

- **Workday calendar**: Auto-update trading days via cron
- **Heartbeat**: 30-second keep-alive ping
- **Auto-reconnect**: On connection failure

## Dependencies

| Package | Purpose |
|---------|---------|
| github.com/injoyai/* | Base framework (base, conv, ios, logs) |
| xorm.io/xorm | ORM for SQLite/MySQL |
| github.com/robfig/cron/v3 | Scheduled tasks |
| github.com/glebarez/go-sqlite | SQLite driver |

## Important Notes

- **Go version**: Root module requires Go 1.20+, web module requires Go 1.23+
- **Entry point**: Use `cd web && go run .` - NOT `go run server.go`
- **Multi-module**: Two go.mod files (root + web), they replace each other
- **Data source**: TDX public servers, data may have delays
- **Client initialization**: Creates pool of 4 connections by default
