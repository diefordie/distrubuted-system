# Distributed Synchronization System

## Overview
Sistem sinkronisasi terdistribusi dengan Raft consensus, distributed locks, queue, dan cache coherence.

## Architecture


## Setup
```bash
pip install -r requirements.txt
cp .env.example .env
# Edit NODE_ID, NODE_PORT, CLUSTER_ADDRS di .env
```

## Run Locally (3 Nodes)
Terminal 1: `NODE_ID=node1 NODE_PORT=8001 python src/main.py`
Terminal 2: `NODE_ID=node2 NODE_PORT=8002 python src/main.py`
Terminal 3: `NODE_ID=node3 NODE_PORT=8003 python src/main.py`

## Docker
`docker compose up `

## API Docs
Akses http://localhost:8001/docs untuk Swagger UI


## Video dan Laporan


## Testing
`pytest tests/`
```

---
