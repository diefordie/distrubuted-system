# System Architecture Documentation

## Gambaran Umum

Sistem ini merupakan Distributed Synchronization System — sistem terdistribusi yang tahan gangguan (fault-tolerant) dan menyediakan sinkronisasi data antar node menggunakan beberapa komponen utama: 
    - Raft Consensus Algorithm untuk koordinasi dan replikasi log,
    - Distributed Lock Manager untuk manajemen kunci bersama,
    - Distributed Cache dengan protokol koherensi MESI, dan
    - Distributed Queue berbasis Consistent Hashing.

Fokus sistem ini adalah menjaga konsistensi dan koherensi data antar node dalam satu klaster tanpa bergantung pada load balancer atau sistem eksternal lain.

## Arsitektur Diagram
```
┌───────────────────────────────────────────────────────────┐
│                     CLUSTER NODES                         │
├───────────────────────────────────────────────────────────┤
│                                                           │
│   ┌──────────────┐          ┌──────────────┐              │
│   │   Node 1     │◄────────►│   Node 2     │◄────────┐    │
│   │ (Follower)   │  Raft +  │  (Leader)    │  Raft + │    │
│   │   :8001      │  Cache   │   :8002      │  Queue  │    │
│   └──────┬───────┘  Sync    └──────┬───────┘         │    │
│          │                         │                 │    │
│          │                         │                 │    │
│          └───────────────►──────────┘                │    │
│                  Raft Heartbeat                      │    │
│                                                      │    │
│                  ┌──────────────┐                    │    │
│                  │   Node 3     │◄───────────────────┘    │
│                  │ (Follower)   │                         │
│                  │   :8003      │                         │
│                  └──────────────┘                         │
└───────────────────────────────────────────────────────────┘

```

Komunikasi antarnode dilakukan melalui HTTP RPC (REST API) menggunakan AioHTTP, dengan serialisasi data berbasis JSON.
## Arsitektur Komponen

### 1. **Raft Consensus Layer**

**Flow Diagram:**
```
Client Command
      │
      ▼
┌─────────────┐     AppendEntries RPC      ┌─────────────┐
│   Leader    │──────────────────────────► │  Follower   │
│   Node 2    │◄────────────────────────── │   Node 1    │
└──────┬──────┘     Success/Failure        └─────────────┘
       │
       │ Majority Ack?
       ▼
┌─────────────┐
│  Commit &   │
│   Apply     │
└─────────────┘
```


### 2. **Distributed Lock Manager**

**Architecture:**
```
┌──────────────────────────────────────────────────────┐
│              Lock Manager Architecture               │
├──────────────────────────────────────────────────────┤
│                                                      │
│  ┌───────────────┐        ┌──────────────┐           │
│  │  Lock Table   │        │ Wait-for     │           │
│  │               │        │   Graph      │           │
│  │ resource1 →   │        │              │           │
│  │  {owners,     │        │ client1 → R2 │           │
│  │   mode,       │        │ client2 → R1 │           │
│  │   acquired_at}│        │   (Deadlock!)│           │
│  └───────┬───────┘        └──────┬───────┘           │
│          │                       │                   │
│          └───────┬───────────────┘                   │
│                  │                                   │
│          ┌───────▼───────┐                           │
│          │  Deadlock     │                           │
│          │  Detector     │                           │
│          │  (DFS Cycle)  │                           │
│          └───────────────┘                           │
│                  │                                   │
│          ┌───────▼───────┐                           │
│          │ Redis Persist │                           │
│          └───────────────┘                           │
└──────────────────────────────────────────────────────┘
```


---

### 3. **Distributed Cache with MESI Protocol**

**MESI State Machine:**
```
                    ┌─────────────┐
                    │   Invalid   │◄────┐
                    └──────┬──────┘     │
                           │            │
                    Local Read          │
                    (Miss, Load)        │
                           │            │
                    ┌──────▼──────┐     │
              ┌────►│  Exclusive  │     │
              │     └──────┬──────┘     │
              │            │            │
        No other      Local Write       │
         caches            │            │
              │     ┌──────▼──────┐     │
              │     │  Modified   │     │
              │     └──────┬──────┘     │
              │            │            │
              │      Write-back     Remote
              │            │         Write
              │     ┌──────▼──────┐     │
              └─────┤   Shared    ├─────┘
                    └─────────────┘
```



### 4. **Distributed Queue with Consistent Hashing**

**Consistent Hash Ring:**
```
             Hash Space: 0 to 2^32-1
                 
                    Node1-vn0
                      │
        Node3-vn2 ────┼──── Node2-vn1
                 \    │    /
                  \   │   /
                   \  │  /
                    \ │ /
                     \│/
               ┌──────┴──────┐
               │   Messages   │
               │  hash(msg)   │
               │  → clockwise │
               │  → node      │
               └──────────────┘
```

## Pola Komunikasi


**Normal (Leader Ada):**
```
Client Request
     │
     ▼
┌─────────┐
│ Node 1  │──────► Forward to Leader ──────►┌─────────┐
│(Follower│                                  │ Node 2  │
└─────────┘                                  │(Leader) │
                                             └────┬────┘
                                                  │
                                             Execute
                                             Command
                                                  │
                                             ┌────▼────┐
                                             │ Response│
                                             │ to Client
                                             └─────────┘
```

**Ketika Leader Bermasalah:**
```
1. Leader fails (Node 2)
         │
         ▼
2. Heartbeat timeout (3-5s)
         │
         ▼
3. New election triggered
         │
         ▼
4. Node 1 becomes candidate
         │
         ▼
5. Request votes from Node 3
         │
         ▼
6. Majority achieved → Node 1 is Leader
         │
         ▼
7. Resume normal operations
```




## 🚀 Deployment Architecture

### Production Deployment (Recommended):
```
┌───────────────────────────────────────────────────┐
│               Load Balancer (HAProxy/Nginx)       │
│                    443 (HTTPS)                    │
└────┬─────────────────────┬────────────────────┬───┘
     │                     │                    │
┌────▼────┐          ┌─────▼────┐        ┌─────▼────┐
│ Node 1  │          │  Node 2  │        │  Node 3  │
│ (AZ-1)  │          │  (AZ-2)  │        │  (AZ-3)  │
│ 8001    │          │  8002    │        │  8003    │
└────┬────┘          └─────┬────┘        └─────┬────┘
     │                     │                    │
     └─────────────────────┴────────────────────┘
                           │
                    ┌──────▼──────┐
                    │ Redis Cluster│
                    │ (Replicated) │
                    └──────────────┘
```


## 🔧 Configuration Parameters

### Environment Variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | node1 | Unique node identifier |
| `NODE_PORT` | 8001 | HTTP API port |
| `CLUSTER_ADDRS` | localhost:8001,... | Comma-separated cluster addresses |
| `REDIS_HOST` | localhost | Redis server host |
| `REDIS_PORT` | 6379 | Redis server port |
| `RAFT_ELECTION_TIMEOUT_MIN` | 3.0 | Min election timeout (seconds) |
| `RAFT_ELECTION_TIMEOUT_MAX` | 5.0 | Max election timeout (seconds) |
| `RAFT_HEARTBEAT_INTERVAL` | 1.0 | Heartbeat interval (seconds) |
| `CACHE_MAX_SIZE` | 1000 | Maximum cache entries |
| `LOCK_DEFAULT_TTL` | 300 | Default lock TTL (seconds) |

