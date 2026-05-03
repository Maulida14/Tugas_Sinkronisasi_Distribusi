# Architecture

## System Overview

Distributed Synchronization System terdiri dari 4 komponen utama yang terintegrasi melalui Raft consensus:

### 1. Raft Consensus Protocol

Custom implementation log-based consensus dengan fitur:
- Request voting untuk leader election
- Heartbeat mechanism
- Log replication
- Commit index management
- Term tracking

**Keunikan**: Implementasi dari scratch, berbeda pattern dari reference

### 2. Distributed Lock Manager

Features:
- Shared & Exclusive locks
- Lock queue untuk fairness
- Deadlock detection menggunakan wait-for graph
- Cycle detection dengan DFS algorithm
- TTL-based lock expiration

**Architecture**:
```
Client Requests
     ↓
Lock Manager API
     ↓
Try Acquire Lock
     ↓
Raft Replication
     ↓
State Machine Apply
```

### 3. Distributed Queue

Features:
- Consistent hashing untuk partitioning
- Virtual nodes untuk load balancing
- Message replication ke multiple nodes
- At-least-once delivery guarantee
- ACK-based persistence

**Consistent Hash Ring**:
```
Primary Replica → Node A
Secondary → Node B
Tertiary → Node C
```

### 4. MOESI Cache Coherence

States:
- **Modified (M)**: Only this node has valid copy
- **Owned (O)**: Multiple readers, this is owner
- **Exclusive (E)**: Only reader, can be modified
- **Shared (S)**: Read-only shared
- **Invalid (I)**: Cache line invalid

**Eviction**: LRU policy dengan TTL tracking

## Data Flow

### Write Operation
1. Client sends write request
2. Lock Manager acquires exclusive lock
3. Cache invalidates other copies
4. Write propagates through Raft
5. Committed to state machine
6. MOESI state = Modified

### Read Operation
1. Client sends read request
2. Check local cache state
3. If shared, need read lock
4. If exclusive, own copy
5. Update LRU access time
6. Return value

## Network Architecture

Nodes berkomunikasi via:
- HTTP REST API
- JSON message format
- Async I/O dengan aiohttp
- Redis untuk distributed state

Docker compose setup:
- 4 node cluster
- Shared Redis
- Custom network bridge
- Health checks

## Deployment

### Single Node
```bash
python -m src.app
```

### Cluster (Docker)
```bash
docker compose up -d
```

### Node Configuration
Via environment variables:
- NODE_ID
- NODE_PORT
- CLUSTER_PEERS
- REDIS_HOST
