# API Documentation

OpenAPI/Swagger specification: `docs/api_spec.yaml`

## Authentication
Endpoint inti tetap public untuk testing. Endpoint bonus security/PBFT/geo/load balancer memakai header `X-API-Key`.

Contoh key default dari `.env.example`:
- `admin-key`
- `operator-key`
- `viewer-key`

## Response Format

```json
{
  "status": "success|error",
  "data": {...},
  "timestamp": "2026-05-02T10:00:00"
}
```

## Endpoints

### Health & Status

#### Health Check
```
GET /health
```
Returns node health status

**Response**: 200 OK
```json
{
  "status": "healthy",
  "node_id": "node_1",
  "timestamp": "2026-05-02T10:00:00"
}
```

#### Readiness Check
```
GET /readyz
```
Returns if node can serve requests (is leader)

**Response**: 200 OK
```json
{
  "ready": true,
  "node_id": "node_1",
  "is_leader": true
}
```

### Lock Manager

#### Acquire Lock
```
POST /lock/acquire

{
  "resource": "resource_name",
  "lock_type": "exclusive|shared",
  "client_id": "client_id",
  "timeout": 30
}
```

**Response**: 200 OK
```json
{
  "lock_id": "uuid",
  "resource": "resource_name",
  "lock_type": "exclusive",
  "status": "acquired"
}
```

#### Release Lock
```
POST /lock/release

{
  "lock_id": "uuid"
}
```

#### Get Lock Status
```
GET /lock/status/{resource}
```

**Response**: 200 OK
```json
{
  "resource": "resource_name",
  "locks": [
    {
      "lock_id": "uuid",
      "owner": "client_id",
      "lock_type": "exclusive",
      "acquired_at": "2026-05-02T10:00:00"
    }
  ]
}
```

### Distributed Queue

#### Publish Message
```
POST /queue/publish

{
  "topic": "topic_name",
  "payload": {"key": "value"}
}
```

**Response**: 200 OK
```json
{
  "msg_id": "uuid",
  "topic": "topic_name",
  "status": "published"
}
```

#### Consume Messages
```
POST /queue/consume

{
  "topic": "topic_name",
  "consumer_id": "consumer_id",
  "batch_size": 1
}
```

**Response**: 200 OK
```json
{
  "topic": "topic_name",
  "consumer_id": "consumer_id",
  "messages": [
    {
      "msg_id": "uuid",
      "payload": {"key": "value"},
      "delivery_count": 1
    }
  ]
}
```

#### Acknowledge Message
```
POST /queue/ack

{
  "msg_id": "uuid",
  "consumer_id": "consumer_id"
}
```

### Cache

#### Get Cache Value
```
GET /cache/get/{key}
```

**Response**: 200 OK
```json
{
  "key": "cache_key",
  "value": "cache_value"
}
```

**Response**: 404 Not Found
```json
{
  "error": "Key not found"
}
```

#### Put Cache Value
```
POST /cache/put

{
  "key": "cache_key",
  "value": "cache_value",
  "ttl": 3600
}
```

**Response**: 200 OK
```json
{
  "key": "cache_key",
  "status": "cached"
}
```

#### Invalidate Cache
```
POST /cache/invalidate/{key}
```

### Metrics

#### Get System Metrics
```
GET /metrics
```

**Response**: 200 OK
```json
{
  "timestamp": "2026-05-02T10:00:00",
  "node_id": "node_1",
  "raft": {
    "term": 1,
    "state": "leader",
    "log_size": 10
  },
  "locks": {
    "total_locks": 5,
    "total_waiting": 2,
    "resources": 3
  },
  "queue": {
    "total_partitions": 10,
    "total_messages": 100,
    "unacked_messages": 5
  },
  "cache": {
    "cache_size": 50,
    "max_size": 1000,
    "hits": 1000,
    "misses": 50,
    "hit_rate": "95.24%"
  }
}
```

#### Get Raft Info
```
GET /raft/info
```

**Response**: 200 OK
```json
{
  "node_id": "node_1",
  "current_term": 1,
  "state": "leader",
  "leader_id": "node_1",
  "log_size": 10,
  "commit_index": 10,
  "last_applied": 10,
  "voted_for": "node_1",
  "peers": ["node_2", "node_3", "node_4"]
}
```

## Bonus Endpoints

### Security

#### Encrypt Payload
```
POST /security/encrypt
Header: X-API-Key: operator-key
```

#### Read Audit Log
```
GET /security/audit
Header: X-API-Key: viewer-key
```

### PBFT

#### Submit PBFT Request
```
POST /bonus/pbft/commit
Header: X-API-Key: operator-key

{
  "payload": {"transaction": "write-x"},
  "faulty_nodes": 1
}
```

### Geo Routing

#### Choose Best Region
```
GET /bonus/geo/route?client_region=us-east
Header: X-API-Key: viewer-key
```

### Adaptive Load Balancer

#### Choose Next Node
```
POST /bonus/load-balance/choose
Header: X-API-Key: operator-key

{
  "workload": 1.0,
  "exclude": ["node_4"]
}
```
