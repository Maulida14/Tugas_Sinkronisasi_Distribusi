# Deployment Guide

## Prerequisites

- Docker Desktop (latest)
- Docker Compose 2.0+
- Python 3.11+ (untuk local development)
- 4GB+ RAM (untuk 4-node cluster)

## Docker Setup

### Pull Latest Image
```bash
docker pull python:3.11-slim
docker pull redis:7-alpine
```

### Build Custom Image
```bash
docker compose -f docker/docker-compose.yml build
```

### Start Cluster
```bash
docker compose -f docker/docker-compose.yml up -d
```

Verify nodes are running:
```bash
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health
curl http://localhost:8004/health
```

### Scale Cluster

Edit `docker-compose.yml` to add more nodes:

```yaml
node_5:
  build: ...
  environment:
    NODE_ID: node_5
    NODE_PORT: 8005
    CLUSTER_PEERS: node_1:8001,node_2:8002,node_3:8003,node_4:8004
```

Then rebuild and restart:
```bash
docker compose -f docker/docker-compose.yml up -d --scale node=5
```

## Local Development

### Setup Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Run Single Node
```bash
export NODE_ID=local_node
export NODE_PORT=8001
export CLUSTER_PEERS=localhost:8002,localhost:8003
python -m src.app
```

### Run Multiple Nodes Locally

Terminal 1:
```bash
NODE_ID=node1 NODE_PORT=8001 python -m src.app
```

Terminal 2:
```bash
NODE_ID=node2 NODE_PORT=8002 python -m src.app
```

Terminal 3:
```bash
NODE_ID=node3 NODE_PORT=8003 python -m src.app
```

## Configuration

Edit `.env` file:

```bash
# Node settings
NODE_ID=node_1
NODE_PORT=8001
CLUSTER_PEERS=localhost:8002,localhost:8003

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# Raft
HEARTBEAT_INTERVAL=0.5
ELECTION_TIMEOUT_MIN=1.5
ELECTION_TIMEOUT_MAX=3.0

# Locks
DEADLOCK_DETECTION_INTERVAL=5
MAX_LOCK_WAIT_TIME=30

# Cache
CACHE_MAX_SIZE=1000
CACHE_TTL=3600

# Queue
QUEUE_REPLICATION_FACTOR=3
QUEUE_PERSISTENCE_ENABLED=true

# Security
ENABLE_ENCRYPTION=true
ENABLE_AUDIT=true

# Logging
LOG_LEVEL=INFO
```

## Monitoring

### Check Node Status
```bash
curl http://localhost:8001/status
```

### View Raft Info
```bash
curl http://localhost:8001/raft/info
```

### Get Metrics
```bash
curl http://localhost:8001/metrics | jq
```

### Docker Logs
```bash
docker logs -f node_1
docker logs -f node_2
```

## Troubleshooting

### Node won't start
- Check Redis is running: `docker ps | grep redis`
- Check ports are available: `netstat -an | grep 8001`
- Check logs: `docker logs node_1`

### Leader election fails
- Ensure all nodes can reach each other: `docker network ls`
- Check CLUSTER_PEERS config
- Check heartbeat timeout settings

### Cache inconsistency
- Restart affected nodes
- Clear cache: `curl -X POST http://localhost:8001/cache/clear`

### High CPU usage
- Reduce LOG_LEVEL to WARNING
- Increase HEARTBEAT_INTERVAL
- Check for stuck locks: `curl http://localhost:8001/metrics`

## Shutdown

```bash
docker compose -f docker/docker-compose.yml down
```

To remove volumes:
```bash
docker compose -f docker/docker-compose.yml down -v
```

## Performance Tuning

### For High Throughput
```
HEARTBEAT_INTERVAL=0.2
CACHE_MAX_SIZE=5000
QUEUE_REPLICATION_FACTOR=1
```

### For High Availability
```
HEARTBEAT_INTERVAL=1.0
CACHE_MAX_SIZE=500
QUEUE_REPLICATION_FACTOR=3
```

## Production Deployment

1. Use managed Redis (AWS ElastiCache, Azure Cache)
2. Deploy on Kubernetes with multiple replicas
3. Setup monitoring with Prometheus
4. Enable encryption and authentication
5. Use external load balancer
6. Setup backup strategy for logs
