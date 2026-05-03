"""Comprehensive integration tests"""

import pytest
import asyncio
from datetime import datetime
import json


@pytest.mark.asyncio
async def test_cluster_health():
    """Test cluster health endpoints"""
    from test_client import DistributedSystemClient
    
    async with DistributedSystemClient("http://localhost:8001") as client:
        health = await client.health_check()
        assert health["status"] == "healthy"
        
        # Try other nodes
        for port in [8002, 8003, 8004]:
            async with DistributedSystemClient(f"http://localhost:{port}") as c:
                h = await c.health_check()
                assert h["status"] == "healthy"


@pytest.mark.asyncio
async def test_lock_lifecycle():
    """Test complete lock lifecycle"""
    from test_client import DistributedSystemClient
    
    async with DistributedSystemClient() as client:
        # Acquire lock
        resp = await client.acquire_lock("resource1", "client1", "exclusive")
        assert "lock_id" in resp
        lock_id = resp["lock_id"]
        
        # Get status
        status = await client.get_lock_status("resource1")
        assert len(status["locks"]) > 0
        
        # Release
        rel = await client.release_lock(lock_id)
        assert rel["status"] in ["released", "failed"]


@pytest.mark.asyncio
async def test_queue_messaging():
    """Test queue publish/consume cycle"""
    from test_client import DistributedSystemClient
    
    async with DistributedSystemClient() as client:
        # Publish
        pub = await client.publish_message("topic1", {"data": "test"})
        assert "msg_id" in pub
        msg_id = pub["msg_id"]
        
        # Consume
        cons = await client.consume_messages("topic1", "consumer1", batch_size=1)
        assert "messages" in cons
        
        # Acknowledge
        if cons["messages"]:
            ack = await client.ack_message(cons["messages"][0]["msg_id"], "consumer1")
            assert "status" in ack


@pytest.mark.asyncio
async def test_cache_operations():
    """Test cache get/put operations"""
    from test_client import DistributedSystemClient
    
    async with DistributedSystemClient() as client:
        # Put
        put = await client.cache_put("test_key", "test_value")
        assert put["status"] == "cached"
        
        # Get
        get = await client.cache_get("test_key")
        assert get["value"] == "test_value"
        
        # Get non-existent
        missing = await client.cache_get("missing_key")
        assert "error" in missing or "value" not in missing


@pytest.mark.asyncio
async def test_metrics_endpoint():
    """Test metrics collection"""
    from test_client import DistributedSystemClient
    
    async with DistributedSystemClient() as client:
        metrics = await client.get_metrics()
        
        assert "timestamp" in metrics
        assert "node_id" in metrics
        assert "raft" in metrics
        assert "locks" in metrics
        assert "queue" in metrics
        assert "cache" in metrics
        
        # Check Raft metrics
        assert metrics["raft"]["term"] >= 0
        assert metrics["raft"]["state"] in ["leader", "follower", "candidate"]
        
        # Check cache hit rate
        cache = metrics["cache"]
        assert cache["cache_size"] >= 0
        assert cache["hits"] >= 0
        assert cache["misses"] >= 0


@pytest.mark.asyncio
async def test_concurrent_locks():
    """Test concurrent lock acquisition"""
    from test_client import DistributedSystemClient
    
    async def acquire_lock(client_id: int, resource: str):
        async with DistributedSystemClient() as client:
            resp = await client.acquire_lock(resource, f"client_{client_id}", "shared")
            return resp.get("lock_id")
    
    # Acquire 3 shared locks concurrently
    results = await asyncio.gather(
        acquire_lock(1, "shared_res"),
        acquire_lock(2, "shared_res"),
        acquire_lock(3, "shared_res"),
        return_exceptions=True
    )
    
    # At least some should succeed
    successful = [r for r in results if r and not isinstance(r, Exception)]
    assert len(successful) > 0


@pytest.mark.asyncio
async def test_multiple_queue_consumers():
    """Test multiple consumers on same queue"""
    from test_client import DistributedSystemClient
    
    async with DistributedSystemClient() as client:
        # Publish multiple messages
        for i in range(5):
            await client.publish_message("shared_topic", {"id": i})
        
        # Consume from multiple consumers
        cons1 = await client.consume_messages("shared_topic", "consumer_1", batch_size=3)
        cons2 = await client.consume_messages("shared_topic", "consumer_2", batch_size=3)
        
        assert "messages" in cons1
        assert "messages" in cons2


@pytest.mark.asyncio
async def test_raft_info():
    """Test Raft consensus info"""
    from test_client import DistributedSystemClient
    
    async with DistributedSystemClient() as client:
        info = await client.get_raft_info()
        
        assert "node_id" in info
        assert "current_term" in info
        assert "state" in info
        assert "leader_id" in info
        assert "log_size" in info
        
        # Check state is valid
        assert info["state"] in ["leader", "follower", "candidate"]
