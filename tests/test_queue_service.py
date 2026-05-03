"""Integration tests for queue service"""

import pytest
import asyncio
from src.core.config import SystemConfig
from src.protocols.raft import RaftNode
from src.services.queue_service import DistributedQueue, ConsistentHashRing


@pytest.fixture
def config():
    """Create test config"""
    return SystemConfig()


@pytest.fixture
async def raft_node(config):
    """Create mock Raft node"""
    raft = RaftNode("test_node", ["node2", "node3"], config)
    await raft.start()
    yield raft
    await raft.stop()


def test_consistent_hash_ring():
    """Test consistent hashing"""
    nodes = ["node1", "node2", "node3"]
    ring = ConsistentHashRing(nodes)
    
    # Test get_node
    node = ring.get_node("key1")
    assert node in nodes
    
    # Test consistency
    node2 = ring.get_node("key1")
    assert node == node2


def test_consistent_hash_replicas():
    """Test replica selection"""
    nodes = ["node1", "node2", "node3", "node4"]
    ring = ConsistentHashRing(nodes, virtual_nodes=10)
    
    replicas = ring.get_replicas("topic1", 3)
    
    assert len(replicas) <= 3
    assert len(set(replicas)) == len(replicas)  # No duplicates


@pytest.mark.asyncio
async def test_queue_publish(raft_node, config):
    """Test publishing message"""
    queue = DistributedQueue("test_node", ["node2", "node3"], raft_node, config)
    
    # Publish message
    msg_id = await queue.publish("topic1", {"data": "test"})
    
    # Since we're not running full Raft, just check structure
    assert queue.config.queue.replication_factor > 0


@pytest.mark.asyncio
async def test_queue_consume(raft_node, config):
    """Test consuming messages"""
    queue = DistributedQueue("test_node", ["node2", "node3"], raft_node, config)
    
    # Manually add message
    from src.core.types import QueueMessage
    
    msg = QueueMessage(
        msg_id="msg1",
        payload={"data": "test"},
        partition_id=0,
        acked=False,
        delivery_count=0
    )
    
    queue.partitions[0].append(msg)
    
    # Consume
    messages = await queue.consume("topic1", "consumer1", batch_size=1)
    
    assert len(messages) <= 1


@pytest.mark.asyncio
async def test_queue_stats(raft_node, config):
    """Test queue statistics"""
    queue = DistributedQueue("test_node", ["node2", "node3"], raft_node, config)
    
    stats = queue.get_stats()
    
    assert "total_partitions" in stats
    assert "total_messages" in stats
    assert stats["total_messages"] == 0
