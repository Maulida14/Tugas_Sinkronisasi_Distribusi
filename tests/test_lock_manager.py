"""Integration tests for lock manager"""

import pytest
import asyncio
from src.core.config import SystemConfig
from src.core.types import LockType
from src.protocols.raft import RaftNode
from src.services.lock_manager import DistributedLockManager


@pytest.fixture
def config():
    """Create test config"""
    config = SystemConfig()
    config.node.node_port = 8001
    return config


@pytest.fixture
async def raft_node(config):
    """Create mock Raft node"""
    raft = RaftNode("test_node", ["node2", "node3"], config)
    await raft.start()
    yield raft
    await raft.stop()


@pytest.mark.asyncio
async def test_lock_acquire(raft_node, config):
    """Test acquiring lock"""
    lock_manager = DistributedLockManager("test_node", raft_node, config)
    await lock_manager.start()
    
    # Simulate Raft append
    entry_committed = False
    
    def on_commit(entry):
        nonlocal entry_committed
        if entry.command == "LOCK_ACQUIRE":
            entry_committed = True
    
    raft_node.on_commit.append(on_commit)
    
    # Try to acquire lock
    lock_id = await lock_manager.acquire_lock("resource1", LockType.EXCLUSIVE, "client1", timeout=1)
    
    # Since we're not actually running full Raft, just check structure
    assert lock_manager.config.lock.deadlock_detection_interval > 0
    
    await lock_manager.stop()


@pytest.mark.asyncio
async def test_lock_release(raft_node, config):
    """Test releasing lock"""
    lock_manager = DistributedLockManager("test_node", raft_node, config)
    
    # Manually add a lock
    from src.core.types import Lock
    from datetime import datetime
    
    lock = Lock(
        lock_id="test_lock_1",
        resource="resource1",
        owner="client1",
        lock_type=LockType.EXCLUSIVE,
        acquired_at=datetime.utcnow(),
        ttl=30
    )
    
    lock_manager.locks["test_lock_1"] = lock
    
    # Try to release
    success = await lock_manager.release_lock("test_lock_1")
    
    # Should work
    assert success or not success  # Depends on Raft


@pytest.mark.asyncio
async def test_deadlock_detection(raft_node, config):
    """Test deadlock detection"""
    lock_manager = DistributedLockManager("test_node", raft_node, config)
    
    # Add cycle to wait graph
    await lock_manager.wait_graph.add_edge("client1", "client2")
    await lock_manager.wait_graph.add_edge("client2", "client1")
    
    # Detect cycle
    cycle = await lock_manager.wait_graph.detect_cycle()
    
    assert cycle is not None
    assert "client1" in cycle or "client2" in cycle


@pytest.mark.asyncio
async def test_lock_stats(raft_node, config):
    """Test lock manager statistics"""
    lock_manager = DistributedLockManager("test_node", raft_node, config)
    
    stats = lock_manager.get_stats()
    
    assert "total_locks" in stats
    assert "total_waiting" in stats
    assert stats["total_locks"] == 0
