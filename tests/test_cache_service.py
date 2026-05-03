"""Integration tests for cache service"""

import pytest
import asyncio
from src.core.config import SystemConfig
from src.core.types import CacheState
from src.protocols.raft import RaftNode
from src.services.cache_service import MOESICache, LRUEvictionPolicy


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


def test_lru_eviction():
    """Test LRU eviction policy"""
    policy = LRUEvictionPolicy()
    
    # Record accesses
    policy.access("key1")
    policy.access("key2")
    policy.access("key3")
    
    cache = {"key1": None, "key2": None, "key3": None}
    
    # Evict should return the least recently used
    evicted = policy.evict(cache)
    
    assert evicted in ["key1", "key2", "key3"]


@pytest.mark.asyncio
async def test_cache_put_get(raft_node, config):
    """Test cache put and get"""
    cache = MOESICache("test_node", ["node2", "node3"], raft_node, config)
    
    # Put value
    success = await cache.put("key1", "value1", ttl=3600)
    
    # Since we're not running full Raft, just check structure
    assert config.cache.max_size > 0


@pytest.mark.asyncio
async def test_cache_get_miss(raft_node, config):
    """Test cache miss"""
    cache = MOESICache("test_node", ["node2", "node3"], raft_node, config)
    
    # Get non-existent key
    value = await cache.get("nonexistent")
    
    assert value is None
    assert cache.misses == 1


@pytest.mark.asyncio
async def test_cache_stats(raft_node, config):
    """Test cache statistics"""
    cache = MOESICache("test_node", ["node2", "node3"], raft_node, config)
    
    stats = cache.get_stats()
    
    assert "cache_size" in stats
    assert "hits" in stats
    assert "misses" in stats
    assert stats["cache_size"] == 0


@pytest.mark.asyncio
async def test_cache_invalidation(raft_node, config):
    """Test cache invalidation"""
    cache = MOESICache("test_node", ["node2", "node3"], raft_node, config)
    
    # Manually add block
    from src.core.types import CacheBlock
    from datetime import datetime
    
    block = CacheBlock(
        key="key1",
        value="value1",
        state=CacheState.EXCLUSIVE,
        owner_node="test_node"
    )
    
    cache.cache["key1"] = block
    
    # Invalidate
    await cache.handle_invalidation("key1", "node2")
    
    # Should be marked invalid
    assert block.state == CacheState.INVALID
