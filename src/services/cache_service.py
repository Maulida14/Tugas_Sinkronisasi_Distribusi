"""Distributed Cache with MOESI Coherence Protocol"""

import asyncio
import logging
from typing import Dict, Optional, List, Any, Set
from datetime import datetime, timedelta
from collections import OrderedDict
import time

from ..core.types import CacheBlock, CacheState, LogEntry
from ..core.config import SystemConfig


logger = logging.getLogger(__name__)


class CacheEvictionPolicy:
    """Base class for cache eviction policies"""
    
    def evict(self, cache: Dict[str, CacheBlock]) -> Optional[str]:
        """Return key to evict"""
        raise NotImplementedError


class LRUEvictionPolicy(CacheEvictionPolicy):
    """Least Recently Used eviction policy"""
    
    def __init__(self):
        self.access_times: Dict[str, float] = {}
    
    
    def access(self, key: str):
        """Record access time"""
        self.access_times[key] = time.time()
    
    
    def evict(self, cache: Dict[str, CacheBlock]) -> Optional[str]:
        """Evict least recently used item"""
        if not cache:
            return None
        
        # Find key with oldest access time
        min_key = None
        min_time = float('inf')
        
        for key in cache:
            access_time = self.access_times.get(key, 0)
            if access_time < min_time:
                min_time = access_time
                min_key = key
        
        return min_key


class MOESICache:
    """Distributed cache with MOESI coherence protocol"""
    
    def __init__(self, node_id: str, peer_nodes: List[str], raft_node, config: SystemConfig):
        self.node_id = node_id
        self.peer_nodes = peer_nodes
        self.raft_node = raft_node
        self.config = config
        
        # Cache storage
        self.cache: Dict[str, CacheBlock] = {}
        
        # Eviction policy
        self.eviction_policy = LRUEvictionPolicy()
        
        # Statistics
        self.hits = 0
        self.misses = 0
        self.invalidations = 0
        
        # Pending invalidations
        self.pending_invalidations: Set[str] = set()
        
        # Register as commit callback
        self.raft_node.on_commit.append(self._handle_commit)
    
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        block = self.cache.get(key)
        
        if block is None:
            self.misses += 1
            logger.debug(f"Cache miss for key {key}")
            return None
        
        # Check if valid
        if block.state == CacheState.INVALID:
            self.misses += 1
            del self.cache[key]
            return None
        
        # Check TTL
        if block.ttl > 0:
            age = (datetime.utcnow() - block.last_modified).total_seconds()
            if age > block.ttl:
                self.misses += 1
                await self._invalidate_block(key)
                return None
        
        # Record access for LRU
        self.eviction_policy.access(key)
        
        # State transition
        if block.state == CacheState.EXCLUSIVE:
            # E -> S (when read by other node, would transition to S, but we're just reading locally)
            pass
        
        self.hits += 1
        logger.debug(f"Cache hit for key {key}, value: {block.value}")
        return block.value
    
    
    async def put(self, key: str, value: Any, ttl: float = None) -> bool:
        """Put value in cache with MOESI state management"""
        if ttl is None:
            ttl = self.config.cache.ttl
        
        # Check if key exists
        existing = self.cache.get(key)
        
        if existing and existing.state in [CacheState.MODIFIED, CacheState.OWNED]:
            # We have ownership, can update directly
            block = CacheBlock(
                key=key,
                value=value,
                state=CacheState.MODIFIED,
                owner_node=self.node_id,
                ttl=ttl
            )
        else:
            # Acquire exclusive ownership
            success = await self.raft_node.append_entry(
                command="CACHE_ACQUIRE_EXCLUSIVE",
                data={"key": key, "owner": self.node_id}
            )
            
            if not success:
                logger.warning(f"Failed to acquire exclusive lock for {key}")
                return False
            
            # Invalidate at other nodes
            await self._broadcast_invalidation(key)
            
            block = CacheBlock(
                key=key,
                value=value,
                state=CacheState.EXCLUSIVE,
                owner_node=self.node_id,
                ttl=ttl
            )
        
        # Replicate through Raft
        success = await self.raft_node.append_entry(
            command="CACHE_PUT",
            data={
                "key": key,
                "value": value if isinstance(value, (str, int, float, bool, list, dict)) else str(value),
                "state": block.state.value,
                "owner": self.node_id,
                "ttl": ttl
            }
        )
        
        if success:
            self.cache[key] = block
            self.eviction_policy.access(key)
            logger.info(f"Put key {key} in cache with state {block.state.value}")
            
            # Check cache size
            if len(self.cache) > self.config.cache.max_size:
                await self._evict_one()
            
            return True
        
        return False
    
    
    async def _evict_one(self):
        """Evict one item from cache"""
        key_to_evict = self.eviction_policy.evict(self.cache)
        
        if key_to_evict:
            await self._invalidate_block(key_to_evict)
            logger.info(f"Evicted key {key_to_evict} from cache (size: {len(self.cache)})")
    
    
    async def _invalidate_block(self, key: str):
        """Invalidate cache block"""
        if key in self.cache:
            block = self.cache[key]
            block.state = CacheState.INVALID
            
            success = await self.raft_node.append_entry(
                command="CACHE_INVALIDATE",
                data={"key": key}
            )
            
            if success:
                del self.cache[key]
                self.invalidations += 1
                logger.debug(f"Invalidated block {key}")
    
    
    async def _broadcast_invalidation(self, key: str):
        """Broadcast invalidation to all cache nodes"""
        success = await self.raft_node.append_entry(
            command="CACHE_INVALIDATE_ALL",
            data={"key": key, "except_node": self.node_id}
        )
        
        if success:
            self.pending_invalidations.add(key)
            logger.debug(f"Broadcasting invalidation for {key}")
    
    
    async def handle_invalidation(self, key: str, from_node: str):
        """Handle invalidation request from other node"""
        if key in self.cache:
            block = self.cache[key]
            
            # State transition for MOESI
            if block.state == CacheState.EXCLUSIVE:
                # E -> I when another node requests invalidation
                if from_node != self.node_id:
                    block.state = CacheState.INVALID
            elif block.state == CacheState.MODIFIED:
                # M -> I after ownership is transferred elsewhere
                if from_node != self.node_id:
                    block.state = CacheState.INVALID
            elif block.state == CacheState.SHARED:
                # S -> I
                block.state = CacheState.INVALID
            elif block.state == CacheState.OWNED:
                # O -> I (owner releases)
                block.state = CacheState.INVALID
            
            if block.state == CacheState.INVALID:
                del self.cache[key]
                self.invalidations += 1
                logger.debug(f"Invalidated block {key} from {from_node}")
    
    
    def _handle_commit(self, entry: LogEntry):
        """Handle committed log entry"""
        if entry.command == "CACHE_PUT":
            data = entry.data
            key = data["key"]
            
            block = CacheBlock(
                key=key,
                value=data["value"],
                state=CacheState[data["state"].upper()],
                owner_node=data["owner"],
                ttl=data["ttl"]
            )
            
            self.cache[key] = block
        
        elif entry.command == "CACHE_INVALIDATE":
            data = entry.data
            key = data["key"]
            
            if key in self.cache:
                del self.cache[key]
                self.invalidations += 1
        
        elif entry.command == "CACHE_INVALIDATE_ALL":
            data = entry.data
            key = data["key"]
            except_node = data.get("except_node")
            
            if except_node != self.node_id and key in self.cache:
                del self.cache[key]
                self.invalidations += 1
    
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        total_accesses = self.hits + self.misses
        hit_rate = (self.hits / total_accesses * 100) if total_accesses > 0 else 0
        
        return {
            "cache_size": len(self.cache),
            "max_size": self.config.cache.max_size,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": f"{hit_rate:.2f}%",
            "invalidations": self.invalidations,
            "pending_invalidations": len(self.pending_invalidations)
        }
    
    
    def clear_cache(self):
        """Clear all cache entries"""
        self.cache.clear()
        logger.info("Cache cleared")
