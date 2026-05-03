"""Distributed Lock Manager with Raft consensus and deadlock detection"""

import asyncio
import logging
from typing import Dict, Optional, List, Set
from datetime import datetime, timedelta
from uuid import uuid4
import hashlib

from ..core.types import Lock, LockType, LogEntry, NodeState
from ..core.config import SystemConfig


logger = logging.getLogger(__name__)


class LockWaitGraph:
    """Wait-for graph for deadlock detection"""
    
    def __init__(self):
        self.graph: Dict[str, Set[str]] = {}
        self.lock = asyncio.Lock()
    
    
    async def add_edge(self, waiter: str, holder: str):
        """Add edge: waiter waits for holder"""
        async with self.lock:
            if waiter not in self.graph:
                self.graph[waiter] = set()
            self.graph[waiter].add(holder)
    
    
    async def remove_edges(self, waiter: str):
        """Remove all edges from waiter"""
        async with self.lock:
            if waiter in self.graph:
                del self.graph[waiter]
            for holders in self.graph.values():
                holders.discard(waiter)
    
    
    async def detect_cycle(self) -> Optional[List[str]]:
        """Detect cycle in wait-for graph using DFS"""
        async with self.lock:
            visited: Set[str] = set()
            rec_stack: Set[str] = set()
            parent: Dict[str, str] = {}
            
            def dfs(node: str, path: List[str]) -> Optional[List[str]]:
                visited.add(node)
                rec_stack.add(node)
                path.append(node)
                
                if node in self.graph:
                    for neighbor in self.graph[node]:
                        if neighbor not in visited:
                            result = dfs(neighbor, path.copy())
                            if result:
                                return result
                        elif neighbor in rec_stack:
                            return path + [neighbor]
                
                rec_stack.remove(node)
                return None
            
            for node in self.graph:
                if node not in visited:
                    cycle = dfs(node, [])
                    if cycle:
                        return cycle
            
            return None


class DistributedLockManager:
    """Distributed lock manager with Raft consensus and deadlock detection"""
    
    def __init__(self, node_id: str, raft_node, config: SystemConfig):
        self.node_id = node_id
        self.raft_node = raft_node
        self.config = config
        
        # Lock storage
        self.locks: Dict[str, Lock] = {}
        self.lock_queue: Dict[str, List[tuple]] = {}  # resource -> [(client_id, lock_type)]
        
        # Wait-for graph for deadlock detection
        self.wait_graph = LockWaitGraph()
        
        # Deadlock detection task
        self.deadlock_detector: Optional[asyncio.Task] = None
        
        # Callbacks
        self.lock_acquired_callbacks: List = []
        self.deadlock_detected_callbacks: List = []
        
        # Register as commit callback
        self.raft_node.on_commit.append(self._handle_commit)
    
    
    async def start(self):
        """Start lock manager"""
        self.deadlock_detector = asyncio.create_task(self._deadlock_detection_loop())
    
    
    async def stop(self):
        """Stop lock manager"""
        if self.deadlock_detector:
            self.deadlock_detector.cancel()
    
    
    async def acquire_lock(self, resource: str, lock_type: LockType, client_id: str, timeout: float = 30) -> Optional[str]:
        """Acquire lock on resource"""
        lock_id = str(uuid4())
        
        try:
            # Try to acquire
            can_acquire = await self._try_acquire(resource, lock_type)
            
            if can_acquire:
                # Create lock entry
                lock = Lock(
                    lock_id=lock_id,
                    resource=resource,
                    owner=client_id,
                    lock_type=lock_type,
                    acquired_at=datetime.utcnow(),
                    ttl=timeout
                )
                
                # Replicate through Raft
                success = await self.raft_node.append_entry(
                    command="LOCK_ACQUIRE",
                    data={
                        "lock_id": lock_id,
                        "resource": resource,
                        "owner": client_id,
                        "lock_type": lock_type.value,
                        "ttl": timeout
                    }
                )
                
                if success:
                    logger.info(f"Lock {lock_id} acquired for {client_id} on {resource}")
                    return lock_id
            else:
                # Add to wait queue
                if resource not in self.lock_queue:
                    self.lock_queue[resource] = []
                
                self.lock_queue[resource].append((client_id, lock_type, lock_id))
                
                # Add to wait graph
                current_holder = await self._get_lock_holder(resource)
                if current_holder:
                    await self.wait_graph.add_edge(client_id, current_holder)
                
                # Wait for lock
                start_time = datetime.utcnow()
                while True:
                    can_acquire = await self._try_acquire(resource, lock_type)
                    if can_acquire:
                        lock = Lock(
                            lock_id=lock_id,
                            resource=resource,
                            owner=client_id,
                            lock_type=lock_type,
                            acquired_at=datetime.utcnow(),
                            ttl=timeout
                        )
                        
                        success = await self.raft_node.append_entry(
                            command="LOCK_ACQUIRE",
                            data={
                                "lock_id": lock_id,
                                "resource": resource,
                                "owner": client_id,
                                "lock_type": lock_type.value,
                                "ttl": timeout
                            }
                        )
                        
                        if success:
                            await self.wait_graph.remove_edges(client_id)
                            logger.info(f"Lock {lock_id} acquired for {client_id} after waiting")
                            return lock_id
                    
                    elapsed = (datetime.utcnow() - start_time).total_seconds()
                    if elapsed > timeout:
                        logger.warning(f"Lock acquisition timeout for {client_id} on {resource}")
                        return None
                    
                    await asyncio.sleep(0.1)
        
        except Exception as e:
            logger.error(f"Error acquiring lock: {e}")
            return None
    
    
    async def release_lock(self, lock_id: str) -> bool:
        """Release lock"""
        if lock_id not in self.locks:
            logger.warning(f"Lock {lock_id} not found")
            return False
        
        lock = self.locks[lock_id]
        
        success = await self.raft_node.append_entry(
            command="LOCK_RELEASE",
            data={"lock_id": lock_id}
        )
        
        if success:
            logger.info(f"Lock {lock_id} released by {lock.owner}")
            return True
        
        return False
    
    
    async def _try_acquire(self, resource: str, lock_type: LockType) -> bool:
        """Try to acquire lock without waiting"""
        # Get current holders
        current_locks = [l for l in self.locks.values() if l.resource == resource]
        
        if not current_locks:
            return True
        
        # If trying to get exclusive, need no other locks
        if lock_type == LockType.EXCLUSIVE:
            return False
        
        # If trying to get shared, need no exclusive locks
        if lock_type == LockType.SHARED:
            for lock in current_locks:
                if lock.lock_type == LockType.EXCLUSIVE:
                    return False
        
        return True
    
    
    async def _get_lock_holder(self, resource: str) -> Optional[str]:
        """Get current lock holder for resource"""
        for lock in self.locks.values():
            if lock.resource == resource and lock.lock_type == LockType.EXCLUSIVE:
                return lock.owner
        return None
    
    
    async def _deadlock_detection_loop(self):
        """Periodic deadlock detection"""
        try:
            while True:
                await asyncio.sleep(self.config.lock.deadlock_detection_interval)
                
                cycle = await self.wait_graph.detect_cycle()
                if cycle:
                    logger.warning(f"Deadlock detected: {cycle}")
                    
                    # Notify callbacks
                    for callback in self.deadlock_detected_callbacks:
                        callback(cycle)
                    
                    # Break deadlock by releasing lock from first in cycle
                    if cycle:
                        victim = cycle[0]
                        # Find and release victim's lock
                        for lock_id, lock in list(self.locks.items()):
                            if lock.owner == victim:
                                await self.release_lock(lock_id)
                                break
        
        except asyncio.CancelledError:
            pass
    
    
    def _handle_commit(self, entry: LogEntry):
        """Handle committed log entry"""
        if entry.command == "LOCK_ACQUIRE":
            data = entry.data
            lock = Lock(
                lock_id=data["lock_id"],
                resource=data["resource"],
                owner=data["owner"],
                lock_type=LockType(data["lock_type"]),
                acquired_at=datetime.utcnow(),
                ttl=data["ttl"]
            )
            self.locks[lock.lock_id] = lock
            
            # Remove from queue
            if data["resource"] in self.lock_queue:
                self.lock_queue[data["resource"]] = [
                    (c, t, l) for c, t, l in self.lock_queue[data["resource"]]
                    if l != lock.lock_id
                ]
            
            for callback in self.lock_acquired_callbacks:
                callback(lock)
        
        elif entry.command == "LOCK_RELEASE":
            data = entry.data
            lock_id = data["lock_id"]
            if lock_id in self.locks:
                del self.locks[lock_id]
    
    
    def get_locks_for_resource(self, resource: str) -> List[Lock]:
        """Get all locks for resource"""
        return [l for l in self.locks.values() if l.resource == resource]
    
    
    def get_stats(self) -> Dict:
        """Get lock manager statistics"""
        return {
            "total_locks": len(self.locks),
            "total_waiting": sum(len(q) for q in self.lock_queue.values()),
            "resources": len(self.lock_queue),
            "deadlock_cycles": 0  # Would need async call to detect
        }
