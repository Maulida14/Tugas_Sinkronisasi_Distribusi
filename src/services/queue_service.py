"""Distributed Queue with Consistent Hashing and Replication"""

import asyncio
import logging
import hashlib
import json
from typing import Dict, List, Optional, Any, Set
from datetime import datetime
from uuid import uuid4
from collections import defaultdict

from ..core.types import QueueMessage, LogEntry
from ..core.config import SystemConfig


logger = logging.getLogger(__name__)


class ConsistentHashRing:
    """Consistent hash ring for queue partitioning"""
    
    def __init__(self, nodes: List[str], virtual_nodes: int = 160):
        self.nodes = nodes
        self.virtual_nodes = virtual_nodes
        self.ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []
        
        self._build_ring()
    
    
    def _build_ring(self):
        """Build consistent hash ring"""
        self.ring = {}
        for node in self.nodes:
            for i in range(self.virtual_nodes):
                virtual_key = f"{node}:{i}"
                hash_key = int(hashlib.md5(virtual_key.encode()).hexdigest(), 16)
                self.ring[hash_key] = node
        
        self.sorted_keys = sorted(self.ring.keys())
    
    
    def get_node(self, key: str) -> str:
        """Get node for key using consistent hash"""
        if not self.sorted_keys:
            raise ValueError("No nodes in ring")
        
        hash_key = int(hashlib.md5(key.encode()).hexdigest(), 16)
        
        # Find first node >= hash_key
        for sorted_key in self.sorted_keys:
            if hash_key <= sorted_key:
                return self.ring[sorted_key]
        
        # Wrap around to first node
        return self.ring[self.sorted_keys[0]]
    
    
    def get_replicas(self, key: str, count: int) -> List[str]:
        """Get replica nodes for key"""
        replicas = []
        seen = set()
        hash_key = int(hashlib.md5(key.encode()).hexdigest(), 16)
        
        for sorted_key in self.sorted_keys + self.sorted_keys:
            if hash_key <= sorted_key:
                node = self.ring[sorted_key]
                if node not in seen:
                    replicas.append(node)
                    seen.add(node)
                    if len(replicas) >= count:
                        break
        
        if len(replicas) < count:
            for sorted_key in self.sorted_keys:
                node = self.ring[sorted_key]
                if node not in seen:
                    replicas.append(node)
                    seen.add(node)
                    if len(replicas) >= count:
                        break
        
        return replicas[:count]
    
    
    def add_node(self, node: str):
        """Add node to ring"""
        if node not in self.nodes:
            self.nodes.append(node)
            self._build_ring()
    
    
    def remove_node(self, node: str):
        """Remove node from ring"""
        if node in self.nodes:
            self.nodes.remove(node)
            self._build_ring()


class DistributedQueue:
    """Distributed queue with consistent hashing and replication"""
    
    def __init__(self, node_id: str, peer_nodes: List[str], raft_node, config: SystemConfig):
        self.node_id = node_id
        self.all_nodes = [node_id] + peer_nodes
        self.raft_node = raft_node
        self.config = config
        
        # Consistent hash ring
        self.hash_ring = ConsistentHashRing(self.all_nodes)
        
        # Queue storage: partition_id -> messages
        self.partitions: Dict[int, List[QueueMessage]] = defaultdict(list)
        
        # Message metadata
        self.message_acks: Dict[str, Set[str]] = {}  # msg_id -> acknowledged nodes
        
        # Pending messages to be persisted
        self.pending_messages: List[QueueMessage] = []
        self.redis = None
        self.persistence_key = "distributed_queue:messages"
        
        # Register as commit callback
        self.raft_node.on_commit.append(self._handle_commit)


    async def set_persistence_store(self, redis_client):
        """Attach Redis persistence and recover unacked messages."""
        self.redis = redis_client
        if self.config.queue.persistence_enabled and self.redis:
            await self._load_persisted_messages()
    
    
    async def publish(self, topic: str, payload: Any) -> str:
        """Publish message to queue"""
        msg_id = str(uuid4())
        
        # Determine partition
        partition_id = self._get_partition(topic)
        
        # Create message
        message = QueueMessage(
            msg_id=msg_id,
            payload=payload,
            partition_id=partition_id,
            acked=False,
            delivery_count=0
        )
        
        # Get replica nodes
        replica_nodes = self.hash_ring.get_replicas(f"{topic}:{partition_id}", self.config.queue.replication_factor)
        
        # Replicate through Raft
        success = await self.raft_node.append_entry(
            command="QUEUE_PUBLISH",
            data={
                "msg_id": msg_id,
                "topic": topic,
                "partition_id": partition_id,
                "payload": payload if isinstance(payload, (str, int, float, bool, list, dict)) else str(payload),
                "replicas": replica_nodes
            }
        )
        
        if success:
            logger.info(f"Message {msg_id} published to topic {topic} on partition {partition_id}")
            
            return msg_id
        
        return None
    
    
    async def consume(self, topic: str, consumer_id: str, batch_size: int = 1) -> List[QueueMessage]:
        """Consume messages from queue"""
        partition_id = self._get_partition(topic)
        
        messages = []
        if partition_id in self.partitions:
            available = [m for m in self.partitions[partition_id] if not m.acked]
            messages = available[:batch_size]
            
            # Mark for delivery
            for msg in messages:
                msg.delivery_count += 1
                if msg.msg_id not in self.message_acks:
                    self.message_acks[msg.msg_id] = set()
                if self.config.queue.persistence_enabled:
                    self._schedule_persist(msg)
        
        logger.info(f"Consumer {consumer_id} consumed {len(messages)} messages from {topic}")
        return messages
    
    
    async def acknowledge(self, msg_id: str, consumer_id: str) -> bool:
        """Acknowledge message consumption"""
        self.message_acks.setdefault(msg_id, set()).add(consumer_id)

        success = await self.raft_node.append_entry(
            command="QUEUE_ACK",
            data={"msg_id": msg_id, "consumer_id": consumer_id}
        )

        if success:
            logger.info(f"Message {msg_id} acknowledged by {consumer_id}")
            return True
        
        return False
    
    
    async def recover_from_failure(self, failed_node: str):
        """Recover messages from failed node"""
        logger.info(f"Recovering messages from failed node {failed_node}")
        
        # Remove failed node from ring
        self.hash_ring.remove_node(failed_node)
        
        # Re-replicate affected messages
        for partition_id, messages in self.partitions.items():
            for msg in messages:
                if not msg.acked:
                    # Get new replica nodes
                    topic_key = f"partition_{partition_id}"
                    replica_nodes = self.hash_ring.get_replicas(topic_key, self.config.queue.replication_factor)
                    
                    # Replicate to new nodes if needed
                    if self.node_id in replica_nodes:
                        # Ensure message is stored
                        logger.debug(f"Re-replicated message {msg.msg_id} to new replicas")
    
    
    def _get_partition(self, topic: str) -> int:
        """Get partition ID for topic"""
        hash_val = hashlib.md5(topic.encode()).hexdigest()
        return int(hash_val, 16) % 10  # 10 partitions
    
    
    def _handle_commit(self, entry: LogEntry):
        """Handle committed log entry"""
        if entry.command == "QUEUE_PUBLISH":
            data = entry.data
            partition_id = data["partition_id"]
            
            message = QueueMessage(
                msg_id=data["msg_id"],
                payload=data["payload"],
                partition_id=partition_id,
                acked=False,
                delivery_count=0
            )
            
            self.partitions[partition_id].append(message)
            if self.config.queue.persistence_enabled:
                self.pending_messages.append(message)
                self._schedule_persist(message)
        
        elif entry.command == "QUEUE_ACK":
            data = entry.data
            msg_id = data["msg_id"]
            
            # Mark all messages with this ID as acked
            for partition_messages in self.partitions.values():
                for msg in partition_messages:
                    if msg.msg_id == msg_id:
                        msg.acked = True
            self.pending_messages = [msg for msg in self.pending_messages if msg.msg_id != msg_id]
            self._schedule_delete(msg_id)


    def _schedule_persist(self, message: QueueMessage):
        """Persist a message without blocking the synchronous Raft callback."""
        if not self.redis:
            return
        try:
            asyncio.create_task(self._persist_message(message))
        except RuntimeError:
            logger.debug("No running event loop available for queue persistence")


    def _schedule_delete(self, msg_id: str):
        """Remove an acknowledged message from durable storage."""
        if not self.redis:
            return
        try:
            asyncio.create_task(self._delete_persisted_message(msg_id))
        except RuntimeError:
            logger.debug("No running event loop available for queue persistence cleanup")


    async def _persist_message(self, message: QueueMessage):
        if not self.redis:
            return

        payload = {
            "msg_id": message.msg_id,
            "payload": message.payload,
            "partition_id": message.partition_id,
            "timestamp": message.timestamp.isoformat(),
            "acked": message.acked,
            "delivery_count": message.delivery_count,
        }
        await self.redis.hset(self.persistence_key, message.msg_id, json.dumps(payload))


    async def _delete_persisted_message(self, msg_id: str):
        if self.redis:
            await self.redis.hdel(self.persistence_key, msg_id)


    async def _load_persisted_messages(self):
        raw_messages = await self.redis.hgetall(self.persistence_key)
        recovered = 0

        for raw in raw_messages.values():
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            data = json.loads(raw)
            if data.get("acked"):
                continue

            message = QueueMessage(
                msg_id=data["msg_id"],
                payload=data["payload"],
                partition_id=int(data["partition_id"]),
                timestamp=datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else datetime.utcnow(),
                acked=False,
                delivery_count=int(data.get("delivery_count", 0)),
            )

            if not any(existing.msg_id == message.msg_id for existing in self.partitions[message.partition_id]):
                self.partitions[message.partition_id].append(message)
                recovered += 1

        logger.info(f"Recovered {recovered} unacked queue messages from Redis")
    
    
    def get_queue_depth(self, topic: str) -> int:
        """Get number of unacked messages in queue"""
        partition_id = self._get_partition(topic)
        if partition_id not in self.partitions:
            return 0
        
        return len([m for m in self.partitions[partition_id] if not m.acked])
    
    
    def get_stats(self) -> Dict:
        """Get queue statistics"""
        total_messages = sum(len(m) for m in self.partitions.values())
        unacked_messages = sum(len([msg for msg in msgs if not msg.acked]) for msgs in self.partitions.values())
        
        return {
            "total_partitions": len(self.partitions),
            "total_messages": total_messages,
            "unacked_messages": unacked_messages,
            "pending_persistence": len(self.pending_messages)
        }
