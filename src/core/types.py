"""Type definitions and enums for distributed system"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum
from datetime import datetime


class MessageType(str, Enum):
    """Consensus message types"""
    REQUEST_VOTE = "request_vote"
    VOTE_RESPONSE = "vote_response"
    APPEND_ENTRIES = "append_entries"
    APPEND_RESPONSE = "append_response"
    LOCK_REQUEST = "lock_request"
    LOCK_GRANT = "lock_grant"
    QUEUE_PUBLISH = "queue_publish"
    CACHE_UPDATE = "cache_update"
    CACHE_INVALIDATE = "cache_invalidate"
    HEARTBEAT = "heartbeat"


class LockType(str, Enum):
    """Lock types"""
    SHARED = "shared"
    EXCLUSIVE = "exclusive"


class CacheState(str, Enum):
    """MOESI cache coherence states"""
    MODIFIED = "modified"
    OWNED = "owned"
    EXCLUSIVE = "exclusive"
    SHARED = "shared"
    INVALID = "invalid"


class NodeState(str, Enum):
    """Raft node states"""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """Consensus log entry"""
    term: int
    index: int
    command: str
    data: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Message:
    """Inter-node message"""
    message_type: MessageType
    sender_id: str
    term: int
    data: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Lock:
    """Distributed lock"""
    lock_id: str
    resource: str
    owner: str
    lock_type: LockType
    acquired_at: datetime
    ttl: float  # seconds


@dataclass
class QueueMessage:
    """Message in distributed queue"""
    msg_id: str
    payload: Any
    partition_id: int
    timestamp: datetime = field(default_factory=datetime.utcnow)
    acked: bool = False
    delivery_count: int = 0


@dataclass
class CacheBlock:
    """Cache block with MOESI state"""
    key: str
    value: Any
    state: CacheState
    owner_node: str
    last_modified: datetime = field(default_factory=datetime.utcnow)
    ttl: float = 3600  # seconds
    sharers: List[str] = field(default_factory=list)


@dataclass
class MetricsData:
    """System metrics"""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    locks_active: int = 0
    locks_waiting: int = 0
    queue_size: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    consensus_term: int = 0
    consensus_leader: Optional[str] = None
