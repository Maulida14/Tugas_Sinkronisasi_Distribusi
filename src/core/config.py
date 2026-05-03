"""Configuration management for distributed system"""

import os
from dataclasses import dataclass
from typing import List


@dataclass
class ConsensusConfig:
    """Raft consensus configuration"""
    heartbeat_interval: float = float(os.getenv("HEARTBEAT_INTERVAL", "0.5"))
    election_timeout_min: float = float(os.getenv("ELECTION_TIMEOUT_MIN", "1.5"))
    election_timeout_max: float = float(os.getenv("ELECTION_TIMEOUT_MAX", "3.0"))
    request_timeout: float = 5.0


@dataclass
class LockConfig:
    """Distributed lock configuration"""
    deadlock_detection_interval: float = float(os.getenv("DEADLOCK_DETECTION_INTERVAL", "5"))
    max_lock_wait_time: float = float(os.getenv("MAX_LOCK_WAIT_TIME", "30"))


@dataclass
class CacheConfig:
    """Distributed cache configuration"""
    max_size: int = int(os.getenv("CACHE_MAX_SIZE", "1000"))
    ttl: int = int(os.getenv("CACHE_TTL", "3600"))
    eviction_policy: str = os.getenv("CACHE_EVICTION_POLICY", "LRU")


@dataclass
class QueueConfig:
    """Distributed queue configuration"""
    replication_factor: int = int(os.getenv("QUEUE_REPLICATION_FACTOR", "3"))
    persistence_enabled: bool = os.getenv("QUEUE_PERSISTENCE_ENABLED", "true").lower() == "true"


@dataclass
class SecurityConfig:
    """Security configuration"""
    enable_encryption: bool = os.getenv("ENABLE_ENCRYPTION", "true").lower() == "true"
    enable_audit: bool = os.getenv("ENABLE_AUDIT", "true").lower() == "true"
    auth_enabled: bool = os.getenv("AUTH_ENABLED", "true").lower() == "true"
    api_keys: str = os.getenv(
        "API_KEYS",
        "admin-key:admin,operator-key:operator,viewer-key:viewer"
    )
    audit_log_path: str = os.getenv("AUDIT_LOG_PATH", "data/audit.log")
    audit_hash_path: str = os.getenv("AUDIT_HASH_PATH", "data/audit.hash")
    encryption_secret: str = os.getenv("ENCRYPTION_SECRET", "distributed-sync-secret")


@dataclass
class BonusConfig:
    """Bonus feature configuration"""
    geo_region: str = os.getenv("GEO_REGION", "asia-southeast")
    geo_region_map: str = os.getenv(
        "GEO_REGION_MAP",
        "asia-southeast:22,us-east:70,eu-west:95"
    )
    pbft_fault_tolerance_override: int = int(os.getenv("PBFT_FAULT_TOLERANCE_OVERRIDE", "0"))


@dataclass
class NodeConfig:
    """Node configuration"""
    node_id: str = os.getenv("NODE_ID", "node_1")
    node_port: int = int(os.getenv("NODE_PORT", "8001"))
    cluster_peers: List[str] = None
    
    def __post_init__(self):
        if self.cluster_peers is None:
            peers_str = os.getenv("CLUSTER_PEERS", "localhost:8002,localhost:8003,localhost:8004")
            self.cluster_peers = [p.strip() for p in peers_str.split(",")]


@dataclass
class RedisConfig:
    """Redis configuration"""
    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    db: int = int(os.getenv("REDIS_DB", "0"))


class SystemConfig:
    """Main system configuration"""
    
    def __init__(self):
        self.node = NodeConfig()
        self.consensus = ConsensusConfig()
        self.lock = LockConfig()
        self.cache = CacheConfig()
        self.queue = QueueConfig()
        self.security = SecurityConfig()
        self.bonus = BonusConfig()
        self.redis = RedisConfig()
        self.log_level = os.getenv("LOG_LEVEL", "INFO")


def get_config() -> SystemConfig:
    """Get global system configuration"""
    return SystemConfig()
