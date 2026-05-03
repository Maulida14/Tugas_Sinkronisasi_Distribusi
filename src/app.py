"""Main application node - integrates all distributed system components"""

import asyncio
import logging
import os
from typing import Dict, Optional
import json
from datetime import datetime
import yaml

import aiohttp
from aiohttp import web
import redis.asyncio as redis

from src.bonus.features import (
    AdaptiveLoadBalancer,
    BonusSecurityService,
    GeoDistributedRouter,
    PBFTCoordinator,
)
from src.core.config import get_config
from src.core.types import MessageType, LockType, CacheState, NodeState
from src.protocols.raft import RaftNode
from src.services.lock_manager import DistributedLockManager
from src.services.queue_service import DistributedQueue
from src.services.cache_service import MOESICache


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DistributedNode:
    """Distributed system node - integrates Raft, Lock Manager, Queue, and Cache"""
    
    def __init__(self, config=None):
        self.config = config or get_config()
        
        # Initialize components
        self.raft_node = RaftNode(
            node_id=self.config.node.node_id,
            peers=self.config.node.cluster_peers,
            config=self.config
        )
        
        self.lock_manager = DistributedLockManager(
            node_id=self.config.node.node_id,
            raft_node=self.raft_node,
            config=self.config
        )
        
        self.queue_service = DistributedQueue(
            node_id=self.config.node.node_id,
            peer_nodes=self.config.node.cluster_peers,
            raft_node=self.raft_node,
            config=self.config
        )
        
        self.cache_service = MOESICache(
            node_id=self.config.node.node_id,
            peer_nodes=self.config.node.cluster_peers,
            raft_node=self.raft_node,
            config=self.config
        )

        # Bonus feature services
        cluster_nodes = [self.config.node.node_id] + self.config.node.cluster_peers
        self.security_service = BonusSecurityService(
            node_id=self.config.node.node_id,
            enable_encryption=self.config.security.enable_encryption,
            enable_audit=self.config.security.enable_audit,
            auth_enabled=self.config.security.auth_enabled,
            api_keys=self.config.security.api_keys,
            encryption_secret=self.config.security.encryption_secret,
            audit_log_path=self.config.security.audit_log_path,
            audit_hash_path=self.config.security.audit_hash_path,
        )
        self.pbft_service = PBFTCoordinator(
            node_id=self.config.node.node_id,
            replicas=cluster_nodes,
        )
        self.geo_router = GeoDistributedRouter(node_id=self.config.node.node_id)
        self.load_balancer = AdaptiveLoadBalancer(nodes=cluster_nodes)
        
        # Redis client
        self.redis: Optional[redis.Redis] = None
        
        # Web app
        self.app = web.Application(middlewares=[self._auth_middleware])
        self._setup_routes()
    
    
    def _setup_routes(self):
        """Setup HTTP routes"""
        self.app.router.add_get('/health', self._handle_health)
        self.app.router.add_get('/readyz', self._handle_readyz)
        self.app.router.add_get('/status', self._handle_status)
        
        # Lock endpoints
        self.app.router.add_post('/lock/acquire', self._handle_lock_acquire)
        self.app.router.add_post('/lock/release', self._handle_lock_release)
        self.app.router.add_get('/lock/status/{resource}', self._handle_lock_status)
        
        # Queue endpoints
        self.app.router.add_post('/queue/publish', self._handle_queue_publish)
        self.app.router.add_post('/queue/consume', self._handle_queue_consume)
        self.app.router.add_post('/queue/ack', self._handle_queue_ack)
        
        # Cache endpoints
        self.app.router.add_get('/cache/get/{key}', self._handle_cache_get)
        self.app.router.add_post('/cache/put', self._handle_cache_put)
        self.app.router.add_post('/cache/invalidate/{key}', self._handle_cache_invalidate)
        
        # Admin endpoints
        self.app.router.add_get('/metrics', self._handle_metrics)
        self.app.router.add_get('/raft/info', self._handle_raft_info)
        self.app.router.add_post('/raft/message', self._handle_raft_message)

        # Bonus feature endpoints
        self.app.router.add_post('/security/encrypt', self._handle_security_encrypt)
        self.app.router.add_post('/security/decrypt', self._handle_security_decrypt)
        self.app.router.add_post('/security/authorize', self._handle_security_authorize)
        self.app.router.add_get('/security/audit', self._handle_security_audit)

        self.app.router.add_post('/bonus/pbft/commit', self._handle_pbft_commit)
        self.app.router.add_get('/bonus/pbft/status', self._handle_pbft_status)

        self.app.router.add_post('/bonus/geo/replicate', self._handle_geo_replicate)
        self.app.router.add_get('/bonus/geo/status', self._handle_geo_status)

        self.app.router.add_post('/bonus/load-balance/choose', self._handle_load_balance_choose)
        self.app.router.add_post('/bonus/load-balance/feedback', self._handle_load_balance_feedback)
        self.app.router.add_get('/bonus/load-balance/status', self._handle_load_balance_status)
    
    
    @web.middleware
    async def _auth_middleware(self, request: web.Request, handler):
        """Authentication middleware for security endpoints"""
        # Check if this is a protected endpoint
        protected_paths = ['/security/', '/bonus/pbft/', '/bonus/geo/', '/bonus/load-balance/']
        is_protected = any(request.path.startswith(path) for path in protected_paths)
        
        if is_protected and self.security_service.auth_enabled:
            api_key = request.headers.get('X-API-Key')
            if not api_key:
                return web.json_response({"error": "missing_api_key"}, status=401)
            try:
                auth_info = self.security_service.enforce(api_key, "read")
                request['auth'] = auth_info
            except PermissionError as e:
                return web.json_response({"error": str(e)}, status=403)
        
        return await handler(request)
    
    
    async def start(self):
        """Start the distributed node"""
        logger.info(f"Starting node {self.config.node.node_id}")
        
        # Connect to Redis
        self.redis = await redis.from_url(
            f"redis://{self.config.redis.host}:{self.config.redis.port}/{self.config.redis.db}"
        )
        logger.info(f"Connected to Redis at {self.config.redis.host}:{self.config.redis.port}")
        
        # Start Raft
        await self.raft_node.start()
        
        # Start services
        await self.lock_manager.start()
        
        # Start Raft message processing loop [FIX: Process incoming RPCs]
        asyncio.create_task(self._raft_message_processor())
        
        logger.info(f"Node {self.config.node.node_id} started successfully")
    
    
    async def stop(self):
        """Stop the distributed node"""
        logger.info(f"Stopping node {self.config.node.node_id}")
        
        await self.raft_node.stop()
        await self.lock_manager.stop()
        
        if self.redis:
            await self.redis.close()
        
        logger.info(f"Node {self.config.node.node_id} stopped")
    
    
    # Health check handlers
    
    async def _handle_health(self, request):
        """Health check endpoint"""
        return web.json_response({
            "status": "healthy",
            "node_id": self.config.node.node_id,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    
    async def _handle_readyz(self, request):
        """Readiness check - true if can serve requests"""
        ready = self.raft_node.is_leader()
        return web.json_response({
            "ready": ready,
            "node_id": self.config.node.node_id,
            "is_leader": ready
        })
    
    
    async def _handle_status(self, request):
        """Get node status"""
        return web.json_response({
            "node_id": self.config.node.node_id,
            "raft_state": self.raft_node.state.value,
            "raft_term": self.raft_node.current_term,
            "leader": self.raft_node.leader_id,
            "log_size": len(self.raft_node.log),
            "timestamp": datetime.utcnow().isoformat()
        })
    
    
    # Lock handlers
    
    async def _handle_lock_acquire(self, request):
        """Acquire distributed lock"""
        try:
            data = await request.json()
            resource = data.get("resource")
            lock_type_str = data.get("lock_type", "exclusive")
            client_id = data.get("client_id")
            timeout = data.get("timeout", 30)
            
            if not resource or not client_id:
                return web.json_response(
                    {"error": "Missing required fields"},
                    status=400
                )
            
            lock_type = LockType(lock_type_str)
            lock_id = await self.lock_manager.acquire_lock(resource, lock_type, client_id, timeout)
            
            if lock_id:
                return web.json_response({
                    "lock_id": lock_id,
                    "resource": resource,
                    "lock_type": lock_type_str,
                    "status": "acquired"
                })
            else:
                return web.json_response(
                    {"error": "Failed to acquire lock"},
                    status=500
                )
        except Exception as e:
            logger.error(f"Error acquiring lock: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500
            )
    
    
    async def _handle_lock_release(self, request):
        """Release distributed lock"""
        try:
            data = await request.json()
            lock_id = data.get("lock_id")
            
            if not lock_id:
                return web.json_response(
                    {"error": "Missing lock_id"},
                    status=400
                )
            
            success = await self.lock_manager.release_lock(lock_id)
            
            return web.json_response({
                "lock_id": lock_id,
                "status": "released" if success else "failed"
            })
        except Exception as e:
            logger.error(f"Error releasing lock: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500
            )
    
    
    async def _handle_lock_status(self, request):
        """Get lock status for resource"""
        resource = request.match_info.get("resource")
        locks = self.lock_manager.get_locks_for_resource(resource)
        
        return web.json_response({
            "resource": resource,
            "locks": [
                {
                    "lock_id": lock.lock_id,
                    "owner": lock.owner,
                    "lock_type": lock.lock_type.value,
                    "acquired_at": lock.acquired_at.isoformat()
                }
                for lock in locks
            ]
        })
    
    
    # Queue handlers
    
    async def _handle_queue_publish(self, request):
        """Publish message to queue"""
        try:
            data = await request.json()
            topic = data.get("topic")
            payload = data.get("payload")
            
            if not topic:
                return web.json_response(
                    {"error": "Missing topic"},
                    status=400
                )
            
            msg_id = await self.queue_service.publish(topic, payload)
            
            if msg_id:
                return web.json_response({
                    "msg_id": msg_id,
                    "topic": topic,
                    "status": "published"
                })
            else:
                return web.json_response(
                    {"error": "Failed to publish message"},
                    status=500
                )
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500
            )
    
    
    async def _handle_queue_consume(self, request):
        """Consume messages from queue"""
        try:
            data = await request.json()
            topic = data.get("topic")
            consumer_id = data.get("consumer_id")
            batch_size = data.get("batch_size", 1)
            
            if not topic or not consumer_id:
                return web.json_response(
                    {"error": "Missing required fields"},
                    status=400
                )
            
            messages = await self.queue_service.consume(topic, consumer_id, batch_size)
            
            return web.json_response({
                "topic": topic,
                "consumer_id": consumer_id,
                "messages": [
                    {
                        "msg_id": msg.msg_id,
                        "payload": msg.payload,
                        "delivery_count": msg.delivery_count
                    }
                    for msg in messages
                ]
            })
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500
            )
    
    
    async def _handle_queue_ack(self, request):
        """Acknowledge message consumption"""
        try:
            data = await request.json()
            msg_id = data.get("msg_id")
            consumer_id = data.get("consumer_id")
            
            if not msg_id or not consumer_id:
                return web.json_response(
                    {"error": "Missing required fields"},
                    status=400
                )
            
            success = await self.queue_service.acknowledge(msg_id, consumer_id)
            
            return web.json_response({
                "msg_id": msg_id,
                "status": "acked" if success else "failed"
            })
        except Exception as e:
            logger.error(f"Error acknowledging message: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500
            )
    
    
    # Cache handlers
    
    async def _handle_cache_get(self, request):
        """Get value from cache"""
        key = request.match_info.get("key")
        value = await self.cache_service.get(key)
        
        if value is None:
            return web.json_response(
                {"error": "Key not found"},
                status=404
            )
        
        return web.json_response({
            "key": key,
            "value": value
        })
    
    
    async def _handle_cache_put(self, request):
        """Put value in cache"""
        try:
            data = await request.json()
            key = data.get("key")
            value = data.get("value")
            ttl = data.get("ttl")
            
            if not key:
                return web.json_response(
                    {"error": "Missing key"},
                    status=400
                )
            
            success = await self.cache_service.put(key, value, ttl)
            
            return web.json_response({
                "key": key,
                "status": "cached" if success else "failed"
            })
        except Exception as e:
            logger.error(f"Error putting in cache: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500
            )
    
    
    async def _handle_cache_invalidate(self, request):
        """Invalidate cache entry"""
        key = request.match_info.get("key")
        await self.cache_service.handle_invalidation(key, self.config.node.node_id)
        
        return web.json_response({
            "key": key,
            "status": "invalidated"
        })
    
    
    # Metrics and info
    
    async def _handle_metrics(self, request):
        """Get system metrics"""
        return web.json_response({
            "timestamp": datetime.utcnow().isoformat(),
            "node_id": self.config.node.node_id,
            "raft": {
                "term": self.raft_node.current_term,
                "state": self.raft_node.state.value,
                "log_size": len(self.raft_node.log)
            },
            "locks": self.lock_manager.get_stats(),
            "queue": self.queue_service.get_stats(),
            "cache": self.cache_service.get_stats()
        })
    
    
    async def _handle_raft_info(self, request):
        """Get Raft consensus info"""
        return web.json_response({
            "node_id": self.config.node.node_id,
            "current_term": self.raft_node.current_term,
            "state": self.raft_node.state.value,
            "leader_id": self.raft_node.leader_id,
            "log_size": len(self.raft_node.log),
            "commit_index": self.raft_node.commit_index,
            "last_applied": self.raft_node.last_applied,
            "voted_for": self.raft_node.voted_for,
            "peers": self.raft_node.peers
        })

    async def _raft_message_processor(self):
        """Background task to process Raft state changes and trigger RPCs [RAFT FIX]"""
        while True:
            try:
                if self.raft_node.state == NodeState.CANDIDATE and not self.raft_node._election_in_progress:
                    logger.info(f"[{self.config.node.node_id}] Triggering election")
                    await self.raft_node._start_election()
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Raft processor error: {e}")
                await asyncio.sleep(1)

    async def _handle_raft_message(self, request):
        """Handle incoming Raft RPC message from a peer"""
        try:
            data = await request.json()
            message = self.raft_node.message_from_dict(data)
            response = await self.raft_node.handle_message(message)
            return web.json_response(self.raft_node.message_to_dict(response))
        except Exception as e:
            logger.error(f"Error handling Raft message: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500
            )



    # Bonus feature handlers

    async def _handle_security_encrypt(self, request):
        try:
            data = await request.json()
            payload = data.get("payload")
            key_material = data.get("key_material")
            
            if payload is None:
                return web.json_response({"error": "Missing payload"}, status=400)
            
            ciphertext = self.security_service.encrypt(payload, key_material)
            self.security_service.record_audit("encrypt", self.config.node.node_id, "payload", "success")
            return web.json_response({"ciphertext": ciphertext, "enabled": self.config.security.enable_encryption})
        except Exception as e:
            logger.error(f"Encrypt error: {e}")
            self.security_service.record_audit("encrypt", self.config.node.node_id, "payload", "failed")
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_security_decrypt(self, request):
        try:
            data = await request.json()
            logger.info(f"Decrypt request data: {data}")
            
            ciphertext = data.get("ciphertext")
            key_material = data.get("key_material")
            
            if ciphertext is None:
                logger.error(f"Missing ciphertext. Received data: {data}")
                return web.json_response({"error": "Missing ciphertext"}, status=400)
            
            plaintext = self.security_service.decrypt(ciphertext, key_material)
            self.security_service.record_audit("decrypt", self.config.node.node_id, "payload", "success")
            return web.json_response({"plaintext": plaintext, "enabled": self.config.security.enable_encryption})
        except Exception as e:
            logger.error(f"Decrypt error: {e}", exc_info=True)
            self.security_service.record_audit("decrypt", self.config.node.node_id, "payload", "failed")
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_security_authorize(self, request):
        data = await request.json()
        role = data.get("role", "viewer")
        action = data.get("action", "read")
        allowed = self.security_service.authorize(role, action)
        self.security_service.record_audit("authorize", role, action, "allowed" if allowed else "denied")
        return web.json_response({"role": role, "action": action, "allowed": allowed})

    async def _handle_security_audit(self, request):
        return web.json_response({
            "count": len(self.security_service.audit_log),
            "records": self.security_service.get_audit_log(),
        })

    async def _handle_pbft_commit(self, request):
        data = await request.json()
        payload = data.get("payload", {})
        faulty_nodes = int(data.get("faulty_nodes", 0))
        result = self.pbft_service.submit_request(payload, faulty_nodes=faulty_nodes)
        return web.json_response({
            "request_id": result.request_id,
            "status": result.status,
            "quorum": result.quorum,
            "replicas": result.replicas,
            "prepare_votes": result.prepare_votes,
            "commit_votes": result.commit_votes,
            "committed_value": result.committed_value,
        })

    async def _handle_pbft_status(self, request):
        return web.json_response(self.pbft_service.get_stats())

    async def _handle_geo_replicate(self, request):
        data = await request.json()
        key = data.get("key")
        value = data.get("value")
        preferred_region = data.get("preferred_region")

        if not key:
            return web.json_response({"error": "Missing key"}, status=400)

        record = self.geo_router.replicate(key, value, preferred_region=preferred_region)
        return web.json_response({"status": "replicated", "record": record})

    async def _handle_geo_status(self, request):
        return web.json_response(self.geo_router.get_stats())

    async def _handle_load_balance_choose(self, request):
        data = await request.json()
        workload = float(data.get("workload", 1.0))
        exclude = data.get("exclude", [])
        node = self.load_balancer.choose_node(workload=workload, exclude=exclude)
        return web.json_response({"selected_node": node, "workload": workload})

    async def _handle_load_balance_feedback(self, request):
        data = await request.json()
        node = data.get("node")
        latency_ms = float(data.get("latency_ms", 0.0))
        success = bool(data.get("success", True))
        if not node:
            return web.json_response({"error": "Missing node"}, status=400)
        state = self.load_balancer.feedback(node, latency_ms, success=success)
        return web.json_response({"node": node, "state": state})

    async def _handle_load_balance_status(self, request):
        return web.json_response(self.load_balancer.get_stats())


async def create_and_run_node(node_config=None):
    """Factory function to create and run node"""
    node = DistributedNode(node_config)
    
    await node.start()
    
    runner = web.AppRunner(node.app)
    await runner.setup()
    
    site = web.TCPSite(runner, "0.0.0.0", node.config.node.node_port)
    await site.start()
    
    logger.info(f"Node {node.config.node.node_id} listening on 0.0.0.0:{node.config.node.node_port}")
    
    try:
        await asyncio.Event().wait()  # Run forever
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await node.stop()
        await runner.cleanup()


async def main():
    """Main entry point"""
    config = get_config()
    
    # Load environment
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    if os.path.exists(dotenv_path):
        from dotenv import load_dotenv
        load_dotenv(dotenv_path)
    
    await create_and_run_node(config)


if __name__ == "__main__":
    asyncio.run(main())
