"""Test client untuk manual testing API"""

import asyncio
import aiohttp
import json
from typing import Dict, Any


class DistributedSystemClient:
    """Client untuk testing Distributed System API"""
    
    def __init__(self, base_url: str = "http://localhost:8001"):
        self.base_url = base_url
        self.session: aiohttp.ClientSession = None
    
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    
    async def __aexit__(self, *args):
        await self.session.close()
    
    
    async def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request"""
        url = f"{self.base_url}{endpoint}"
        async with self.session.request(method, url, **kwargs) as resp:
            return await resp.json()
    
    
    async def health_check(self) -> Dict:
        """Health check"""
        return await self._request("GET", "/health")
    
    
    async def readiness_check(self) -> Dict:
        """Readiness check"""
        return await self._request("GET", "/readyz")
    
    
    async def acquire_lock(self, resource: str, client_id: str, lock_type: str = "exclusive") -> Dict:
        """Acquire lock"""
        return await self._request("POST", "/lock/acquire", json={
            "resource": resource,
            "lock_type": lock_type,
            "client_id": client_id,
            "timeout": 30
        })
    
    
    async def release_lock(self, lock_id: str) -> Dict:
        """Release lock"""
        return await self._request("POST", "/lock/release", json={
            "lock_id": lock_id
        })
    
    
    async def get_lock_status(self, resource: str) -> Dict:
        """Get lock status"""
        return await self._request("GET", f"/lock/status/{resource}")
    
    
    async def publish_message(self, topic: str, payload: Dict) -> Dict:
        """Publish message"""
        return await self._request("POST", "/queue/publish", json={
            "topic": topic,
            "payload": payload
        })
    
    
    async def consume_messages(self, topic: str, consumer_id: str, batch_size: int = 1) -> Dict:
        """Consume messages"""
        return await self._request("POST", "/queue/consume", json={
            "topic": topic,
            "consumer_id": consumer_id,
            "batch_size": batch_size
        })
    
    
    async def ack_message(self, msg_id: str, consumer_id: str) -> Dict:
        """Acknowledge message"""
        return await self._request("POST", "/queue/ack", json={
            "msg_id": msg_id,
            "consumer_id": consumer_id
        })
    
    
    async def cache_put(self, key: str, value: Any, ttl: int = 3600) -> Dict:
        """Put value in cache"""
        return await self._request("POST", "/cache/put", json={
            "key": key,
            "value": value,
            "ttl": ttl
        })
    
    
    async def cache_get(self, key: str) -> Dict:
        """Get value from cache"""
        return await self._request("GET", f"/cache/get/{key}")
    
    
    async def get_metrics(self) -> Dict:
        """Get metrics"""
        return await self._request("GET", "/metrics")
    
    
    async def get_raft_info(self) -> Dict:
        """Get Raft info"""
        return await self._request("GET", "/raft/info")


async def demo():
    """Demo test run"""
    async with DistributedSystemClient() as client:
        # Health check
        print("1. Health Check")
        health = await client.health_check()
        print(json.dumps(health, indent=2))
        
        # Acquire lock
        print("\n2. Acquire Lock")
        lock_resp = await client.acquire_lock("resource1", "client1", "exclusive")
        print(json.dumps(lock_resp, indent=2))
        lock_id = lock_resp.get("lock_id")
        
        # Get lock status
        if lock_id:
            print("\n3. Get Lock Status")
            status = await client.get_lock_status("resource1")
            print(json.dumps(status, indent=2))
        
        # Publish message
        print("\n4. Publish Message")
        msg_resp = await client.publish_message("topic1", {"data": "test"})
        print(json.dumps(msg_resp, indent=2))
        
        # Cache operations
        print("\n5. Cache Put")
        cache_put = await client.cache_put("key1", "value1")
        print(json.dumps(cache_put, indent=2))
        
        print("\n6. Cache Get")
        cache_get = await client.cache_get("key1")
        print(json.dumps(cache_get, indent=2))
        
        # Get metrics
        print("\n7. System Metrics")
        metrics = await client.get_metrics()
        print(json.dumps(metrics, indent=2))
        
        # Release lock
        if lock_id:
            print("\n8. Release Lock")
            release = await client.release_lock(lock_id)
            print(json.dumps(release, indent=2))


if __name__ == "__main__":
    asyncio.run(demo())
