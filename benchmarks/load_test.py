"""Load testing with Locust"""

from locust import HttpUser, task, constant, between
import json
import random


class DistributedSystemUser(HttpUser):
    """Locust user for load testing"""
    
    wait_time = between(1, 3)
    host = "http://localhost:8001"
    
    def on_start(self):
        """Initialize user"""
        self.locks = []
        self.messages = []
    
    
    @task(1)
    def health_check(self):
        """Health check"""
        self.client.get("/health")
    
    
    @task(2)
    def acquire_lock(self):
        """Acquire distributed lock"""
        response = self.client.post(
            "/lock/acquire",
            json={
                "resource": f"resource_{random.randint(1, 5)}",
                "lock_type": random.choice(["shared", "exclusive"]),
                "client_id": f"client_{random.randint(1, 10)}",
                "timeout": 10
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            self.locks.append(data.get("lock_id"))
    
    
    @task(1)
    def release_lock(self):
        """Release lock"""
        if self.locks:
            lock_id = self.locks.pop(0)
            self.client.post(
                "/lock/release",
                json={"lock_id": lock_id}
            )
    
    
    @task(2)
    def publish_message(self):
        """Publish message"""
        response = self.client.post(
            "/queue/publish",
            json={
                "topic": f"topic_{random.randint(1, 3)}",
                "payload": {"data": f"message_{random.randint(1, 1000)}"}
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            self.messages.append(data.get("msg_id"))
    
    
    @task(2)
    def consume_message(self):
        """Consume messages"""
        self.client.post(
            "/queue/consume",
            json={
                "topic": f"topic_{random.randint(1, 3)}",
                "consumer_id": f"consumer_{random.randint(1, 5)}",
                "batch_size": 1
            }
        )
    
    
    @task(2)
    def cache_put(self):
        """Put value in cache"""
        self.client.post(
            "/cache/put",
            json={
                "key": f"cache_key_{random.randint(1, 100)}",
                "value": f"cache_value_{random.randint(1, 1000)}",
                "ttl": 3600
            }
        )
    
    
    @task(2)
    def cache_get(self):
        """Get value from cache"""
        self.client.get(
            f"/cache/get/cache_key_{random.randint(1, 100)}"
        )
    
    
    @task(1)
    def get_metrics(self):
        """Get system metrics"""
        self.client.get("/metrics")
