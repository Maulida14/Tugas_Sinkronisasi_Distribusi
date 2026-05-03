"""Bonus feature implementations: PBFT, geo replication, ML balancing, and security."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4


logger = logging.getLogger(__name__)


@dataclass
class AuditRecord:
    timestamp: str
    action: str
    actor: str
    resource: str
    result: str
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GeoRegion:
    name: str
    base_latency_ms: float
    healthy: bool = True


@dataclass
class PBFTResult:
    request_id: str
    status: str
    quorum: int
    replicas: int
    prepare_votes: int
    commit_votes: int
    committed_value: Dict[str, Any]


class BonusSecurityService:
    """Lightweight security features: encryption, RBAC, and audit logging."""

    def __init__(
        self,
        node_id: str,
        enable_encryption: bool = True,
        enable_audit: bool = True,
        auth_enabled: bool = False,
        api_keys: str = "",
        encryption_secret: Optional[str] = None,
        audit_log_path: Optional[str] = None,
        audit_hash_path: Optional[str] = None,
    ):
        self.node_id = node_id
        self.enable_encryption = enable_encryption
        self.enable_audit = enable_audit
        self.auth_enabled = auth_enabled
        self.encryption_secret = encryption_secret
        self.audit_log_path = Path(audit_log_path) if audit_log_path else None
        self.audit_hash_path = Path(audit_hash_path) if audit_hash_path else None
        self.audit_log: List[AuditRecord] = []
        self.role_permissions: Dict[str, set] = {
            "admin": {"*"},
            "operator": {"read", "write", "publish", "consume", "lock", "encrypt", "decrypt", "pbft:commit"},
            "viewer": {"read", "status"},
        }
        self.api_keys = self._parse_api_keys(api_keys)

    def _parse_api_keys(self, api_keys: str) -> Dict[str, str]:
        parsed: Dict[str, str] = {}
        for pair in (api_keys or "").split(","):
            if not pair.strip() or ":" not in pair:
                continue
            token, role = pair.split(":", 1)
            parsed[token.strip()] = role.strip()
        return parsed

    def _key_material(self, key_material: Optional[str]) -> bytes:
        material = key_material or self.encryption_secret or self.node_id
        return hashlib.sha256(material.encode("utf-8")).digest()

    def _xor_bytes(self, data: bytes, key_material: Optional[str]) -> bytes:
        key = self._key_material(key_material)
        return bytes(byte ^ key[index % len(key)] for index, byte in enumerate(data))

    def encrypt(self, payload: Any, key_material: Optional[str] = None) -> str:
        if not self.enable_encryption:
            return json.dumps(payload) if not isinstance(payload, str) else payload

        raw = payload if isinstance(payload, str) else json.dumps(payload, separators=(",", ":"))
        cipher_bytes = self._xor_bytes(raw.encode("utf-8"), key_material)
        return base64.urlsafe_b64encode(cipher_bytes).decode("utf-8")

    def decrypt(self, ciphertext: str, key_material: Optional[str] = None) -> Any:
        if not self.enable_encryption:
            try:
                return json.loads(ciphertext)
            except Exception:
                return ciphertext

        decoded = base64.urlsafe_b64decode(ciphertext.encode("utf-8"))
        plaintext = self._xor_bytes(decoded, key_material).decode("utf-8")
        try:
            return json.loads(plaintext)
        except Exception:
            return plaintext

    def authorize(self, role: str, action: str) -> bool:
        permissions = self.role_permissions.get(role, set())
        return "*" in permissions or action in permissions

    def enforce(self, api_key: str, action: str) -> Dict[str, str]:
        if not self.auth_enabled:
            return {"api_key": api_key, "role": "admin"}

        role = self.api_keys.get(api_key)
        if not role:
            raise PermissionError("invalid_api_key")
        if not self.authorize(role, action):
            raise PermissionError("insufficient_role")
        return {"api_key": api_key, "role": role}

    def _record_hash(self, record: AuditRecord) -> str:
        previous = ""
        if self.audit_hash_path and self.audit_hash_path.exists():
            previous = self.audit_hash_path.read_text(encoding="utf-8").strip()

        payload = json.dumps(record.__dict__, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(f"{previous}{payload}".encode("utf-8")).hexdigest()

        if self.audit_log_path:
            self.audit_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.audit_log_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record.__dict__, sort_keys=True) + "\n")
        if self.audit_hash_path:
            self.audit_hash_path.parent.mkdir(parents=True, exist_ok=True)
            self.audit_hash_path.write_text(digest, encoding="utf-8")
        return digest

    def record_audit(self, action: str, actor: str, resource: str, result: str, details: Optional[Dict[str, Any]] = None) -> None:
        if not self.enable_audit:
            return

        record = AuditRecord(
            timestamp=datetime.utcnow().isoformat(),
            action=action,
            actor=actor,
            resource=resource,
            result=result,
            details=details or {},
        )
        self.audit_log.append(record)
        self._record_hash(record)

    def get_audit_log(self) -> List[Dict[str, Any]]:
        return [record.__dict__ for record in self.audit_log]

    def get_stats(self) -> Dict[str, Any]:
        return {
            "encryption_enabled": self.enable_encryption,
            "audit_enabled": self.enable_audit,
            "audit_count": len(self.audit_log),
        }

    def verify_audit_chain(self) -> bool:
        if not self.audit_log_path or not self.audit_hash_path:
            return True
        if not self.audit_log_path.exists() or not self.audit_hash_path.exists():
            return False

        previous = ""
        for line in self.audit_log_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.dumps(json.loads(line), sort_keys=True, separators=(",", ":"))
            previous = hashlib.sha256(f"{previous}{payload}".encode("utf-8")).hexdigest()
        return previous == self.audit_hash_path.read_text(encoding="utf-8").strip()


class PBFTCoordinator:
    """PBFT-style request simulator for bonus validation and demonstration."""

    def __init__(self, node_id: str, replicas: List[str]):
        self.node_id = node_id
        self.replicas = replicas[:]
        if node_id not in self.replicas:
            self.replicas.insert(0, node_id)
        self.request_log: List[PBFTResult] = []

    @property
    def fault_tolerance(self) -> int:
        return max(1, (len(self.replicas) - 1) // 3)

    @property
    def quorum(self) -> int:
        return (2 * self.fault_tolerance) + 1

    def submit_request(self, payload: Dict[str, Any], faulty_nodes: int = 0) -> PBFTResult:
        request_id = str(uuid4())
        active_replicas = max(0, len(self.replicas) - max(0, faulty_nodes))
        prepare_votes = active_replicas
        commit_votes = active_replicas
        status = "committed" if prepare_votes >= self.quorum and commit_votes >= self.quorum else "partial"

        result = PBFTResult(
            request_id=request_id,
            status=status,
            quorum=self.quorum,
            replicas=len(self.replicas),
            prepare_votes=prepare_votes,
            commit_votes=commit_votes,
            committed_value=payload,
        )
        self.request_log.append(result)
        return result

    def get_stats(self) -> Dict[str, Any]:
        committed = sum(1 for item in self.request_log if item.status == "committed")
        return {
            "node_id": self.node_id,
            "replicas": len(self.replicas),
            "fault_tolerance": self.fault_tolerance,
            "quorum": self.quorum,
            "requests": len(self.request_log),
            "committed": committed,
            "recent": [item.__dict__ for item in self.request_log[-5:]],
        }


class GeoDistributedRouter:
    """Simple geo-distribution simulator with latency-aware routing."""

    def __init__(
        self,
        node_id: str,
        regions: Optional[List[GeoRegion]] = None,
        local_region: Optional[str] = None,
        region_map: Optional[str] = None,
    ):
        self.node_id = node_id
        self.local_region = local_region
        self.regions: List[GeoRegion] = regions or self._parse_region_map(region_map) or [
            GeoRegion("asia-southeast", 22.0),
            GeoRegion("us-east", 70.0),
            GeoRegion("eu-west", 95.0),
        ]
        self.replication_log: List[Dict[str, Any]] = []

    def _parse_region_map(self, region_map: Optional[str]) -> List[GeoRegion]:
        regions: List[GeoRegion] = []
        for pair in (region_map or "").split(","):
            if not pair.strip() or ":" not in pair:
                continue
            name, latency = pair.split(":", 1)
            regions.append(GeoRegion(name.strip(), float(latency.strip())))
        return regions

    def route(self, preferred_region: Optional[str] = None, client_region: Optional[str] = None) -> GeoRegion:
        healthy_regions = [region for region in self.regions if region.healthy]
        if not healthy_regions:
            raise ValueError("No healthy regions available")

        preferred_region = preferred_region or client_region
        if preferred_region:
            for region in healthy_regions:
                if region.name == preferred_region:
                    return region

        return min(healthy_regions, key=lambda region: region.base_latency_ms)

    def replicate(self, key: str, value: Any, preferred_region: Optional[str] = None) -> Dict[str, Any]:
        primary = self.route(preferred_region)
        replica_regions = sorted(
            [region for region in self.regions if region.healthy and region.name != primary.name],
            key=lambda region: region.base_latency_ms,
        )

        record = {
            "key": key,
            "primary_region": primary.name,
            "replica_regions": [region.name for region in replica_regions],
            "consistency_model": "eventual",
            "payload": value,
            "timestamp": datetime.utcnow().isoformat(),
        }
        self.replication_log.append(record)
        return record

    def get_stats(self) -> Dict[str, Any]:
        healthy = [region for region in self.regions if region.healthy]
        return {
            "regions": len(self.regions),
            "healthy_regions": len(healthy),
            "fastest_region": self.route().name if healthy else None,
            "replications": len(self.replication_log),
        }


class AdaptiveLoadBalancer:
    """Online adaptive load balancer with latency feedback and scoring."""

    def __init__(self, nodes: List[str]):
        self.nodes = nodes[:]
        self.node_state: Dict[str, Dict[str, float]] = {
            node: {"load": 0.0, "latency": 50.0, "success_rate": 1.0} for node in self.nodes
        }
        self.history: List[Dict[str, Any]] = []

    def _score(self, node: str, predicted_load: float = 0.0) -> float:
        state = self.node_state[node]
        load_component = state["load"] + predicted_load
        latency_component = state["latency"]
        reliability_component = 100.0 - (state["success_rate"] * 100.0)
        return (load_component * 0.45) + (latency_component * 0.4) + (reliability_component * 0.15)

    def choose_node(self, workload: float = 1.0, exclude: Optional[List[str]] = None) -> str:
        exclude = set(exclude or [])
        candidates = [node for node in self.nodes if node not in exclude]
        if not candidates:
            raise ValueError("No nodes available for selection")

        selected = min(candidates, key=lambda node: self._score(node, workload))
        self.node_state[selected]["load"] += workload
        self.history.append(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "selected": selected,
                "workload": workload,
            }
        )
        return selected

    def feedback(self, node: str, latency_ms: float, success: bool = True) -> Dict[str, float]:
        if node not in self.node_state:
            self.node_state[node] = {"load": 0.0, "latency": latency_ms, "success_rate": 1.0 if success else 0.0}

        state = self.node_state[node]
        alpha = 0.3
        state["latency"] = (state["latency"] * (1 - alpha)) + (latency_ms * alpha)
        state["success_rate"] = (state["success_rate"] * (1 - alpha)) + ((1.0 if success else 0.0) * alpha)
        state["load"] = max(0.0, state["load"] - 1.0)
        return state

    def get_stats(self) -> Dict[str, Any]:
        return {
            "nodes": len(self.nodes),
            "history": len(self.history),
            "best_node": min(self.nodes, key=lambda node: self._score(node)) if self.nodes else None,
            "states": self.node_state,
        }
