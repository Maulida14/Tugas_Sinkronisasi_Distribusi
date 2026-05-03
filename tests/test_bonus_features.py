"""Tests for project-specific bonus features."""

from src.bonus.features import (
    AdaptiveLoadBalancer,
    BonusSecurityService,
    GeoDistributedRouter,
    PBFTCoordinator,
)


def test_security_encrypt_decrypt_and_audit(tmp_path):
    audit_log = tmp_path / "audit.log"
    audit_hash = tmp_path / "audit.hash"
    service = BonusSecurityService(
        node_id="node_1",
        enable_encryption=True,
        enable_audit=True,
        auth_enabled=True,
        api_keys="admin-key:admin,viewer-key:viewer",
        encryption_secret="test-secret",
        audit_log_path=str(audit_log),
        audit_hash_path=str(audit_hash),
    )

    token = service.encrypt({"message": "hello"}, key_material="shared")
    plaintext = service.decrypt(token, key_material="shared")

    assert plaintext["message"] == "hello"
    principal = service.enforce("admin-key", "encrypt")
    assert principal["role"] == "admin"

    service.record_audit("encrypt", "admin", "payload", "success")
    service.record_audit("decrypt", "admin", "payload", "success")

    assert audit_log.exists()
    assert service.verify_audit_chain() is True
    assert len(service.get_audit_log()) == 2


def test_security_authorization_failure(tmp_path):
    service = BonusSecurityService(
        node_id="node_1",
        auth_enabled=True,
        api_keys="viewer-key:viewer",
        audit_log_path=str(tmp_path / "audit.log"),
        audit_hash_path=str(tmp_path / "audit.hash"),
    )

    try:
        service.enforce("viewer-key", "pbft:commit")
    except PermissionError as exc:
        assert str(exc) == "insufficient_role"
    else:
        raise AssertionError("Expected insufficient_role error")


def test_pbft_tracks_prepare_and_commit_votes():
    coordinator = PBFTCoordinator("node_1", ["node_1", "node_2", "node_3", "node_4"])

    result = coordinator.submit_request({"transaction": "write-x"}, faulty_nodes=1)

    assert result.status == "committed"
    assert result.prepare_votes >= coordinator.quorum
    assert result.commit_votes >= coordinator.quorum
    stats = coordinator.get_stats()
    assert stats["requests"] == 1
    assert stats["recent"][0]["status"] == "committed"


def test_geo_router_uses_latency_map_and_records_replication():
    router = GeoDistributedRouter(
        node_id="node_1",
        local_region="asia-southeast",
        region_map="asia-southeast:22,us-east:70,eu-west:95",
    )

    route = router.route(client_region="us-east")
    replication = router.replicate("user:1", {"name": "andi"}, preferred_region="us-east")

    assert route.name == "us-east"
    assert replication["primary_region"] == "us-east"
    assert replication["consistency_model"] == "eventual"
    assert router.get_stats()["replications"] == 1


def test_adaptive_balancer_prefers_healthier_node():
    balancer = AdaptiveLoadBalancer(["node_1", "node_2", "node_3"])

    balancer.feedback("node_1", latency_ms=120.0, success=False)
    balancer.feedback("node_2", latency_ms=20.0, success=True)
    balancer.feedback("node_3", latency_ms=65.0, success=True)

    selected = balancer.choose_node(workload=1.0)
    stats = balancer.get_stats()

    assert selected == "node_2"
    assert stats["best_node"] == "node_2"
    assert "node_2" in stats["states"]
