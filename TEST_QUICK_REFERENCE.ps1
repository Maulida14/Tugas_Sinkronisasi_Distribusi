#!/usr/bin/env pwsh
# Test Scripts Quick Reference
# Save as: TEST_QUICK_REFERENCE.txt

Write-Host @"
╔════════════════════════════════════════════════════════════════════════════╗
║                 DISTRIBUTED SYSTEM - TEST QUICK REFERENCE                 ║
╚════════════════════════════════════════════════════════════════════════════╝

📌 PREREQUISITE - Start Docker Cluster
─────────────────────────────────────────────────────────────────────────────
Run this FIRST in one PowerShell window:

    docker compose -f docker/docker-compose.yml up -d

Wait 10 seconds for cluster to initialize...

Verify all nodes are running:

    curl http://localhost:8001/health
    curl http://localhost:8002/health
    curl http://localhost:8003/health
    curl http://localhost:8004/health

Response should show: "status": "healthy"

─────────────────────────────────────────────────────────────────────────────

🧪 TEST SCRIPTS - Choose One:
─────────────────────────────────────────────────────────────────────────────

1️⃣  TEST RAFT CONSENSUS
   Purpose: Verify distributed consensus, leader election, log replication
   Command: .\test_raft_consensus.ps1
   Runtime: ~1-2 minutes
   Key Metrics:
   - All 4 nodes healthy
   - 1 leader elected, 3 followers
   - Log replication successful
   - Heartbeat stable

2️⃣  TEST LOCK MANAGER
   Purpose: Verify distributed locks, deadlock detection, fairness
   Command: .\test_lock_manager.ps1
   Runtime: ~1 minute
   Key Metrics:
   - Exclusive locks prevent access
   - Shared locks allow multiple readers
   - Deadlock detection working
   - Lock release successful

3️⃣  TEST QUEUE SERVICE
   Purpose: Verify message publishing, consumption, at-least-once delivery
   Command: .\test_queue_service.ps1
   Runtime: ~1-2 minutes
   Key Metrics:
   - 50+ messages published
   - Multiple consumers working
   - ACK-based persistence
   - Queue depth tracking

4️⃣  TEST CACHE SERVICE
   Purpose: Verify cache coherence, MOESI protocol, hit rate
   Command: .\test_cache_service.ps1
   Runtime: ~1-2 minutes
   Key Metrics:
   - Cache hit rate > 80%
   - LRU eviction working
   - TTL expiration accurate
   - Invalidation broadcast successful

5️⃣  RUN ALL TESTS (Interactive Menu)
   Purpose: Run all tests with visual menu
   Command: .\run_all_tests.ps1
   Runtime: ~5-10 minutes
   Features:
   - Choose which tests to run
   - Sequential execution
   - Summary report

─────────────────────────────────────────────────────────────────────────────

🎯 COMMON WORKFLOWS:
─────────────────────────────────────────────────────────────────────────────

WORKFLOW 1: Quick Verification (5 minutes)
─────────────────────────────────────────
cd c:\Users\maulida\TUGAS3_Sister\distributed_system
.\test_raft_consensus.ps1           # 1-2 min: Check consensus
.\test_lock_manager.ps1             # 1 min: Check locks
.\test_queue_service.ps1            # 1-2 min: Check queue

Expected: All green checkmarks

─────────────────────────────────────────

WORKFLOW 2: Deep Testing (Full Features)
─────────────────────────────────────────
.\run_all_tests.ps1
# Select: 5 (All Tests)
# Wait for completion
# Review Summary Report

Expected: All 4 test suites pass

─────────────────────────────────────────

WORKFLOW 3: Focused Testing
─────────────────────────────────────────
# Test specific feature multiple times
.\test_lock_manager.ps1
.\test_lock_manager.ps1  # Run again to verify consistency
.\test_lock_manager.ps1  # Third run

Expected: Consistent results

─────────────────────────────────────────

WORKFLOW 4: Performance Monitoring
─────────────────────────────────────────
# Run while monitoring system metrics
.\test_queue_service.ps1
# Watch throughput and latency numbers

─────────────────────────────────────────

🔍 OUTPUT SYMBOLS GUIDE:
─────────────────────────────────────────────────────────────────────────────

✓ Green  = PASS - Feature working correctly
✗ Red    = FAIL - Feature not working, check logs
⚠ Yellow = SKIP or WARNING - Non-critical issue or node not running
─ Dash   = Section separator

─────────────────────────────────────────────────────────────────────────────

⚡ QUICK COMMANDS:
─────────────────────────────────────────────────────────────────────────────

# Check cluster status
curl http://localhost:8001/status | jq

# View system metrics
curl http://localhost:8001/metrics | jq

# View Raft info
curl http://localhost:8001/raft/info | jq

# Stop cluster
docker compose down

# View logs from specific node
docker logs -f node_1

# Remove all data and restart
docker compose down -v
docker compose up -d

─────────────────────────────────────────────────────────────────────────────

🆘 TROUBLESHOOTING:
─────────────────────────────────────────────────────────────────────────────

PROBLEM: "Cannot connect to localhost:8001"
SOLUTION: 
  1. Verify Docker is running: docker ps
  2. Start cluster: docker compose -f docker/docker-compose.yml up -d
  3. Wait 10-15 seconds for nodes to start
  4. Try again: curl http://localhost:8001/health

PROBLEM: "No leader detected"
SOLUTION:
  1. Wait 5-10 seconds for election to complete
  2. Check all nodes: docker ps
  3. If <3 nodes running, cluster won't have leader
  4. Restart: docker compose restart

PROBLEM: "Lock/Queue/Cache tests are slow"
SOLUTION:
  1. Check system resources: docker stats
  2. Verify no other heavy processes
  3. Increase timeout in test script
  4. Reduce load if testing under heavy stress

PROBLEM: "Some tests pass, some fail randomly"
SOLUTION:
  1. Likely network timing issue
  2. Increase timeout values in test script
  3. Run tests sequentially instead of parallel
  4. Check Docker network: docker network inspect distributed_network

─────────────────────────────────────────────────────────────────────────────

📊 EXPECTED PERFORMANCE:
─────────────────────────────────────────────────────────────────────────────

Feature              Latency      Throughput    Success Rate
─────────────────────────────────────────────────────────────
Raft Consensus       < 100ms      N/A           100%
Lock Acquire         ~50ms        2000/sec      100%
Lock Release         ~20ms        5000/sec      100%
Queue Publish        ~30ms        1000/sec      100%
Queue Consume        ~20ms        1000/sec      100%
Cache Get (hit)      ~5ms         10000/sec     95%+ hit rate
Cache Get (miss)     ~2ms         N/A           As expected
Cache Put            ~40ms        1000/sec      100%
Cache Invalidate     ~5ms         5000/sec      100%

─────────────────────────────────────────────────────────────────────────────

✅ TEST PASS CRITERIA:
─────────────────────────────────────────────────────────────────────────────

RAFT CONSENSUS TEST - PASS if:
  ☑ All 4 nodes healthy
  ☑ 1 leader elected
  ☑ 3 followers confirmed
  ☑ Log replication working
  ☑ Heartbeat stable
  ☑ No term changes during monitoring

LOCK MANAGER TEST - PASS if:
  ☑ Exclusive locks acquired
  ☑ Exclusive locks prevent concurrent access
  ☑ Shared locks allow multiple readers
  ☑ Lock release successful
  ☑ Statistics accurate
  ☑ Stress test handles 10+ locks

QUEUE SERVICE TEST - PASS if:
  ☑ Messages published successfully
  ☑ Multiple consumers working
  ☑ Messages acknowledged
  ☑ Queue depth tracked
  ☑ Stress test handles 50+ messages
  ☑ Different payload types supported

CACHE SERVICE TEST - PASS if:
  ☑ Cache hit rate > 80%
  ☑ GET/PUT operations working
  ☑ TTL expiration accurate
  ☑ Cache invalidation working
  ☑ LRU eviction functioning
  ☑ Stress test handles 100+ keys

─────────────────────────────────────────────────────────────────────────────

📝 NOTES:
─────────────────────────────────────────────────────────────────────────────

• Always start Docker cluster FIRST
• Tests require ALL 4 nodes running
• Run tests in order: Raft → Lock → Queue → Cache
• Keep terminal windows open to see full output
• Use TEST_QUICK_REFERENCE.ps1 to display this guide in terminal

─────────────────────────────────────────────────────────────────────────────

Last Updated: May 2, 2026
Version: 1.0.0
"@
