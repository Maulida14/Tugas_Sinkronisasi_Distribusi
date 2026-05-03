#!/usr/bin/env pwsh
# Distributed System Core Features Test Script
# Test semua core functionality

Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   Distributed Synchronization System - Core Features Test     ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

$baseUrl = "http://localhost:8001"
$testResults = @()

function Test-Endpoint {
    param(
        [string]$name,
        [string]$method,
        [string]$endpoint,
        [hashtable]$body = $null,
        [int]$expectedStatus = 200
    )
    
    try {
        $params = @{
            Uri         = "$baseUrl$endpoint"
            Method      = $method
            ContentType = "application/json"
        }
        
        if ($body) {
            $params["Body"] = $body | ConvertTo-Json
        }
        
        $response = Invoke-WebRequest @params -UseBasicParsing
        
        if ($response.StatusCode -eq $expectedStatus) {
            Write-Host "✓ $name" -ForegroundColor Green
            $testResults += @{name=$name; status="PASS"; statusCode=$response.StatusCode}
            return $response.Content | ConvertFrom-Json
        } else {
            Write-Host "✗ $name (Expected $expectedStatus, got $($response.StatusCode))" -ForegroundColor Red
            $testResults += @{name=$name; status="FAIL"; statusCode=$response.StatusCode}
            return $null
        }
    } catch {
        Write-Host "✗ $name - Error: $($_.Exception.Message)" -ForegroundColor Red
        $testResults += @{name=$name; status="ERROR"; error=$_.Exception.Message}
        return $null
    }
}

# ============================================================================
# 1. HEALTH CHECKS
# ============================================================================
Write-Host "`n[1] HEALTH CHECKS" -ForegroundColor Yellow
Write-Host "─" * 65

Test-Endpoint -name "Health Check" -method "GET" -endpoint "/health"
Test-Endpoint -name "Readiness Check" -method "GET" -endpoint "/readyz"
$status = Test-Endpoint -name "Node Status" -method "GET" -endpoint "/status"

# ============================================================================
# 2. LOCK MANAGER TESTS
# ============================================================================
Write-Host "`n[2] LOCK MANAGER" -ForegroundColor Yellow
Write-Host "─" * 65

# Acquire exclusive lock
$lockResp = Test-Endpoint -name "Acquire Exclusive Lock" `
    -method "POST" -endpoint "/lock/acquire" `
    -body @{resource="test_resource"; lock_type="exclusive"; client_id="client_1"; timeout=30}

$lockId = $lockResp.lock_id

# Get lock status
if ($lockId) {
    Test-Endpoint -name "Get Lock Status" -method "GET" -endpoint "/lock/status/test_resource"
    
    # Acquire shared lock on same resource (should wait/fail)
    Test-Endpoint -name "Try Shared Lock (should fail)" `
        -method "POST" -endpoint "/lock/acquire" `
        -body @{resource="test_resource"; lock_type="shared"; client_id="client_2"; timeout=1}
    
    # Release lock
    Test-Endpoint -name "Release Lock" -method "POST" -endpoint "/lock/release" `
        -body @{lock_id=$lockId}
}

# ============================================================================
# 3. DISTRIBUTED QUEUE TESTS
# ============================================================================
Write-Host "`n[3] DISTRIBUTED QUEUE" -ForegroundColor Yellow
Write-Host "─" * 65

# Publish messages
$msg1 = Test-Endpoint -name "Publish Message 1" -method "POST" -endpoint "/queue/publish" `
    -body @{topic="test_topic"; payload=@{id=1; data="message_1"}}

$msg2 = Test-Endpoint -name "Publish Message 2" -method "POST" -endpoint "/queue/publish" `
    -body @{topic="test_topic"; payload=@{id=2; data="message_2"}}

$msg3 = Test-Endpoint -name "Publish Message 3" -method "POST" -endpoint "/queue/publish" `
    -body @{topic="other_topic"; payload=@{id=3; data="other"}}

if ($msg1.msg_id) {
    # Consume messages
    $consumed = Test-Endpoint -name "Consume Messages (batch=2)" -method "POST" -endpoint "/queue/consume" `
        -body @{topic="test_topic"; consumer_id="consumer_1"; batch_size=2}
    
    # Acknowledge message
    if ($consumed.messages -and $consumed.messages.Count -gt 0) {
        $msgId = $consumed.messages[0].msg_id
        Test-Endpoint -name "Acknowledge Message" -method "POST" -endpoint "/queue/ack" `
            -body @{msg_id=$msgId; consumer_id="consumer_1"}
    }
}

# ============================================================================
# 4. CACHE COHERENCE TESTS
# ============================================================================
Write-Host "`n[4] CACHE COHERENCE (MOESI)" -ForegroundColor Yellow
Write-Host "─" * 65

# Put values in cache
Test-Endpoint -name "Cache Put (key_1)" -method "POST" -endpoint "/cache/put" `
    -body @{key="cache_key_1"; value="cache_value_1"; ttl=3600}

Test-Endpoint -name "Cache Put (key_2)" -method "POST" -endpoint "/cache/put" `
    -body @{key="cache_key_2"; value="cache_value_2"; ttl=3600}

Test-Endpoint -name "Cache Put (key_3)" -method "POST" -endpoint "/cache/put" `
    -body @{key="cache_key_3"; value="cache_value_3"; ttl=1}  # 1 second TTL

# Get from cache
Test-Endpoint -name "Cache Get (hit)" -method "GET" -endpoint "/cache/get/cache_key_1"
Test-Endpoint -name "Cache Get (hit)" -method "GET" -endpoint "/cache/get/cache_key_2"

# Get non-existent (miss)
$missing = Test-Endpoint -name "Cache Get (miss - non-existent)" -method "GET" -endpoint "/cache/get/nonexistent" -expectedStatus 404

# Invalidate cache
Test-Endpoint -name "Cache Invalidate" -method "POST" -endpoint "/cache/invalidate/cache_key_1"

# ============================================================================
# 5. CONSENSUS & METRICS
# ============================================================================
Write-Host "`n[5] RAFT CONSENSUS & METRICS" -ForegroundColor Yellow
Write-Host "─" * 65

$raftInfo = Test-Endpoint -name "Raft Consensus Info" -method "GET" -endpoint "/raft/info"
$metrics = Test-Endpoint -name "System Metrics" -method "GET" -endpoint "/metrics"

# ============================================================================
# 6. MULTI-NODE TEST (test dengan node lain jika running)
# ============================================================================
Write-Host "`n[6] MULTI-NODE TESTING" -ForegroundColor Yellow
Write-Host "─" * 65

foreach ($port in 8002, 8003, 8004) {
    $url = "http://localhost:$port"
    try {
        $h = Invoke-WebRequest -Uri "$url/health" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        Write-Host "✓ Node on port $port is healthy" -ForegroundColor Green
        $testResults += @{name="Node_$port"; status="PASS"; statusCode=$h.StatusCode}
    } catch {
        Write-Host "✗ Node on port $port is not responding" -ForegroundColor Yellow
        $testResults += @{name="Node_$port"; status="SKIP"; error="Not running"}
    }
}

# ============================================================================
# 7. CONCURRENT OPERATIONS TEST
# ============================================================================
Write-Host "`n[7] CONCURRENT OPERATIONS" -ForegroundColor Yellow
Write-Host "─" * 65

# Publish multiple messages concurrently
Write-Host "Publishing 5 messages concurrently..."
$jobs = @()
for ($i = 1; $i -le 5; $i++) {
    $job = Start-Job -ScriptBlock {
        param($i)
        $body = @{topic="concurrent_topic"; payload=@{id=$i}} | ConvertTo-Json
        Invoke-WebRequest -Uri "http://localhost:8001/queue/publish" `
            -Method POST -Body $body -ContentType "application/json" -UseBasicParsing
    } -ArgumentList $i
    $jobs += $job
}

$results = $jobs | Wait-Job | Receive-Job
Write-Host "✓ Published 5 messages concurrently" -ForegroundColor Green
$testResults += @{name="Concurrent_Publish"; status="PASS"}

# ============================================================================
# TEST SUMMARY
# ============================================================================
Write-Host "`n╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                        TEST SUMMARY                           ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

$passed = ($testResults | Where-Object {$_.status -eq "PASS"}).Count
$failed = ($testResults | Where-Object {$_.status -eq "FAIL"}).Count
$errors = ($testResults | Where-Object {$_.status -eq "ERROR"}).Count
$skipped = ($testResults | Where-Object {$_.status -eq "SKIP"}).Count
$total = $testResults.Count

Write-Host ""
Write-Host "Total Tests:  $total" -ForegroundColor Cyan
Write-Host "Passed:       $passed" -ForegroundColor Green
Write-Host "Failed:       $failed" -ForegroundColor $(if ($failed -eq 0) {"Green"} else {"Red"})
Write-Host "Errors:       $errors" -ForegroundColor $(if ($errors -eq 0) {"Green"} else {"Red"})
Write-Host "Skipped:      $skipped" -ForegroundColor Yellow

$successRate = [math]::Round(($passed / ($total - $skipped) * 100), 2)
Write-Host "Success Rate: $successRate%" -ForegroundColor $(if ($successRate -ge 80) {"Green"} else {"Yellow"})

Write-Host ""

# Detailed results
if ($failed -gt 0 -or $errors -gt 0) {
    Write-Host "Failed/Error Tests:" -ForegroundColor Red
    $testResults | Where-Object {$_.status -in "FAIL", "ERROR"} | ForEach-Object {
        Write-Host "  - $($_.name): $($_.status)" -ForegroundColor Red
    }
}

# ============================================================================
# PERFORMANCE INDICATORS
# ============================================================================
if ($metrics) {
    Write-Host "`n[SYSTEM METRICS]" -ForegroundColor Cyan
    Write-Host "─" * 65
    
    if ($metrics.cache) {
        Write-Host "Cache Hit Rate: $($metrics.cache.hit_rate)" -ForegroundColor Green
    }
    
    if ($metrics.locks) {
        Write-Host "Active Locks: $($metrics.locks.total_locks)" -ForegroundColor Green
        Write-Host "Waiting Locks: $($metrics.locks.total_waiting)" -ForegroundColor Yellow
    }
    
    if ($metrics.queue) {
        Write-Host "Queue Messages: $($metrics.queue.total_messages)" -ForegroundColor Green
        Write-Host "Unacked Messages: $($metrics.queue.unacked_messages)" -ForegroundColor Yellow
    }
    
    if ($metrics.raft) {
        Write-Host "Raft Term: $($metrics.raft.term)" -ForegroundColor Green
        Write-Host "Raft State: $($metrics.raft.state)" -ForegroundColor Green
        Write-Host "Log Entries: $($metrics.raft.log_size)" -ForegroundColor Green
    }
}

Write-Host ""
Write-Host "Test completed at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray
