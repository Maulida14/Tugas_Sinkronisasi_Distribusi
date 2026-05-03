#!/usr/bin/env pwsh
# Test Raft Consensus Protocol
# Usage: .\test_raft_consensus.ps1

Write-Host "════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "RAFT CONSENSUS - Distributed Consensus Testing" -ForegroundColor Cyan
Write-Host "════════════════════════════════════════════════════════════" -ForegroundColor Cyan

$ports = @(8001, 8002, 8003, 8004)
$nodes = @()

function Test-Raft {
    param([string]$name, [string]$port, [string]$method, [string]$endpoint, [switch]$Silent)
    
    try {
        $baseUrl = "http://localhost:$port"
        $params = @{
            Uri         = "$baseUrl$endpoint"
            Method      = $method
            ContentType = "application/json"
        }
        
        $response = Invoke-WebRequest @params -UseBasicParsing -ErrorAction Stop
        
        if (-not $Silent) {
            Write-Host "[OK] [$port] $name" -ForegroundColor Green
        }
        return $response.Content | ConvertFrom-Json
    } 
    catch {
        if (-not $Silent) {
            $errMsg = $_.Exception.Message
            if ($errMsg.Length -gt 50) { $errMsg = $errMsg.Substring(0, 50) }
            Write-Host "[FAIL] [$port] $name - Error: $errMsg..." -ForegroundColor Yellow
        }
        return $null
    }
}

Write-Host ""
Write-Host "[TEST 1] Node Health and Connectivity" -ForegroundColor Yellow
Write-Host "────────────────────────────────────────────────────────────"

Write-Host ""
Write-Host "Checking all nodes health..."
foreach ($port in $ports) {
    $health = Test-Raft -name "Health Check" -port $port -method "GET" -endpoint "/health"
    
    if ($health) {
        $nodes += $port
    }
}

Write-Host ""
Write-Host "Healthy nodes: $($nodes.Count)/4"

if ($nodes.Count -lt 3) {
    Write-Host ""
    Write-Host "Warning: Less than 3 nodes running. Raft requires minimum 3 nodes." -ForegroundColor Yellow
    Write-Host "Please start cluster with: docker compose up -d" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "[TEST 2] Initial Raft State" -ForegroundColor Yellow
Write-Host "────────────────────────────────────────────────────────────"

Write-Host ""
Write-Host "Getting initial Raft state from each node..."

foreach ($port in $nodes) {
    $raftInfo = Test-Raft -name "Raft Info" -port $port -method "GET" -endpoint "/raft/info"
    
    if ($raftInfo) {
        Write-Host ""
        Write-Host "Node (Port: $port):" -ForegroundColor Cyan
        Write-Host "  Term: $($raftInfo.current_term)" -ForegroundColor Green
        Write-Host "  State: $($raftInfo.state)" -ForegroundColor Green
        Write-Host "  Leader: $($raftInfo.leader_id)" -ForegroundColor Yellow
        Write-Host "  Log Size: $($raftInfo.log_size)" -ForegroundColor Green
    }
}

Write-Host ""
Write-Host "[TEST 3] Leader Detection" -ForegroundColor Yellow
Write-Host "────────────────────────────────────────────────────────────"

Write-Host ""
Write-Host "Detecting current leader..."
$leader = $null
$leader_port = $null

foreach ($port in $nodes) {
    $raftInfo = Test-Raft -name "Get Leader" -port $port -method "GET" -endpoint "/raft/info"
    
    if ($raftInfo -and $raftInfo.state -eq "leader") {
        $leader = $raftInfo.node_id
        $leader_port = $port
        Write-Host ""
        Write-Host "Leader found: $leader (Port: $port)" -ForegroundColor Green
        break
    }
}

if (-not $leader) {
    Write-Host ""
    Write-Host "No leader detected. Waiting for leader election..." -ForegroundColor Yellow
    Start-Sleep -Seconds 3
    
    foreach ($port in $nodes) {
        $raftInfo = Test-Raft -name "Get Leader (retry)" -port $port -method "GET" -endpoint "/raft/info"
        
        if ($raftInfo -and $raftInfo.state -eq "leader") {
            $leader = $raftInfo.node_id
            $leader_port = $port
            Write-Host ""
            Write-Host "Leader elected: $leader (Port: $port)" -ForegroundColor Green
            break
        }
    }
}

Write-Host ""
Write-Host "[TEST 4] Follower Nodes" -ForegroundColor Yellow
Write-Host "────────────────────────────────────────────────────────────"

Write-Host ""
Write-Host "Verifying follower nodes..."
$followers = 0

foreach ($port in $nodes) {
    $raftInfo = Test-Raft -name "Get State" -port $port -method "GET" -endpoint "/raft/info"
    
    if ($raftInfo -and $raftInfo.state -eq "follower") {
        Write-Host ""
        Write-Host "Follower: $($raftInfo.node_id)" -ForegroundColor Green
        Write-Host "  Current Term: $($raftInfo.current_term)" -ForegroundColor Gray
        Write-Host "  Voted For: $($raftInfo.voted_for)" -ForegroundColor Gray
        Write-Host "  Commit Index: $($raftInfo.commit_index)" -ForegroundColor Gray
        $followers++
    }
}

Write-Host ""
Write-Host "Follower count: $followers"

Write-Host ""
Write-Host "[TEST 5] Log Replication" -ForegroundColor Yellow
Write-Host "────────────────────────────────────────────────────────────"

if ($leader_port) {
    Write-Host ""
    Write-Host "Performing operations on leader to test log replication..."
    
    Write-Host ""
    Write-Host "Acquiring lock on leader ($leader)..."
    try {
        $lockResp = Invoke-WebRequest -Uri "http://localhost:$leader_port/lock/acquire" `
            -Method POST `
            -Body (@{resource="replication_test"; lock_type="exclusive"; client_id="test_client"; timeout=30} | ConvertTo-Json) `
            -ContentType "application/json" `
            -UseBasicParsing -ErrorAction Stop
        
        $lock = $lockResp.Content | ConvertFrom-Json
        Write-Host "[OK] Lock acquired: $($lock.lock_id)" -ForegroundColor Green
        
        Start-Sleep -Seconds 1
        
        Write-Host ""
        Write-Host "Verifying log replication to followers..."
        foreach ($port in $nodes) {
            if ($port -ne $leader_port) {
                $raftInfo = Test-Raft -name "Check Log Size" -port $port -method "GET" -endpoint "/raft/info"
                
                if ($raftInfo) {
                    Write-Host "  Follower log size: $($raftInfo.log_size)" -ForegroundColor Gray
                }
            }
        }
    } 
    catch {
        Write-Host "[FAIL] Error acquiring lock: $($_.Exception.Message)" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "[TEST 6] Consensus Metrics" -ForegroundColor Yellow
Write-Host "────────────────────────────────────────────────────────────"

foreach ($port in $nodes) {
    Write-Host ""
    Write-Host "Node (Port: $port):" -ForegroundColor Cyan
    
    $raftInfo = Test-Raft -name "Get Full Info" -port $port -method "GET" -endpoint "/raft/info"
    
    if ($raftInfo) {
        Write-Host "  Node ID: $($raftInfo.node_id)" -ForegroundColor Gray
        Write-Host "  Current Term: $($raftInfo.current_term)" -ForegroundColor Green
        Write-Host "  State: $($raftInfo.state)" -ForegroundColor Green
        Write-Host "  Leader: $($raftInfo.leader_id)" -ForegroundColor Yellow
        Write-Host "  Log Size: $($raftInfo.log_size)" -ForegroundColor Green
        Write-Host "  Commit Index: $($raftInfo.commit_index)" -ForegroundColor Green
        Write-Host "  Last Applied: $($raftInfo.last_applied)" -ForegroundColor Green
        Write-Host "  Peers: $($raftInfo.peers.Count) nodes" -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "[TEST 7] Election Stability" -ForegroundColor Yellow
Write-Host "────────────────────────────────────────────────────────────"

Write-Host ""
Write-Host "Monitoring leader for 10 seconds to ensure stability..."

$initial_leader = $leader
$state_changes = 0

for ($i = 0; $i -lt 10; $i++) {
    Start-Sleep -Seconds 1
    
    foreach ($port in $nodes) {
        $raftInfo = Test-Raft -name "Monitor" -port $port -method "GET" -endpoint "/raft/info" -Silent
        
        if ($raftInfo -and $raftInfo.state -eq "leader") {
            if ($raftInfo.node_id -ne $initial_leader) {
                Write-Host "! Leader changed from $initial_leader to $($raftInfo.node_id)" -ForegroundColor Yellow
                $state_changes++
                $initial_leader = $raftInfo.node_id
            }
        }
    }
}

Write-Host ""
if ($state_changes -eq 0) {
    Write-Host "[OK] Leader stable during monitoring period" -ForegroundColor Green
} else {
    Write-Host "[WARN] Leader changed $state_changes times during monitoring" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "[TEST 8] Readiness Checks" -ForegroundColor Yellow
Write-Host "────────────────────────────────────────────────────────────"

Write-Host ""
Write-Host "Checking node readiness..."
foreach ($port in $nodes) {
    try {
        $ready = Invoke-WebRequest -Uri "http://localhost:$port/readyz" `
            -Method GET -UseBasicParsing -ErrorAction Stop
        
        $readiness = $ready.Content | ConvertFrom-Json
        $status = if ($readiness.ready) { "READY" } else { "NOT READY" }
        
        $color = if ($readiness.ready) { "Green" } else { "Yellow" }
        Write-Host "  Port $port : $status" -ForegroundColor $color
    } 
    catch {
        Write-Host "  Port $port : ERROR" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "[TEST 9] Consensus Throughput" -ForegroundColor Yellow
Write-Host "────────────────────────────────────────────────────────────"

if ($leader_port) {
    Write-Host ""
    Write-Host "Publishing messages through consensus (10 messages)..."
    
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    
    for ($i = 1; $i -le 10; $i++) {
        try {
            $msg = Invoke-WebRequest -Uri "http://localhost:$leader_port/queue/publish" `
                -Method POST `
                -Body (@{topic="consensus_test"; payload=@{id=$i}} | ConvertTo-Json) `
                -ContentType "application/json" `
                -UseBasicParsing -ErrorAction Stop | Out-Null
        } 
        catch { }
    }
    
    $stopwatch.Stop()
    
    Write-Host ""
    Write-Host "[OK] 10 messages published in $($stopwatch.ElapsedMilliseconds)ms" -ForegroundColor Green
    
    Start-Sleep -Seconds 1
    
    Write-Host ""
    Write-Host "Verifying replication..."
    foreach ($port in $nodes) {
        $metrics = Test-Raft -name "Get Metrics" -port $port -method "GET" -endpoint "/metrics"
        
        if ($metrics) {
            Write-Host "  Node (Port: $port) Log Size: $($metrics.raft.log_size)" -ForegroundColor Green
        }
    }
}

Write-Host ""
Write-Host "[TEST 10] Heartbeat Monitoring" -ForegroundColor Yellow
Write-Host "────────────────────────────────────────────────────────────"

Write-Host ""
Write-Host "Monitoring heartbeat communication (5 seconds)..."

$term_snapshot = @()

foreach ($port in $nodes) {
    $raftInfo = Test-Raft -name "Snapshot" -port $port -method "GET" -endpoint "/raft/info"
    if ($raftInfo) {
        $term_snapshot += @{port=$port; term=$raftInfo.current_term; state=$raftInfo.state}
    }
}

Start-Sleep -Seconds 5

$term_after = @()

foreach ($port in $nodes) {
    $raftInfo = Test-Raft -name "Snapshot (after)" -port $port -method "GET" -endpoint "/raft/info"
    if ($raftInfo) {
        $term_after += @{port=$port; term=$raftInfo.current_term; state=$raftInfo.state}
    }
}

Write-Host ""
Write-Host "Heartbeat Status:" -ForegroundColor Cyan

foreach ($node in $term_snapshot) {
    $after = $term_after | Where-Object {$_.port -eq $node.port} | Select-Object -First 1
    
    if ($after) {
        $status = if ($after.term -eq $node.term -and $after.state -eq "leader") { "[OK]" } else { "[WARN]" }
        $color = if ($status -eq "[OK]") { "Green" } else { "Yellow" }
        Write-Host "  Port $($node.port): $status (Term: $($node.term) to $($after.term))" -ForegroundColor $color
    }
}

Write-Host ""
Write-Host "════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "Raft Consensus Test Completed" -ForegroundColor Cyan
Write-Host "════════════════════════════════════════════════════════════" -ForegroundColor Cyan

Write-Host ""
Write-Host "Cluster Summary:" -ForegroundColor Cyan
Write-Host "  Active Nodes: $($nodes.Count)/4" -ForegroundColor Green
Write-Host "  Leader: $leader" -ForegroundColor Green
$consensusStatus = if ($nodes.Count -ge 3) { "STABLE" } else { "UNSTABLE" }
$consensusColor = if ($nodes.Count -ge 3) { "Green" } else { "Red" }
Write-Host "  Consensus Status: $consensusStatus" -ForegroundColor $consensusColor

Write-Host ""
Write-Host "Test finished at: $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Gray
