#!/usr/bin/env pwsh
# Test Lock Manager Feature
# Usage: .\test_lock_manager.ps1

Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║        LOCK MANAGER - Distributed Lock Testing            ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

$baseUrl = "http://localhost:8001"
$locks = @()

function Test-Lock {
    param([string]$name, [string]$method, [string]$endpoint, [hashtable]$body = $null)
    
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
        
        Write-Host "✓ $name" -ForegroundColor Green
        return $response.Content | ConvertFrom-Json
    } catch {
        Write-Host "✗ $name - Error: $($_.Exception.Message)" -ForegroundColor Red
        return $null
    }
}

Write-Host "`n[TEST 1] Basic Lock Operations" -ForegroundColor Yellow
Write-Host "─" * 60

# Test 1: Acquire exclusive lock
Write-Host "`nAcquiring EXCLUSIVE lock on 'database'..."
$lock1 = Test-Lock -name "Acquire Exclusive Lock" -method "POST" -endpoint "/lock/acquire" `
    -body @{
        resource = "database"
        lock_type = "exclusive"
        client_id = "client_1"
        timeout = 30
    }

if ($lock1.lock_id) {
    $locks += $lock1.lock_id
    Write-Host "Lock ID: $($lock1.lock_id)" -ForegroundColor Green
}

Write-Host "`n[TEST 2] Lock Status Check" -ForegroundColor Yellow
Write-Host "─" * 60

Test-Lock -name "Get Lock Status for 'database'" -method "GET" -endpoint "/lock/status/database"

Write-Host "`n[TEST 3] Concurrent Lock Attempts" -ForegroundColor Yellow
Write-Host "─" * 60

# Test 3a: Try to get exclusive lock on same resource (should fail or wait)
Write-Host "`nAttempting second EXCLUSIVE lock (should fail)..."
$lock2_fail = Test-Lock -name "Try Exclusive Lock (should wait/timeout)" -method "POST" -endpoint "/lock/acquire" `
    -body @{
        resource = "database"
        lock_type = "exclusive"
        client_id = "client_2"
        timeout = 2
    }

Write-Host "`n[TEST 4] Shared Locks" -ForegroundColor Yellow
Write-Host "─" * 60

# Release first lock
if ($lock1.lock_id) {
    Write-Host "`nReleasing exclusive lock..."
    Test-Lock -name "Release Lock" -method "POST" -endpoint "/lock/release" `
        -body @{lock_id = $lock1.lock_id}
    $locks = @()
}

# Now try shared locks
Write-Host "`nAcquiring SHARED lock (read-only)..."
$lock_shared1 = Test-Lock -name "Acquire Shared Lock #1" -method "POST" -endpoint "/lock/acquire" `
    -body @{
        resource = "config"
        lock_type = "shared"
        client_id = "client_1"
        timeout = 30
    }

if ($lock_shared1.lock_id) {
    $locks += $lock_shared1.lock_id
}

Write-Host "`nAcquiring another SHARED lock on same resource..."
$lock_shared2 = Test-Lock -name "Acquire Shared Lock #2" -method "POST" -endpoint "/lock/acquire" `
    -body @{
        resource = "config"
        lock_type = "shared"
        client_id = "client_2"
        timeout = 30
    }

if ($lock_shared2.lock_id) {
    $locks += $lock_shared2.lock_id
}

Write-Host "`n[TEST 5] Multiple Resources" -ForegroundColor Yellow
Write-Host "─" * 60

# Lock different resources
Write-Host "`nLocking different resources..."
$lock_res1 = Test-Lock -name "Lock 'resource_A'" -method "POST" -endpoint "/lock/acquire" `
    -body @{
        resource = "resource_A"
        lock_type = "exclusive"
        client_id = "client_3"
        timeout = 30
    }

if ($lock_res1.lock_id) {
    $locks += $lock_res1.lock_id
}

$lock_res2 = Test-Lock -name "Lock 'resource_B'" -method "POST" -endpoint "/lock/acquire" `
    -body @{
        resource = "resource_B"
        lock_type = "exclusive"
        client_id = "client_3"
        timeout = 30
    }

if ($lock_res2.lock_id) {
    $locks += $lock_res2.lock_id
}

Write-Host "`n[TEST 6] Lock Cleanup" -ForegroundColor Yellow
Write-Host "─" * 60

foreach ($lockId in $locks) {
    Write-Host "`nReleasing lock: $lockId"
    Test-Lock -name "Release $lockId" -method "POST" -endpoint "/lock/release" `
        -body @{lock_id = $lockId}
}

Write-Host "`n[TEST 7] Lock Statistics" -ForegroundColor Yellow
Write-Host "─" * 60

$metrics = Test-Lock -name "Get Metrics" -method "GET" -endpoint "/metrics"

if ($metrics.locks) {
    Write-Host ""
    Write-Host "Lock Manager Statistics:" -ForegroundColor Cyan
    Write-Host "  Active Locks: $($metrics.locks.total_locks)" -ForegroundColor Green
    Write-Host "  Waiting Locks: $($metrics.locks.total_waiting)" -ForegroundColor Yellow
    Write-Host "  Protected Resources: $($metrics.locks.resources)" -ForegroundColor Green
}

Write-Host "`n[TEST 8] Stress Test - Multiple Locks" -ForegroundColor Yellow
Write-Host "─" * 60

Write-Host "`nAcquiring 10 locks rapidly..."
$stress_locks = @()
for ($i = 1; $i -le 10; $i++) {
    $lock = Test-Lock -name "Stress Lock #$i" -method "POST" -endpoint "/lock/acquire" `
        -body @{
            resource = "stress_resource_$i"
            lock_type = "exclusive"
            client_id = "stress_client_1"
            timeout = 5
        }
    
    if ($lock.lock_id) {
        $stress_locks += $lock.lock_id
    }
}

Write-Host "`nReleasing all stress locks..."
foreach ($lockId in $stress_locks) {
    Test-Lock -name "Release stress lock" -method "POST" -endpoint "/lock/release" `
        -body @{lock_id = $lockId} | Out-Null
}

Write-Host "`n╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║             Lock Manager Test Completed                    ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

Write-Host "`nTest finished at: $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Gray
