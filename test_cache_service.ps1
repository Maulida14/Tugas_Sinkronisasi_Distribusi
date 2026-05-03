#!/usr/bin/env pwsh
# Test Cache Coherence Service (MOESI)
# Usage: .\test_cache_service.ps1

Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║    CACHE COHERENCE - MOESI Protocol Testing               ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

$baseUrl = "http://localhost:8001"
$cachedKeys = @()

function Test-Cache {
    param([string]$name, [string]$method, [string]$endpoint, [hashtable]$body = $null, [int]$expectedStatus = 200)
    
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
            return $response.Content | ConvertFrom-Json
        } else {
            Write-Host "✗ $name (expected $expectedStatus, got $($response.StatusCode))" -ForegroundColor Yellow
            return $null
        }
    } catch {
        if ($expectedStatus -eq 404 -and $_.Exception.Response.StatusCode -eq 404) {
            Write-Host "✓ $name (expected miss)" -ForegroundColor Green
            return $null
        }
        Write-Host "✗ $name - Error: $($_.Exception.Message)" -ForegroundColor Red
        return $null
    }
}

Write-Host "`n[TEST 1] Basic Cache Operations - PUT and GET" -ForegroundColor Yellow
Write-Host "─" * 60

# Put values
Write-Host "`nCaching values..."
$testData = @(
    @{key = "user:1"; value = "John Doe"; ttl = 3600}
    @{key = "user:2"; value = "Jane Smith"; ttl = 3600}
    @{key = "config:app"; value = @{version = "1.0"; env = "test"}; ttl = 7200}
    @{key = "session:abc123"; value = @{user_id = 1; login_time = (Get-Date).ToUniversalTime()}; ttl = 1800}
)

foreach ($data in $testData) {
    Test-Cache -name "Cache PUT $($data.key)" -method "POST" -endpoint "/cache/put" `
        -body @{
            key = $data.key
            value = $data.value
            ttl = $data.ttl
        } | Out-Null
    
    $cachedKeys += $data.key
}

# Get values
Write-Host "`nRetrieving cached values (HIT)..."
foreach ($key in $cachedKeys) {
    Test-Cache -name "Cache GET $key (HIT)" -method "GET" -endpoint "/cache/get/$key"
}

Write-Host "`n[TEST 2] Cache Misses" -ForegroundColor Yellow
Write-Host "─" * 60

Write-Host "`nTrying to get non-existent keys (MISS)..."
Test-Cache -name "Cache GET nonexistent (MISS)" -method "GET" -endpoint "/cache/get/nonexistent:key" -expectedStatus 404
Test-Cache -name "Cache GET missing (MISS)" -method "GET" -endpoint "/cache/get/missing:data" -expectedStatus 404

Write-Host "`n[TEST 3] Cache Invalidation" -ForegroundColor Yellow
Write-Host "─" * 60

Write-Host "`nCaching a key..."
Test-Cache -name "Cache PUT temp:data" -method "POST" -endpoint "/cache/put" `
    -body @{key = "temp:data"; value = "temporary"; ttl = 3600} | Out-Null

Write-Host "`nInvalidating the key..."
Test-Cache -name "Cache INVALIDATE temp:data" -method "POST" -endpoint "/cache/invalidate/temp:data"

Write-Host "`nTrying to get invalidated key (should be MISS)..."
Test-Cache -name "Cache GET temp:data (after invalidate)" -method "GET" -endpoint "/cache/get/temp:data" -expectedStatus 404

Write-Host "`n[TEST 4] TTL Expiration" -ForegroundColor Yellow
Write-Host "─" * 60

Write-Host "`nCaching data with 2 second TTL..."
Test-Cache -name "Cache PUT short:ttl (2 sec)" -method "POST" -endpoint "/cache/put" `
    -body @{key = "short:ttl"; value = "expires soon"; ttl = 2} | Out-Null

Write-Host "`nGetting data immediately (should HIT)..."
Test-Cache -name "Cache GET short:ttl (immediate)" -method "GET" -endpoint "/cache/get/short:ttl"

Write-Host "`nWaiting 3 seconds for TTL to expire..."
Start-Sleep -Seconds 3

Write-Host "`nGetting data after expiration (should be MISS)..."
Test-Cache -name "Cache GET short:ttl (after expiration)" -method "GET" -endpoint "/cache/get/short:ttl" -expectedStatus 404

Write-Host "`n[TEST 5] Different Data Types" -ForegroundColor Yellow
Write-Host "─" * 60

Write-Host "`nCaching different data types..."

# String
Test-Cache -name "Cache String" -method "POST" -endpoint "/cache/put" `
    -body @{key = "type:string"; value = "Hello World"; ttl = 3600} | Out-Null

# Number
Test-Cache -name "Cache Number" -method "POST" -endpoint "/cache/put" `
    -body @{key = "type:number"; value = 42; ttl = 3600} | Out-Null

# Boolean
Test-Cache -name "Cache Boolean" -method "POST" -endpoint "/cache/put" `
    -body @{key = "type:boolean"; value = $true; ttl = 3600} | Out-Null

# Object
Test-Cache -name "Cache Object" -method "POST" -endpoint "/cache/put" `
    -body @{
        key = "type:object"
        value = @{
            nested = @{
                level1 = "value1"
                level2 = 123
            }
            array = @(1, 2, 3)
        }
        ttl = 3600
    } | Out-Null

# Array
Test-Cache -name "Cache Array" -method "POST" -endpoint "/cache/put" `
    -body @{key = "type:array"; value = @("a", "b", "c", 1, 2, 3); ttl = 3600} | Out-Null

Write-Host "`nRetrieving different data types..."
Test-Cache -name "Cache GET String" -method "GET" -endpoint "/cache/get/type:string"
Test-Cache -name "Cache GET Number" -method "GET" -endpoint "/cache/get/type:number"
Test-Cache -name "Cache GET Boolean" -method "GET" -endpoint "/cache/get/type:boolean"
Test-Cache -name "Cache GET Object" -method "GET" -endpoint "/cache/get/type:object"
Test-Cache -name "Cache GET Array" -method "GET" -endpoint "/cache/get/type:array"

Write-Host "`n[TEST 6] Large Values" -ForegroundColor Yellow
Write-Host "─" * 60

Write-Host "`nCaching large value (10KB)..."
$largeValue = "x" * 10000
Test-Cache -name "Cache Large Value (10KB)" -method "POST" -endpoint "/cache/put" `
    -body @{key = "large:data"; value = $largeValue; ttl = 3600} | Out-Null

Write-Host "`nRetrieving large value..."
$retrieved = Test-Cache -name "Cache GET Large Value" -method "GET" -endpoint "/cache/get/large:data"

if ($retrieved) {
    $size = ($retrieved.value | ConvertTo-Json).Length
    Write-Host "  Retrieved size: ~$size bytes" -ForegroundColor Gray
}

Write-Host "`n[TEST 7] Cache Performance - Hit Rate" -ForegroundColor Yellow
Write-Host "─" * 60

Write-Host "`nPerforming repeated accesses to test hit rate..."
$metricsStart = Test-Cache -name "Get Metrics (before)" -method "GET" -endpoint "/metrics"

# Simulate repeated cache hits
for ($i = 0; $i -lt 20; $i++) {
    Test-Cache -name "Cache GET user:1 #$i" -method "GET" -endpoint "/cache/get/user:1" | Out-Null
}

$metricsEnd = Test-Cache -name "Get Metrics (after)" -method "GET" -endpoint "/metrics"

if ($metricsEnd.cache) {
    Write-Host ""
    Write-Host "Cache Hit Rate Statistics:" -ForegroundColor Cyan
    Write-Host "  Hit Rate: $($metricsEnd.cache.hit_rate)" -ForegroundColor Green
    Write-Host "  Cache Hits: $($metricsEnd.cache.hits)" -ForegroundColor Green
    Write-Host "  Cache Misses: $($metricsEnd.cache.misses)" -ForegroundColor Yellow
    Write-Host "  Cache Size: $($metricsEnd.cache.cache_size)/$($metricsEnd.cache.max_size)" -ForegroundColor Green
}

Write-Host "`n[TEST 8] MOESI State Transitions" -ForegroundColor Yellow
Write-Host "─" * 60

Write-Host "`nCaching data for MOESI state tracking..."
Test-Cache -name "Cache PUT moesi:test1 (Exclusive -> Modified)" -method "POST" -endpoint "/cache/put" `
    -body @{key = "moesi:test1"; value = "initial"; ttl = 3600} | Out-Null

Write-Host "`nReading the data (Exclusive state)..."
Test-Cache -name "Cache GET moesi:test1 (Read)" -method "GET" -endpoint "/cache/get/moesi:test1"

Write-Host "`nUpdating the data (stays Modified)..."
Test-Cache -name "Cache PUT moesi:test1 (Update)" -method "POST" -endpoint "/cache/put" `
    -body @{key = "moesi:test1"; value = "updated"; ttl = 3600} | Out-Null

Write-Host "`n[TEST 9] Stress Test - Many Keys" -ForegroundColor Yellow
Write-Host "─" * 60

Write-Host "`nCaching 100 keys..."
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

for ($i = 1; $i -le 100; $i++) {
    Test-Cache -name "Cache PUT stress:$i" -method "POST" -endpoint "/cache/put" `
        -body @{
            key = "stress:$i"
            value = "value_$i"
            ttl = 3600
        } | Out-Null
    
    if ($i % 25 -eq 0) {
        Write-Host "  $i/100 cached..." -ForegroundColor Gray
    }
}

$stopwatch.Stop()
Write-Host "  Completed in $($stopwatch.ElapsedMilliseconds)ms" -ForegroundColor Green

Write-Host "`nReading random keys..."
$reads = 0
for ($i = 1; $i -le 50; $i++) {
    $random = Get-Random -Minimum 1 -Maximum 101
    $result = Test-Cache -name "Cache GET stress:$random" -method "GET" -endpoint "/cache/get/stress:$random" | Out-Null
    if ($result) { $reads++ }
}

Write-Host "  Read $reads/50 keys successfully" -ForegroundColor Green

Write-Host "`n[TEST 10] Cache Invalidation Broadcast" -ForegroundColor Yellow
Write-Host "─" * 60

Write-Host "`nCaching shared data..."
Test-Cache -name "Cache PUT shared:resource" -method "POST" -endpoint "/cache/put" `
    -body @{key = "shared:resource"; value = "shared data"; ttl = 3600} | Out-Null

Write-Host "`nBroadcasting invalidation to all nodes..."
Test-Cache -name "Cache INVALIDATE shared:resource (broadcast)" -method "POST" -endpoint "/cache/invalidate/shared:resource"

Write-Host "`nVerifying key is invalidated (should be MISS)..."
Test-Cache -name "Cache GET shared:resource (after broadcast)" -method "GET" -endpoint "/cache/get/shared:resource" -expectedStatus 404

Write-Host "`n╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║         Cache Coherence Test Completed                    ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

$finalMetrics = Test-Cache -name "Get Final Metrics" -method "GET" -endpoint "/metrics"

if ($finalMetrics.cache) {
    Write-Host ""
    Write-Host "Final Cache Statistics:" -ForegroundColor Cyan
    Write-Host "  Current Cache Size: $($finalMetrics.cache.cache_size)/$($finalMetrics.cache.max_size)" -ForegroundColor Green
    Write-Host "  Total Hit Rate: $($finalMetrics.cache.hit_rate)" -ForegroundColor Green
    Write-Host "  Total Invalidations: $($finalMetrics.cache.invalidations)" -ForegroundColor Yellow
    Write-Host "  Pending Invalidations: $($finalMetrics.cache.pending_invalidations)" -ForegroundColor Gray
}

Write-Host "`nTest finished at: $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Gray
