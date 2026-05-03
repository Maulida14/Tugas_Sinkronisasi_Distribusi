#!/usr/bin/env pwsh
# Run All Tests - Master Test Script
# Usage: .\run_all_tests.ps1

Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   Distributed System - Complete Test Suite               ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

$testScripts = @(
    @{name="Raft Consensus"; file="test_raft_consensus.ps1"}
    @{name="Lock Manager"; file="test_lock_manager.ps1"}
    @{name="Queue Service"; file="test_queue_service.ps1"}
    @{name="Cache Service"; file="test_cache_service.ps1"}
)

# Check if docker is running
Write-Host "`nChecking prerequisites..." -ForegroundColor Yellow

try {
    $health = Invoke-WebRequest -Uri "http://localhost:8001/health" -UseBasicParsing -TimeoutSec 2
    Write-Host "✓ Docker cluster is running" -ForegroundColor Green
} catch {
    Write-Host "✗ Docker cluster is not running" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please start the cluster with:" -ForegroundColor Yellow
    Write-Host "  docker compose -f docker/docker-compose.yml up -d" -ForegroundColor White
    exit 1
}

# Menu
Write-Host "`n╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                    TEST MENU                              ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

Write-Host ""
Write-Host "[1] Raft Consensus" -ForegroundColor Green
Write-Host "[2] Lock Manager" -ForegroundColor Green
Write-Host "[3] Queue Service" -ForegroundColor Green
Write-Host "[4] Cache Service" -ForegroundColor Green
Write-Host "[5] All Tests (Sequential)" -ForegroundColor Cyan
Write-Host "[0] Exit" -ForegroundColor Red
Write-Host ""

$choice = Read-Host "Select test (0-5)"

function Run-Test {
    param([string]$scriptFile, [string]$testName)
    
    Write-Host "`n" + ("=" * 65) -ForegroundColor Cyan
    Write-Host "Running: $testName" -ForegroundColor Cyan
    Write-Host ("=" * 65) -ForegroundColor Cyan
    
    if (Test-Path $scriptFile) {
        & $scriptFile
    } else {
        Write-Host "Error: $scriptFile not found" -ForegroundColor Red
    }
    
    Write-Host "`nTest completed. Press Enter to continue..." -ForegroundColor Yellow
    Read-Host
}

$startTime = Get-Date

switch($choice) {
    "1" {
        Run-Test ".\test_raft_consensus.ps1" "Raft Consensus"
    }
    "2" {
        Run-Test ".\test_lock_manager.ps1" "Lock Manager"
    }
    "3" {
        Run-Test ".\test_queue_service.ps1" "Queue Service"
    }
    "4" {
        Run-Test ".\test_cache_service.ps1" "Cache Service"
    }
    "5" {
        foreach ($test in $testScripts) {
            Run-Test ".\$($test.file)" $test.name
            Write-Host ""
        }
    }
    "0" {
        Write-Host "Exiting..." -ForegroundColor Yellow
        exit 0
    }
    default {
        Write-Host "Invalid choice" -ForegroundColor Red
        exit 1
    }
}

$endTime = Get-Date
$duration = ($endTime - $startTime).TotalSeconds

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                    TEST SUMMARY                           ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

Write-Host "`nTest Duration: $([math]::Round($duration, 2)) seconds" -ForegroundColor Green
Write-Host "Completed at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray
