#!/bin/pwsh
# PowerShell script untuk run Docker cluster dan test

Write-Host "Distributed Synchronization System - Setup Script" -ForegroundColor Green

# Check Docker
Write-Host "Checking Docker installation..." -ForegroundColor Yellow
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "Docker not found. Please install Docker Desktop." -ForegroundColor Red
    exit 1
}

# Build images
Write-Host "Building Docker images..." -ForegroundColor Yellow
docker compose -f docker/docker-compose.yml build

if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}

# Start cluster
Write-Host "Starting 4-node cluster..." -ForegroundColor Yellow
docker compose -f docker/docker-compose.yml up -d

# Wait for nodes to be ready
Write-Host "Waiting for nodes to be ready..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# Health check
Write-Host "Performing health checks..." -ForegroundColor Yellow
$nodes = @("8001", "8002", "8003", "8004")
$healthy = 0

foreach ($port in $nodes) {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:$port/health" -UseBasicParsing
        if ($response.StatusCode -eq 200) {
            Write-Host "Node on port $port is healthy" -ForegroundColor Green
            $healthy++
        }
    } catch {
        Write-Host "Node on port $port is not responding" -ForegroundColor Red
    }
}

Write-Host "`nCluster Status: $healthy/4 nodes healthy" -ForegroundColor Green

if ($healthy -lt 3) {
    Write-Host "Warning: Less than 3 nodes healthy!" -ForegroundColor Yellow
}

Write-Host "`nCluster is running. Access nodes at:" -ForegroundColor Green
Write-Host "  Node 1: http://localhost:8001"
Write-Host "  Node 2: http://localhost:8002"
Write-Host "  Node 3: http://localhost:8003"
Write-Host "  Node 4: http://localhost:8004"
Write-Host "`nto stop cluster: docker compose -f docker/docker-compose.yml down"
