#!/usr/bin/env pwsh
# Test Distributed Queue Service
# Usage: .\test_queue_service.ps1

Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║     DISTRIBUTED QUEUE - Message Queue Testing             ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

$baseUrl = "http://localhost:8001"
$publishedMessages = @()

function Test-Queue {
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

Write-Host "`n[TEST 1] Basic Message Publishing" -ForegroundColor Yellow
Write-Host "─" * 60

# Publish messages to topic
for ($i = 1; $i -le 5; $i++) {
    Write-Host "`nPublishing message #$i..."
    $msg = Test-Queue -name "Publish Message #$i to 'user_events'" -method "POST" -endpoint "/queue/publish" `
        -body @{
            topic = "user_events"
            payload = @{
                id = $i
                event = "user_action"
                timestamp = (Get-Date).ToUniversalTime().ToString("o")
                data = "Event data #$i"
            }
        }
    
    if ($msg.msg_id) {
        Write-Host "  Message ID: $($msg.msg_id)" -ForegroundColor Green
        $publishedMessages += @{msg_id = $msg.msg_id; topic = "user_events"}
    }
}

Write-Host "`n[TEST 2] Multiple Topics" -ForegroundColor Yellow
Write-Host "─" * 60

$topics = @("orders", "notifications", "payments")
foreach ($topic in $topics) {
    Write-Host "`nPublishing to topic: $topic"
    $msg = Test-Queue -name "Publish to '$topic'" -method "POST" -endpoint "/queue/publish" `
        -body @{
            topic = $topic
            payload = @{
                id = 1
                topic_name = $topic
                data = "Test message for $topic"
            }
        }
    
    if ($msg.msg_id) {
        $publishedMessages += @{msg_id = $msg.msg_id; topic = $topic}
    }
}

Write-Host "`n[TEST 3] Consume Messages - Single Consumer" -ForegroundColor Yellow
Write-Host "─" * 60

Write-Host "`nConsumer #1 consuming from 'user_events' (batch size = 3)..."
$consumed1 = Test-Queue -name "Consume Messages (batch=3)" -method "POST" -endpoint "/queue/consume" `
    -body @{
        topic = "user_events"
        consumer_id = "consumer_1"
        batch_size = 3
    }

if ($consumed1.messages) {
    Write-Host "  Received $($consumed1.messages.Count) messages" -ForegroundColor Green
    $consumed1.messages | ForEach-Object {
        Write-Host "    - Message: $($_.msg_id) (delivery count: $($_.delivery_count))" -ForegroundColor Gray
    }
}

Write-Host "`n[TEST 4] Acknowledge Messages" -ForegroundColor Yellow
Write-Host "─" * 60

if ($consumed1.messages -and $consumed1.messages.Count -gt 0) {
    Write-Host "`nAcknowledging first message..."
    $firstMsg = $consumed1.messages[0]
    
    $ack = Test-Queue -name "Acknowledge Message" -method "POST" -endpoint "/queue/ack" `
        -body @{
            msg_id = $firstMsg.msg_id
            consumer_id = "consumer_1"
        }
}

Write-Host "`n[TEST 5] Multiple Consumers - Same Topic" -ForegroundColor Yellow
Write-Host "─" * 60

Write-Host "`nPublishing 10 messages to 'distributed_topic'..."
for ($i = 1; $i -le 10; $i++) {
    $msg = Test-Queue -name "Publish #$i" -method "POST" -endpoint "/queue/publish" `
        -body @{
            topic = "distributed_topic"
            payload = @{id = $i; message = "Message $i"}
        } | Out-Null
}

Write-Host "`nConsumer #1 consuming (batch=3)..."
$cons_a = Test-Queue -name "Consumer #1 Consume" -method "POST" -endpoint "/queue/consume" `
    -body @{
        topic = "distributed_topic"
        consumer_id = "consumer_1"
        batch_size = 3
    }

if ($cons_a.messages) {
    Write-Host "  Consumer #1 got $($cons_a.messages.Count) messages" -ForegroundColor Green
}

Write-Host "`nConsumer #2 consuming (batch=3)..."
$cons_b = Test-Queue -name "Consumer #2 Consume" -method "POST" -endpoint "/queue/consume" `
    -body @{
        topic = "distributed_topic"
        consumer_id = "consumer_2"
        batch_size = 3
    }

if ($cons_b.messages) {
    Write-Host "  Consumer #2 got $($cons_b.messages.Count) messages" -ForegroundColor Green
}

Write-Host "`n[TEST 6] Publish Large Payload" -ForegroundColor Yellow
Write-Host "─" * 60

$largePayload = @{
    id = 999
    large_text = ("x" * 1000)
    nested_object = @{
        level1 = @{
            level2 = @{
                level3 = "Deep nested value"
            }
        }
    }
}

Test-Queue -name "Publish Large Message" -method "POST" -endpoint "/queue/publish" `
    -body @{
        topic = "large_messages"
        payload = $largePayload
    } | Out-Null

Write-Host "`n[TEST 7] Queue Depth Check" -ForegroundColor Yellow
Write-Host "─" * 60

# Publish some messages
for ($i = 1; $i -le 3; $i++) {
    Test-Queue -name "Publish" -method "POST" -endpoint "/queue/publish" `
        -body @{
            topic = "depth_test"
            payload = @{id = $i}
        } | Out-Null
}

$metrics = Test-Queue -name "Get Queue Metrics" -method "GET" -endpoint "/metrics"

if ($metrics.queue) {
    Write-Host ""
    Write-Host "Queue Statistics:" -ForegroundColor Cyan
    Write-Host "  Total Partitions: $($metrics.queue.total_partitions)" -ForegroundColor Green
    Write-Host "  Total Messages: $($metrics.queue.total_messages)" -ForegroundColor Green
    Write-Host "  Unacked Messages: $($metrics.queue.unacked_messages)" -ForegroundColor Yellow
    Write-Host "  Pending Persistence: $($metrics.queue.pending_persistence)" -ForegroundColor Gray
}

Write-Host "`n[TEST 8] Stress Test - High Throughput" -ForegroundColor Yellow
Write-Host "─" * 60

Write-Host "`nPublishing 50 messages rapidly..."
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

for ($i = 1; $i -le 50; $i++) {
    Test-Queue -name "Stress Publish #$i" -method "POST" -endpoint "/queue/publish" `
        -body @{
            topic = "stress_topic"
            payload = @{id = $i; seq = $i; timestamp = (Get-Date).ToUniversalTime()}
        } | Out-Null
    
    if ($i % 10 -eq 0) {
        Write-Host "  $i/50 published..." -ForegroundColor Gray
    }
}

$stopwatch.Stop()
Write-Host "  Completed in $($stopwatch.ElapsedMilliseconds)ms" -ForegroundColor Green

Write-Host "`nConsuming from stress topic..."
$stress_consume = Test-Queue -name "Stress Consume (batch=20)" -method "POST" -endpoint "/queue/consume" `
    -body @{
        topic = "stress_topic"
        consumer_id = "stress_consumer"
        batch_size = 20
    }

if ($stress_consume.messages) {
    Write-Host "  Consumed $($stress_consume.messages.Count) messages" -ForegroundColor Green
}

Write-Host "`n[TEST 9] Different Message Formats" -ForegroundColor Yellow
Write-Host "─" * 60

# String payload
Test-Queue -name "Publish String Payload" -method "POST" -endpoint "/queue/publish" `
    -body @{
        topic = "format_test"
        payload = "Simple string message"
    } | Out-Null

# Number payload
Test-Queue -name "Publish Number Payload" -method "POST" -endpoint "/queue/publish" `
    -body @{
        topic = "format_test"
        payload = 12345
    } | Out-Null

# Array payload
Test-Queue -name "Publish Array Payload" -method "POST" -endpoint "/queue/publish" `
    -body @{
        topic = "format_test"
        payload = @(1, 2, 3, 4, 5)
    } | Out-Null

Write-Host "`n╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║          Distributed Queue Test Completed                 ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

Write-Host "`nTotal messages published: $($publishedMessages.Count)" -ForegroundColor Green
Write-Host "Test finished at: $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Gray
