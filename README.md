# Job Queue System

A production-ready Job Queue System in Java with a command-line interface (`queuectl`) for scheduling, executing, retrying, and monitoring background jobs with persistence and multi-threaded worker execution.

## Features- 

- **Job Management**: Enqueue, execute, and monitor background jobs
- **Parallel Execution**: Multi-threaded worker system with configurable worker count
- **Automatic Retries**: Exponential backoff retry mechanism for failed jobs
- **Dead Letter Queue**: Handles permanently failed jobs with retry capabilities
- **Persistence**: JSON-based job storage that survives system restarts
- **CLI Interface**: Complete command-line tool for all operations
- **Thread Safety**: Concurrent job processing without duplication
- **Graceful Shutdown**: Workers complete ongoing jobs before termination
- **Configurable**: Runtime configuration management

## Requirements- 

- Java 17 or higher
- Maven 3.6 or higher

## Setup Instructions

### 1. Clone and Build

```bash
git clone <repository-url>
cd job-queue-system
mvn clean package
```

This creates an executable JAR file: `target/queuectl.jar`

### 2. Create Executable Script (Optional)

Create a script named `queuectl` for easier usage:

```bash
#!/bin/bash
java -jar /path/to/target/queuectl.jar "$@"
```

Make it executable:
```bash
chmod +x queuectl
```

### 3. Initialize Configuration

The system creates default configuration on first run:

```bash
java -jar target/queuectl.jar config show
```

## Usage Examples

### Basic Job Operations

#### Enqueue Jobs

```bash
# Simple command
java -jar target/queuectl.jar enqueue "echo 'Hello World'"

# JSON format with custom retries
java -jar target/queuectl.jar enqueue '{"command":"sleep 5","max_retries":2}'

# With custom retry count
java -jar target/queuectl.jar enqueue "ls -la" --max-retries 5
```

#### Start Workers

```bash
# Start with default worker count (from config)
java -jar target/queuectl.jar worker start

# Start with specific worker count
java -jar target/queuectl.jar worker start --count 5
```

#### Check System Status

```bash
java -jar target/queuectl.jar status
```

Output:
```
Job Queue System Status
=======================

Job Statistics:
  Pending:    3
  Processing: 1
  Completed:  15
  Failed:     2
  Dead:       1
  Total:      22

Worker Status:
  Running:        true
  Total Workers:  3
  Active Workers: 3

Queue Status:
  Pending in Queue: 3

Configuration:
  Config File:    config.json
  Data File:      jobs.json
  Max Retries:    3
  Backoff Base:   2
  Job Timeout:    300 seconds
```

### Job Listing and Monitoring

#### List All Jobs

```bash
java -jar target/queuectl.jar list
```

#### List Jobs by State

```bash
java -jar target/queuectl.jar list --state pending
java -jar target/queuectl.jar list --state failed
java -jar target/queuectl.jar list --state completed
```

#### Detailed Job Information

```bash
java -jar target/queuectl.jar list --verbose --limit 10
```

### Dead Letter Queue Management

#### List Dead Jobs

```bash
java -jar target/queuectl.jar dlq list
```

#### Retry Dead Jobs

```bash
# Retry specific job
java -jar target/queuectl.jar dlq retry job-id-123

# Retry all dead jobs
java -jar target/queuectl.jar dlq retry --all
```

#### DLQ Statistics

```bash
java -jar target/queuectl.jar dlq stats
```

### Configuration Management

#### View Configuration

```bash
java -jar target/queuectl.jar config show
```

#### Update Configuration

```bash
# Set maximum retries
java -jar target/queuectl.jar config set max-retries 5

# Set backoff base
java -jar target/queuectl.jar config set backoff-base 3

# Set worker count
java -jar target/queuectl.jar config set worker-count 4

# Set job timeout
java -jar target/queuectl.jar config set job-timeout 600
```

### Worker Management

#### Check Worker Status

```bash
java -jar target/queuectl.jar worker status
```

#### Stop Workers

```bash
java -jar target/queuectl.jar worker stop
```

##  Architecture Overview

### Job Lifecycle

1. **Enqueue**: Jobs are created with `PENDING` state and stored in persistence
2. **Processing**: Workers pick up jobs and mark them as `PROCESSING`
3. **Execution**: Shell commands are executed with timeout control
4. **Completion**: Successful jobs are marked as `COMPLETED`
5. **Failure**: Failed jobs are marked as `FAILED` with retry scheduling
6. **Retry**: Failed jobs are automatically retried with exponential backoff
7. **Dead Letter Queue**: Jobs exceeding max retries are moved to `DEAD` state

### Core Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CLI Interface │    │   Job Queue     │    │ Persistence     │
│   (queuectl)    │◄──►│   Manager       │◄──►│ Manager         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │                         │
                              ▼                         ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │ Worker Manager  │    │   jobs.json     │
                       │ (Thread Pool)   │    │   config.json   │
                       └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │ Job Executor    │
                       │ (Shell Commands)│
                       └─────────────────┘
```

### Persistence Layer

- **Format**: JSON files for human readability and easy debugging
- **Atomicity**: Temporary files with atomic moves prevent corruption
- **Thread Safety**: Read-write locks ensure concurrent access safety
- **Recovery**: Jobs persist across system restarts

### Worker System

- **Thread Pool**: Configurable number of worker threads
- **Job Polling**: Workers poll for available jobs with timeout
- **Graceful Shutdown**: Ongoing jobs complete before termination
- **Error Handling**: Robust exception handling with detailed logging

### Retry Mechanism

Exponential backoff formula: `delay = base^attempts seconds`

Example with base=2:
- Attempt 1: 2¹ = 2 seconds
- Attempt 2: 2² = 4 seconds  
- Attempt 3: 2³ = 8 seconds
- Maximum delay capped at 1 hour

## Configuration

Default configuration (`config.json`):

```json
{
  "max_retries": 3,
  "backoff_base": 2,
  "worker_count": 3,
  "data_file": "jobs.json",
  "job_timeout_seconds": 300,
  "retry_check_interval_seconds": 30
}
```

### Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `max_retries` | Maximum retry attempts for failed jobs | 3 |
| `backoff_base` | Base for exponential backoff calculation | 2 |
| `worker_count` | Number of worker threads | 3 |
| `data_file` | Path to job persistence file | jobs.json |
| `job_timeout_seconds` | Maximum execution time per job | 300 |
| `retry_check_interval_seconds` | Interval for retry processing | 30 |

##  Testing Steps

### 1. Basic Job Execution

```bash
# Start workers
java -jar target/queuectl.jar worker start --count 2 &

# Enqueue a simple job
java -jar target/queuectl.jar enqueue "echo 'Test successful'"

# Check status
java -jar target/queuectl.jar status

# List completed jobs
java -jar target/queuectl.jar list --state completed
```

### 2. Retry Mechanism Testing

```bash
# Enqueue a failing job
java -jar target/queuectl.jar enqueue "exit 1"

# Watch it retry (check logs or status)
java -jar target/queuectl.jar list --state failed

# Eventually moves to DLQ
java -jar target/queuectl.jar dlq list
```

### 3. Persistence Testing

```bash
# Enqueue jobs
java -jar target/queuectl.jar enqueue "sleep 10"
java -jar target/queuectl.jar enqueue "echo 'Persistent job'"

# Stop workers (Ctrl+C)
# Restart workers
java -jar target/queuectl.jar worker start

# Jobs should still be processed
```

### 4. Concurrent Processing

```bash
# Enqueue multiple jobs
for i in {1..10}; do
  java -jar target/queuectl.jar enqueue "echo 'Job $i' && sleep 2"
done

# Start multiple workers
java -jar target/queuectl.jar worker start --count 5

# Monitor parallel execution
java -jar target/queuectl.jar status
```

### 5. Invalid Command Handling

```bash
# Test invalid command
java -jar target/queuectl.jar enqueue "nonexistent-command"

# Should fail gracefully and retry
java -jar target/queuectl.jar list --state failed
```

## Project Structure

```
src/main/java/com/jobqueue/
├── cli/                    # Command-line interface
│   ├── QueueCtl.java      # Main CLI entry point
│   ├── EnqueueCommand.java
│   ├── WorkerCommand.java
│   ├── StatusCommand.java
│   ├── ListCommand.java
│   ├── DLQCommand.java
│   └── ConfigCommand.java
├── config/                 # Configuration management
│   ├── JobQueueConfig.java
│   └── ConfigManager.java
├── dlq/                    # Dead Letter Queue
│   └── DLQManager.java
├── model/                  # Data models
│   ├── Job.java
│   └── JobState.java
├── persistence/            # Data persistence
│   └── PersistenceManager.java
├── queue/                  # Job queue management
│   └── JobQueue.java
└── worker/                 # Worker system
    ├── JobExecutor.java
    ├── Worker.java
    └── WorkerManager.java
```

## Monitoring & Logging

### Log Files

- `logs/jobqueue.log`: General application logs
- `logs/job-execution.log`: Detailed job execution logs

### Log Levels

- **INFO**: Normal operations, job lifecycle events
- **WARN**: Retryable failures, configuration issues  
- **ERROR**: Permanent failures, system errors
- **DEBUG**: Detailed troubleshooting information

### Monitoring Commands

```bash
# Real-time status monitoring
watch -n 2 'java -jar target/queuectl.jar status'

# Log monitoring
tail -f logs/jobqueue.log
tail -f logs/job-execution.log
```

## Troubleshooting

### Common Issues

1. **Jobs Not Processing**
   ```bash
   # Check if workers are running
   java -jar target/queuectl.jar worker status
   
   # Start workers if needed
   java -jar target/queuectl.jar worker start
   ```

2. **Configuration Issues**
   ```bash
   # Reset to defaults
   rm config.json
   java -jar target/queuectl.jar config show
   ```

3. **Persistence Problems**
   ```bash
   # Check data file permissions
   ls -la jobs.json
   
   # Backup and reset if corrupted
   cp jobs.json jobs.json.backup
   echo "[]" > jobs.json
   ```

4. **Memory Issues**
   ```bash
   # Run with more memory
   java -Xmx2g -jar target/queuectl.jar worker start
   ```

##  Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

