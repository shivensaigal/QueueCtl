package com.jobqueue.worker;

import com.jobqueue.config.JobQueueConfig;
import com.jobqueue.queue.JobQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages multiple worker threads for job processing.
 * Handles worker lifecycle, graceful shutdown, and retry processing.
 */
public class WorkerManager {
    private static final Logger logger = LoggerFactory.getLogger(WorkerManager.class);
    
    private final JobQueue jobQueue;
    private final JobQueueConfig config;
    private final ExecutorService workerExecutor;
    private final ScheduledExecutorService retryScheduler;
    private final List<Worker> workers = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger workerIdCounter = new AtomicInteger(0);
    
    public WorkerManager(JobQueue jobQueue, JobQueueConfig config) {
        this.jobQueue = jobQueue;
        this.config = config;
        this.workerExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "JobWorker-" + workerIdCounter.incrementAndGet());
            t.setDaemon(false); // Ensure workers complete before JVM shutdown
            return t;
        });
        this.retryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "RetryScheduler");
            t.setDaemon(true);
            return t;
        });
        
        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }
    
    /**
     * Start the worker manager with configured number of workers
     */
    public void start() {
        start(config.getWorkerCount());
    }
    
    /**
     * Start the worker manager with specified number of workers
     */
    public void start(int workerCount) {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting WorkerManager with {} workers", workerCount);
            
            // Initialize job queue
            jobQueue.initialize();
            
            // Create and start workers
            JobExecutor jobExecutor = new JobExecutor(config.getJobTimeoutSeconds());
            
            for (int i = 0; i < workerCount; i++) {
                String workerId = "worker-" + (i + 1);
                Worker worker = new Worker(workerId, jobQueue, jobExecutor);
                workers.add(worker);
                workerExecutor.submit(worker);
            }
            
            // Start retry scheduler
            startRetryScheduler();
            
            logger.info("WorkerManager started with {} workers", workers.size());
        } else {
            logger.warn("WorkerManager is already running");
        }
    }
    
    /**
     * Stop all workers gracefully
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping WorkerManager...");
            
            // Request shutdown for all workers
            for (Worker worker : workers) {
                worker.shutdown();
            }
            
            // Shutdown executor services
            shutdownExecutorService(workerExecutor, "WorkerExecutor", 30);
            shutdownExecutorService(retryScheduler, "RetryScheduler", 5);
            
            workers.clear();
            logger.info("WorkerManager stopped");
        } else {
            logger.warn("WorkerManager is not running");
        }
    }
    
    /**
     * Shutdown the worker manager (called by shutdown hook)
     */
    public void shutdown() {
        if (running.get()) {
            logger.info("Graceful shutdown initiated...");
            stop();
        }
    }
    
    /**
     * Check if the worker manager is running
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * Get the number of active workers
     */
    public int getActiveWorkerCount() {
        return (int) workers.stream().filter(Worker::isRunning).count();
    }
    
    /**
     * Get the total number of workers
     */
    public int getTotalWorkerCount() {
        return workers.size();
    }
    
    /**
     * Get worker status information
     */
    public List<WorkerStatus> getWorkerStatus() {
        List<WorkerStatus> statusList = new ArrayList<>();
        for (Worker worker : workers) {
            statusList.add(new WorkerStatus(
                worker.getWorkerId(),
                worker.isRunning(),
                worker.isShutdownRequested()
            ));
        }
        return statusList;
    }
    
    /**
     * Add more workers at runtime
     */
    public void addWorkers(int count) {
        if (!running.get()) {
            throw new IllegalStateException("WorkerManager is not running");
        }
        
        logger.info("Adding {} workers", count);
        JobExecutor jobExecutor = new JobExecutor(config.getJobTimeoutSeconds());
        
        for (int i = 0; i < count; i++) {
            String workerId = "worker-" + workerIdCounter.incrementAndGet();
            Worker worker = new Worker(workerId, jobQueue, jobExecutor);
            workers.add(worker);
            workerExecutor.submit(worker);
        }
        
        logger.info("Added {} workers, total workers: {}", count, workers.size());
    }
    
    /**
     * Start the retry scheduler that periodically processes failed jobs for retry
     */
    private void startRetryScheduler() {
        retryScheduler.scheduleWithFixedDelay(
            this::processRetries,
            config.getRetryCheckIntervalSeconds(),
            config.getRetryCheckIntervalSeconds(),
            TimeUnit.SECONDS
        );
        
        logger.info("Retry scheduler started with interval {} seconds", 
                   config.getRetryCheckIntervalSeconds());
    }
    
    /**
     * Process jobs that are ready for retry
     */
    private void processRetries() {
        try {
            int retriedCount = jobQueue.processRetries();
            if (retriedCount > 0) {
                logger.debug("Processed {} jobs for retry", retriedCount);
            }
        } catch (Exception e) {
            logger.error("Error processing retries", e);
        }
    }
    
    /**
     * Shutdown an executor service gracefully
     */
    private void shutdownExecutorService(ExecutorService executor, String name, int timeoutSeconds) {
        logger.info("Shutting down {}...", name);
        executor.shutdown();
        
        try {
            if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                logger.warn("{} did not terminate gracefully, forcing shutdown", name);
                executor.shutdownNow();
                
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.error("{} did not terminate after forced shutdown", name);
                }
            } else {
                logger.info("{} shut down gracefully", name);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("{} shutdown interrupted, forcing shutdown", name);
            executor.shutdownNow();
        }
    }
    
    /**
     * Worker status information
     */
    public static class WorkerStatus {
        private final String workerId;
        private final boolean running;
        private final boolean shutdownRequested;
        
        public WorkerStatus(String workerId, boolean running, boolean shutdownRequested) {
            this.workerId = workerId;
            this.running = running;
            this.shutdownRequested = shutdownRequested;
        }
        
        public String getWorkerId() {
            return workerId;
        }
        
        public boolean isRunning() {
            return running;
        }
        
        public boolean isShutdownRequested() {
            return shutdownRequested;
        }
        
        @Override
        public String toString() {
            return String.format("WorkerStatus{workerId='%s', running=%s, shutdownRequested=%s}", 
                               workerId, running, shutdownRequested);
        }
    }
}
