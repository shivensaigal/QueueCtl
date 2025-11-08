package com.jobqueue.worker;

import com.jobqueue.model.Job;
import com.jobqueue.queue.JobQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Individual worker that processes jobs from the queue.
 * Runs in its own thread and handles job execution.
 */
public class Worker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);
    
    private final String workerId;
    private final JobQueue jobQueue;
    private final JobExecutor jobExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    
    public Worker(String workerId, JobQueue jobQueue, JobExecutor jobExecutor) {
        this.workerId = workerId;
        this.jobQueue = jobQueue;
        this.jobExecutor = jobExecutor;
    }
    
    @Override
    public void run() {
        running.set(true);
        logger.info("Worker {} started", workerId);
        
        try {
            while (!shutdown.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    processNextJob();
                } catch (Exception e) {
                    logger.error("Worker {} encountered error while processing job", workerId, e);
                    // Continue processing other jobs
                }
            }
        } finally {
            running.set(false);
            logger.info("Worker {} stopped", workerId);
        }
    }
    
    /**
     * Process the next available job from the queue
     */
    private void processNextJob() {
        // Try to get a job with a reasonable timeout
        Optional<Job> jobOpt = jobQueue.dequeue(5, TimeUnit.SECONDS);
        
        if (jobOpt.isPresent()) {
            Job job = jobOpt.get();
            processJob(job);
        }
        // If no job available, the loop will continue and try again
    }
    
    /**
     * Process a specific job
     */
    private void processJob(Job job) {
        logger.info("Worker {} processing job {}: {}", workerId, job.getId(), job.getCommand());
        
        try {
            // Execute the job
            JobExecutor.JobExecutionResult result = jobExecutor.execute(job);
            
            if (result.isSuccess()) {
                jobQueue.markCompleted(job);
                logger.info("Worker {} completed job {}", workerId, job.getId());
                
                // Log output if present
                if (!result.getOutput().isEmpty()) {
                    logger.info("Job {} output: {}", job.getId(), result.getOutput());
                }
            } else {
                jobQueue.markFailed(job, result.getErrorMessage());
                logger.warn("Worker {} failed job {}: {}", workerId, job.getId(), result.getErrorMessage());
                
                // Log output if present (might contain error details)
                if (!result.getOutput().isEmpty()) {
                    logger.warn("Job {} output: {}", job.getId(), result.getOutput());
                }
            }
            
        } catch (Exception e) {
            String errorMessage = "Worker exception: " + e.getMessage();
            jobQueue.markFailed(job, errorMessage);
            logger.error("Worker {} failed job {} due to exception", workerId, job.getId(), e);
        }
    }
    
    /**
     * Request the worker to shutdown gracefully
     */
    public void shutdown() {
        logger.info("Worker {} shutdown requested", workerId);
        shutdown.set(true);
    }
    
    /**
     * Check if the worker is currently running
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * Check if shutdown has been requested
     */
    public boolean isShutdownRequested() {
        return shutdown.get();
    }
    
    /**
     * Get the worker ID
     */
    public String getWorkerId() {
        return workerId;
    }
    
    @Override
    public String toString() {
        return String.format("Worker{id='%s', running=%s, shutdown=%s}", 
                           workerId, running.get(), shutdown.get());
    }
}
