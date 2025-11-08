package com.jobqueue.queue;

import com.jobqueue.config.JobQueueConfig;
import com.jobqueue.model.Job;
import com.jobqueue.model.JobState;
import com.jobqueue.persistence.PersistenceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread-safe job queue that manages job lifecycle and persistence.
 * Handles job enqueuing, dequeuing, retries, and dead letter queue management.
 */
public class JobQueue {
    private static final Logger logger = LoggerFactory.getLogger(JobQueue.class);
    
    private final PersistenceManager persistenceManager;
    private final JobQueueConfig config;
    private final BlockingQueue<Job> pendingJobs = new LinkedBlockingQueue<>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    
    public JobQueue(PersistenceManager persistenceManager, JobQueueConfig config) {
        this.persistenceManager = persistenceManager;
        this.config = config;
    }
    
    /**
     * Initialize the queue by loading pending jobs from persistence
     */
    public void initialize() {
        if (initialized.compareAndSet(false, true)) {
            loadPendingJobs();
            logger.info("JobQueue initialized with {} pending jobs", pendingJobs.size());
        }
    }
    
    /**
     * Enqueue a new job
     */
    public String enqueue(String command) {
        return enqueue(command, config.getMaxRetries());
    }
    
    /**
     * Enqueue a new job with custom max retries
     */
    public String enqueue(String command, int maxRetries) {
        Job job = new Job(command, maxRetries);
        return enqueue(job);
    }
    
    /**
     * Enqueue a job object
     */
    public String enqueue(Job job) {
        if (!initialized.get()) {
            initialize();
        }
        
        // Save to persistence first
        persistenceManager.saveJob(job);
        
        // Add to pending queue if it's in pending state
        if (job.getState() == JobState.PENDING) {
            pendingJobs.offer(job);
            logger.info("Job enqueued: {} - {}", job.getId(), job.getCommand());
        }
        
        return job.getId();
    }
    
    /**
     * Dequeue the next available job for processing
     */
    public Optional<Job> dequeue() {
        return dequeue(1, TimeUnit.SECONDS);
    }
    
    /**
     * Dequeue the next available job with timeout
     */
    public Optional<Job> dequeue(long timeout, TimeUnit unit) {
        if (!initialized.get()) {
            initialize();
        }
        
        try {
            Job job = pendingJobs.poll(timeout, unit);
            if (job != null) {
                // Mark as processing and save
                job.markAsProcessing();
                persistenceManager.saveJob(job);
                logger.debug("Job dequeued for processing: {}", job.getId());
                return Optional.of(job);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("Dequeue interrupted");
        }
        
        return Optional.empty();
    }
    
    /**
     * Mark a job as completed
     */
    public void markCompleted(Job job) {
        job.markAsCompleted();
        persistenceManager.saveJob(job);
        logger.info("Job completed: {} - {}", job.getId(), job.getCommand());
    }
    
    /**
     * Mark a job as failed and handle retry logic
     */
    public void markFailed(Job job, String errorMessage) {
        if (job.canRetry()) {
            // Calculate next retry time using exponential backoff
            LocalDateTime nextRetryAt = calculateNextRetryTime(job.getAttempts());
            job.markAsFailed(errorMessage, nextRetryAt);
            persistenceManager.saveJob(job);
            
            logger.warn("Job failed (attempt {}/{}): {} - {} - Next retry at: {}", 
                       job.getAttempts(), job.getMaxRetries(), job.getId(), 
                       job.getCommand(), nextRetryAt);
        } else {
            // Move to dead letter queue
            job.markAsDead(errorMessage);
            persistenceManager.saveJob(job);
            
            logger.error("Job moved to DLQ after {} attempts: {} - {} - Error: {}", 
                        job.getAttempts(), job.getId(), job.getCommand(), errorMessage);
        }
    }
    
    /**
     * Process retry jobs that are ready for retry
     */
    public int processRetries() {
        List<Job> retryJobs = persistenceManager.getJobsReadyForRetry();
        
        for (Job job : retryJobs) {
            job.resetForRetry();
            persistenceManager.saveJob(job);
            pendingJobs.offer(job);
            logger.info("Job requeued for retry: {} (attempt {}/{})", 
                       job.getId(), job.getAttempts() + 1, job.getMaxRetries());
        }
        
        if (!retryJobs.isEmpty()) {
            logger.info("Processed {} jobs for retry", retryJobs.size());
        }
        
        return retryJobs.size();
    }
    
    /**
     * Get job by ID
     */
    public Optional<Job> getJob(String jobId) {
        return persistenceManager.getJob(jobId);
    }
    
    /**
     * Get all jobs
     */
    public List<Job> getAllJobs() {
        return persistenceManager.getAllJobs();
    }
    
    /**
     * Get jobs by state
     */
    public List<Job> getJobsByState(JobState state) {
        return persistenceManager.getJobsByState(state);
    }
    
    /**
     * Get job statistics
     */
    public Map<JobState, Long> getStatistics() {
        return persistenceManager.getJobStatistics();
    }
    
    /**
     * Get the number of pending jobs in the queue
     */
    public int getPendingCount() {
        return pendingJobs.size();
    }
    
    /**
     * Get the total number of jobs
     */
    public int getTotalJobCount() {
        return persistenceManager.getJobCount();
    }
    
    /**
     * Clear all completed jobs
     */
    public int clearCompletedJobs() {
        return persistenceManager.deleteJobsByState(JobState.COMPLETED);
    }
    
    /**
     * Retry a job from the dead letter queue
     */
    public boolean retryDeadJob(String jobId) {
        Optional<Job> jobOpt = persistenceManager.getJob(jobId);
        if (jobOpt.isPresent()) {
            Job job = jobOpt.get();
            if (job.getState() == JobState.DEAD) {
                // Reset the job for retry
                Job retryJob = new Job(job.getId(), job.getCommand(), job.getMaxRetries());
                return enqueue(retryJob) != null;
            }
        }
        return false;
    }
    
    /**
     * Delete a job
     */
    public boolean deleteJob(String jobId) {
        // Remove from pending queue if present
        pendingJobs.removeIf(job -> job.getId().equals(jobId));
        
        // Remove from persistence
        return persistenceManager.deleteJob(jobId);
    }
    
    /**
     * Shutdown the queue gracefully
     */
    public void shutdown() {
        logger.info("JobQueue shutting down...");
        // The queue itself doesn't need special shutdown handling
        // Workers will handle their own shutdown
    }
    
    /**
     * Calculate next retry time using exponential backoff
     */
    private LocalDateTime calculateNextRetryTime(int attempts) {
        // delay = base ^ attempts seconds
        long delaySeconds = (long) Math.pow(config.getBackoffBase(), attempts);
        
        // Cap the delay to prevent extremely long waits
        delaySeconds = Math.min(delaySeconds, 3600); // Max 1 hour
        
        return LocalDateTime.now().plusSeconds(delaySeconds);
    }
    
    /**
     * Load pending jobs from persistence into the queue
     */
    private void loadPendingJobs() {
        List<Job> pendingJobs = persistenceManager.getPendingJobs();
        for (Job job : pendingJobs) {
            this.pendingJobs.offer(job);
        }
        
        if (!pendingJobs.isEmpty()) {
            logger.info("Loaded {} pending jobs from persistence", pendingJobs.size());
        }
    }
}
