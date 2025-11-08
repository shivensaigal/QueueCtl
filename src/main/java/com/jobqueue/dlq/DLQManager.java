package com.jobqueue.dlq;

import com.jobqueue.model.Job;
import com.jobqueue.model.JobState;
import com.jobqueue.persistence.PersistenceManager;
import com.jobqueue.queue.JobQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Manages the Dead Letter Queue (DLQ) for jobs that have permanently failed.
 * Provides operations to view, retry, and clean up dead jobs.
 */
public class DLQManager {
    private static final Logger logger = LoggerFactory.getLogger(DLQManager.class);
    
    private final PersistenceManager persistenceManager;
    private final JobQueue jobQueue;
    
    public DLQManager(PersistenceManager persistenceManager, JobQueue jobQueue) {
        this.persistenceManager = persistenceManager;
        this.jobQueue = jobQueue;
    }
    
    /**
     * Get all jobs in the Dead Letter Queue
     */
    public List<Job> getDeadJobs() {
        return persistenceManager.getDeadJobs();
    }
    
    /**
     * Get dead jobs with pagination
     */
    public List<Job> getDeadJobs(int offset, int limit) {
        List<Job> allDeadJobs = getDeadJobs();
        
        int start = Math.min(offset, allDeadJobs.size());
        int end = Math.min(start + limit, allDeadJobs.size());
        
        return allDeadJobs.subList(start, end);
    }
    
    /**
     * Get a specific dead job by ID
     */
    public Optional<Job> getDeadJob(String jobId) {
        Optional<Job> jobOpt = persistenceManager.getJob(jobId);
        if (jobOpt.isPresent() && jobOpt.get().getState() == JobState.DEAD) {
            return jobOpt;
        }
        return Optional.empty();
    }
    
    /**
     * Get dead jobs filtered by error message pattern
     */
    public List<Job> getDeadJobsByError(String errorPattern) {
        return getDeadJobs().stream()
                .filter(job -> job.getErrorMessage() != null && 
                              job.getErrorMessage().toLowerCase().contains(errorPattern.toLowerCase()))
                .collect(Collectors.toList());
    }
    
    /**
     * Get dead jobs within a time range
     */
    public List<Job> getDeadJobsByTimeRange(LocalDateTime startTime, LocalDateTime endTime) {
        return getDeadJobs().stream()
                .filter(job -> {
                    LocalDateTime updatedAt = job.getUpdatedAt();
                    return updatedAt.isAfter(startTime) && updatedAt.isBefore(endTime);
                })
                .collect(Collectors.toList());
    }
    
    /**
     * Retry a specific dead job
     */
    public boolean retryDeadJob(String jobId) {
        Optional<Job> jobOpt = getDeadJob(jobId);
        if (jobOpt.isPresent()) {
            Job deadJob = jobOpt.get();
            
            // Create a new job with the same command but reset state
            Job retryJob = new Job(deadJob.getCommand(), deadJob.getMaxRetries());
            
            // Enqueue the new job
            String newJobId = jobQueue.enqueue(retryJob);
            
            logger.info("Dead job {} retried as new job {}: {}", 
                       jobId, newJobId, deadJob.getCommand());
            
            return true;
        }
        
        logger.warn("Cannot retry job {}: not found in DLQ", jobId);
        return false;
    }
    
    /**
     * Retry multiple dead jobs
     */
    public int retryDeadJobs(List<String> jobIds) {
        int retriedCount = 0;
        
        for (String jobId : jobIds) {
            if (retryDeadJob(jobId)) {
                retriedCount++;
            }
        }
        
        logger.info("Retried {} out of {} dead jobs", retriedCount, jobIds.size());
        return retriedCount;
    }
    
    /**
     * Retry all dead jobs
     */
    public int retryAllDeadJobs() {
        List<Job> deadJobs = getDeadJobs();
        List<String> jobIds = deadJobs.stream()
                .map(Job::getId)
                .collect(Collectors.toList());
        
        return retryDeadJobs(jobIds);
    }
    
    /**
     * Retry dead jobs matching an error pattern
     */
    public int retryDeadJobsByError(String errorPattern) {
        List<Job> matchingJobs = getDeadJobsByError(errorPattern);
        List<String> jobIds = matchingJobs.stream()
                .map(Job::getId)
                .collect(Collectors.toList());
        
        return retryDeadJobs(jobIds);
    }
    
    /**
     * Delete a specific dead job permanently
     */
    public boolean deleteDeadJob(String jobId) {
        Optional<Job> jobOpt = getDeadJob(jobId);
        if (jobOpt.isPresent()) {
            boolean deleted = persistenceManager.deleteJob(jobId);
            if (deleted) {
                logger.info("Dead job {} deleted permanently", jobId);
            }
            return deleted;
        }
        
        logger.warn("Cannot delete job {}: not found in DLQ", jobId);
        return false;
    }
    
    /**
     * Delete multiple dead jobs permanently
     */
    public int deleteDeadJobs(List<String> jobIds) {
        int deletedCount = 0;
        
        for (String jobId : jobIds) {
            if (deleteDeadJob(jobId)) {
                deletedCount++;
            }
        }
        
        logger.info("Deleted {} out of {} dead jobs", deletedCount, jobIds.size());
        return deletedCount;
    }
    
    /**
     * Clear all dead jobs permanently
     */
    public int clearAllDeadJobs() {
        int deletedCount = persistenceManager.deleteJobsByState(JobState.DEAD);
        logger.info("Cleared {} dead jobs from DLQ", deletedCount);
        return deletedCount;
    }
    
    /**
     * Clear old dead jobs (older than specified days)
     */
    public int clearOldDeadJobs(int olderThanDays) {
        LocalDateTime cutoffTime = LocalDateTime.now().minusDays(olderThanDays);
        
        List<Job> oldDeadJobs = getDeadJobs().stream()
                .filter(job -> job.getUpdatedAt().isBefore(cutoffTime))
                .collect(Collectors.toList());
        
        List<String> jobIds = oldDeadJobs.stream()
                .map(Job::getId)
                .collect(Collectors.toList());
        
        int deletedCount = deleteDeadJobs(jobIds);
        logger.info("Cleared {} dead jobs older than {} days", deletedCount, olderThanDays);
        
        return deletedCount;
    }
    
    /**
     * Get DLQ statistics
     */
    public DLQStatistics getStatistics() {
        List<Job> deadJobs = getDeadJobs();
        
        if (deadJobs.isEmpty()) {
            return new DLQStatistics(0, null, null, null);
        }
        
        // Find oldest and newest dead jobs
        Job oldest = deadJobs.stream()
                .min((j1, j2) -> j1.getUpdatedAt().compareTo(j2.getUpdatedAt()))
                .orElse(null);
        
        Job newest = deadJobs.stream()
                .max((j1, j2) -> j1.getUpdatedAt().compareTo(j2.getUpdatedAt()))
                .orElse(null);
        
        // Count jobs by error type (simplified)
        long timeoutErrors = deadJobs.stream()
                .filter(job -> job.getErrorMessage() != null && 
                              job.getErrorMessage().toLowerCase().contains("timeout"))
                .count();
        
        return new DLQStatistics(
                deadJobs.size(),
                oldest != null ? oldest.getUpdatedAt() : null,
                newest != null ? newest.getUpdatedAt() : null,
                timeoutErrors
        );
    }
    
    /**
     * Get the count of dead jobs
     */
    public int getDeadJobCount() {
        return getDeadJobs().size();
    }
    
    /**
     * Check if a job exists in the DLQ
     */
    public boolean containsJob(String jobId) {
        return getDeadJob(jobId).isPresent();
    }
    
    /**
     * DLQ Statistics class
     */
    public static class DLQStatistics {
        private final int totalDeadJobs;
        private final LocalDateTime oldestJobTime;
        private final LocalDateTime newestJobTime;
        private final Long timeoutErrorCount;
        
        public DLQStatistics(int totalDeadJobs, LocalDateTime oldestJobTime, 
                           LocalDateTime newestJobTime, Long timeoutErrorCount) {
            this.totalDeadJobs = totalDeadJobs;
            this.oldestJobTime = oldestJobTime;
            this.newestJobTime = newestJobTime;
            this.timeoutErrorCount = timeoutErrorCount;
        }
        
        public int getTotalDeadJobs() {
            return totalDeadJobs;
        }
        
        public LocalDateTime getOldestJobTime() {
            return oldestJobTime;
        }
        
        public LocalDateTime getNewestJobTime() {
            return newestJobTime;
        }
        
        public long getTimeoutErrorCount() {
            return timeoutErrorCount;
        }
        
        @Override
        public String toString() {
            return String.format("DLQStatistics{totalDeadJobs=%d, oldestJobTime=%s, " +
                               "newestJobTime=%s, timeoutErrorCount=%d}",
                               totalDeadJobs, oldestJobTime, newestJobTime, timeoutErrorCount);
        }
    }
}
