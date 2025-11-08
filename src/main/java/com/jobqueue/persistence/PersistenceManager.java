package com.jobqueue.persistence;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.jobqueue.model.Job;
import com.jobqueue.model.JobState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Manages persistence of jobs to JSON file storage.
 * Provides thread-safe operations for job storage and retrieval.
 */
public class PersistenceManager {
    private static final Logger logger = LoggerFactory.getLogger(PersistenceManager.class);
    
    private final ObjectMapper objectMapper;
    private final String dataFile;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, Job> jobCache = new ConcurrentHashMap<>();
    
    public PersistenceManager(String dataFile) {
        this.dataFile = dataFile;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        
        // Load existing jobs into cache
        loadJobs();
    }
    
    /**
     * Save a job to storage
     */
    public void saveJob(Job job) {
        lock.writeLock().lock();
        try {
            jobCache.put(job.getId(), job);
            persistToFile();
            logger.debug("Job saved: {}", job.getId());
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Save multiple jobs to storage
     */
    public void saveJobs(Collection<Job> jobs) {
        lock.writeLock().lock();
        try {
            for (Job job : jobs) {
                jobCache.put(job.getId(), job);
            }
            persistToFile();
            logger.debug("Saved {} jobs to storage", jobs.size());
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Get a job by ID
     */
    public Optional<Job> getJob(String jobId) {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(jobCache.get(jobId));
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get all jobs
     */
    public List<Job> getAllJobs() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(jobCache.values());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get jobs by state
     */
    public List<Job> getJobsByState(JobState state) {
        lock.readLock().lock();
        try {
            return jobCache.values().stream()
                    .filter(job -> job.getState() == state)
                    .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get jobs ready for retry
     */
    public List<Job> getJobsReadyForRetry() {
        lock.readLock().lock();
        try {
            LocalDateTime now = LocalDateTime.now();
            return jobCache.values().stream()
                    .filter(job -> job.getState() == JobState.FAILED)
                    .filter(job -> job.canRetry())
                    .filter(job -> job.getNextRetryAt() == null || now.isAfter(job.getNextRetryAt()))
                    .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get pending jobs
     */
    public List<Job> getPendingJobs() {
        return getJobsByState(JobState.PENDING);
    }
    
    /**
     * Get processing jobs
     */
    public List<Job> getProcessingJobs() {
        return getJobsByState(JobState.PROCESSING);
    }
    
    /**
     * Get completed jobs
     */
    public List<Job> getCompletedJobs() {
        return getJobsByState(JobState.COMPLETED);
    }
    
    /**
     * Get failed jobs
     */
    public List<Job> getFailedJobs() {
        return getJobsByState(JobState.FAILED);
    }
    
    /**
     * Get dead jobs (Dead Letter Queue)
     */
    public List<Job> getDeadJobs() {
        return getJobsByState(JobState.DEAD);
    }
    
    /**
     * Delete a job from storage
     */
    public boolean deleteJob(String jobId) {
        lock.writeLock().lock();
        try {
            Job removed = jobCache.remove(jobId);
            if (removed != null) {
                persistToFile();
                logger.debug("Job deleted: {}", jobId);
                return true;
            }
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Delete jobs by state
     */
    public int deleteJobsByState(JobState state) {
        lock.writeLock().lock();
        try {
            List<String> toDelete = jobCache.values().stream()
                    .filter(job -> job.getState() == state)
                    .map(Job::getId)
                    .collect(Collectors.toList());
            
            toDelete.forEach(jobCache::remove);
            
            if (!toDelete.isEmpty()) {
                persistToFile();
                logger.info("Deleted {} jobs with state {}", toDelete.size(), state);
            }
            
            return toDelete.size();
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Get job statistics
     */
    public Map<JobState, Long> getJobStatistics() {
        lock.readLock().lock();
        try {
            return jobCache.values().stream()
                    .collect(Collectors.groupingBy(
                            Job::getState,
                            Collectors.counting()
                    ));
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Clear all jobs (useful for testing)
     */
    public void clearAllJobs() {
        lock.writeLock().lock();
        try {
            jobCache.clear();
            persistToFile();
            logger.info("All jobs cleared from storage");
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Load jobs from file into cache
     */
    private void loadJobs() {
        Path filePath = Paths.get(dataFile);
        
        if (!Files.exists(filePath)) {
            logger.info("Data file {} does not exist, starting with empty job queue", dataFile);
            return;
        }
        
        try {
            String content = Files.readString(filePath);
            if (content.trim().isEmpty()) {
                logger.info("Data file {} is empty, starting with empty job queue", dataFile);
                return;
            }
            
            List<Job> jobs = objectMapper.readValue(content, new TypeReference<List<Job>>() {});
            
            for (Job job : jobs) {
                jobCache.put(job.getId(), job);
            }
            
            logger.info("Loaded {} jobs from {}", jobs.size(), dataFile);
            
            // Log statistics
            Map<JobState, Long> stats = getJobStatistics();
            logger.info("Job statistics: {}", stats);
            
        } catch (IOException e) {
            logger.error("Failed to load jobs from {}", dataFile, e);
            throw new RuntimeException("Failed to load jobs from storage", e);
        }
    }
    
    /**
     * Persist current job cache to file
     */
    private void persistToFile() {
        try {
            // Ensure parent directory exists
            Path filePath = Paths.get(dataFile);
            Path parentDir = filePath.getParent();
            if (parentDir != null && !Files.exists(parentDir)) {
                Files.createDirectories(parentDir);
            }
            
            // Write to temporary file first for atomic operation
            Path tempFile = Paths.get(dataFile + ".tmp");
            
            List<Job> jobs = new ArrayList<>(jobCache.values());
            String json = objectMapper.writerWithDefaultPrettyPrinter()
                                    .writeValueAsString(jobs);
            
            Files.writeString(tempFile, json);
            
            // Atomic move to final location
            Files.move(tempFile, filePath, StandardCopyOption.REPLACE_EXISTING);
            
            logger.debug("Persisted {} jobs to {}", jobs.size(), dataFile);
            
        } catch (IOException e) {
            logger.error("Failed to persist jobs to {}", dataFile, e);
            throw new RuntimeException("Failed to persist jobs to storage", e);
        }
    }
    
    /**
     * Get the data file path
     */
    public String getDataFile() {
        return dataFile;
    }
    
    /**
     * Get the number of jobs in cache
     */
    public int getJobCount() {
        lock.readLock().lock();
        try {
            return jobCache.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}
