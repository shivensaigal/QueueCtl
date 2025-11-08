package com.jobqueue.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration class for the Job Queue System.
 * Contains all configurable parameters with sensible defaults.
 */
public class JobQueueConfig {
    private final int maxRetries;
    private final int backoffBase;
    private final int workerCount;
    private final String dataFile;
    private final long jobTimeoutSeconds;
    private final long retryCheckIntervalSeconds;
    
    /**
     * Default constructor with sensible defaults
     */
    public JobQueueConfig() {
        this(3, 2, 3, "jobs.json", 300, 30);
    }
    
    /**
     * Full constructor for JSON deserialization and custom configurations
     */
    @JsonCreator
    public JobQueueConfig(@JsonProperty("max_retries") int maxRetries,
                         @JsonProperty("backoff_base") int backoffBase,
                         @JsonProperty("worker_count") int workerCount,
                         @JsonProperty("data_file") String dataFile,
                         @JsonProperty("job_timeout_seconds") long jobTimeoutSeconds,
                         @JsonProperty("retry_check_interval_seconds") long retryCheckIntervalSeconds) {
        this.maxRetries = maxRetries;
        this.backoffBase = backoffBase;
        this.workerCount = workerCount;
        this.dataFile = dataFile;
        this.jobTimeoutSeconds = jobTimeoutSeconds;
        this.retryCheckIntervalSeconds = retryCheckIntervalSeconds;
    }
    
    @JsonProperty("max_retries")
    public int getMaxRetries() {
        return maxRetries;
    }
    
    @JsonProperty("backoff_base")
    public int getBackoffBase() {
        return backoffBase;
    }
    
    @JsonProperty("worker_count")
    public int getWorkerCount() {
        return workerCount;
    }
    
    @JsonProperty("data_file")
    public String getDataFile() {
        return dataFile;
    }
    
    @JsonProperty("job_timeout_seconds")
    public long getJobTimeoutSeconds() {
        return jobTimeoutSeconds;
    }
    
    @JsonProperty("retry_check_interval_seconds")
    public long getRetryCheckIntervalSeconds() {
        return retryCheckIntervalSeconds;
    }
    
    /**
     * Create a new config with updated max retries
     */
    public JobQueueConfig withMaxRetries(int maxRetries) {
        return new JobQueueConfig(maxRetries, backoffBase, workerCount, dataFile, 
                                 jobTimeoutSeconds, retryCheckIntervalSeconds);
    }
    
    /**
     * Create a new config with updated backoff base
     */
    public JobQueueConfig withBackoffBase(int backoffBase) {
        return new JobQueueConfig(maxRetries, backoffBase, workerCount, dataFile, 
                                 jobTimeoutSeconds, retryCheckIntervalSeconds);
    }
    
    /**
     * Create a new config with updated worker count
     */
    public JobQueueConfig withWorkerCount(int workerCount) {
        return new JobQueueConfig(maxRetries, backoffBase, workerCount, dataFile, 
                                 jobTimeoutSeconds, retryCheckIntervalSeconds);
    }
    
    /**
     * Create a new config with updated data file
     */
    public JobQueueConfig withDataFile(String dataFile) {
        return new JobQueueConfig(maxRetries, backoffBase, workerCount, dataFile, 
                                 jobTimeoutSeconds, retryCheckIntervalSeconds);
    }
    
    /**
     * Create a new config with updated job timeout
     */
    public JobQueueConfig withJobTimeout(long jobTimeoutSeconds) {
        return new JobQueueConfig(maxRetries, backoffBase, workerCount, dataFile, 
                                 jobTimeoutSeconds, retryCheckIntervalSeconds);
    }
    
    /**
     * Create a new config with updated retry check interval
     */
    public JobQueueConfig withRetryCheckInterval(long retryCheckIntervalSeconds) {
        return new JobQueueConfig(maxRetries, backoffBase, workerCount, dataFile, 
                                 jobTimeoutSeconds, retryCheckIntervalSeconds);
    }
    
    @Override
    public String toString() {
        return String.format("JobQueueConfig{maxRetries=%d, backoffBase=%d, workerCount=%d, " +
                           "dataFile='%s', jobTimeoutSeconds=%d, retryCheckIntervalSeconds=%d}",
                           maxRetries, backoffBase, workerCount, dataFile, 
                           jobTimeoutSeconds, retryCheckIntervalSeconds);
    }
}
