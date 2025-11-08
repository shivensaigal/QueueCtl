package com.jobqueue.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents a job in the queue system.
 * Contains all necessary information for job execution, tracking, and persistence.
 */
public class Job {
    private final String id;
    private final String command;
    private JobState state;
    private int attempts;
    private final int maxRetries;
    
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private final LocalDateTime createdAt;
    
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime updatedAt;
    
    private String errorMessage;
    private LocalDateTime nextRetryAt;
    
    /**
     * Constructor for creating a new job
     */
    public Job(String command, int maxRetries) {
        this(UUID.randomUUID().toString(), command, JobState.PENDING, 0, maxRetries, 
             LocalDateTime.now(), LocalDateTime.now(), null, null);
    }
    
    /**
     * Constructor for creating a job with specific ID (useful for testing)
     */
    public Job(String id, String command, int maxRetries) {
        this(id, command, JobState.PENDING, 0, maxRetries, 
             LocalDateTime.now(), LocalDateTime.now(), null, null);
    }
    
    /**
     * Full constructor for JSON deserialization
     */
    @JsonCreator
    public Job(@JsonProperty("id") String id,
               @JsonProperty("command") String command,
               @JsonProperty("state") JobState state,
               @JsonProperty("attempts") int attempts,
               @JsonProperty("max_retries") int maxRetries,
               @JsonProperty("created_at") LocalDateTime createdAt,
               @JsonProperty("updated_at") LocalDateTime updatedAt,
               @JsonProperty("error_message") String errorMessage,
               @JsonProperty("next_retry_at") LocalDateTime nextRetryAt) {
        this.id = id;
        this.command = command;
        this.state = state;
        this.attempts = attempts;
        this.maxRetries = maxRetries;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.errorMessage = errorMessage;
        this.nextRetryAt = nextRetryAt;
    }
    
    // Getters
    public String getId() {
        return id;
    }
    
    public String getCommand() {
        return command;
    }
    
    public JobState getState() {
        return state;
    }
    
    public int getAttempts() {
        return attempts;
    }
    
    @JsonProperty("max_retries")
    public int getMaxRetries() {
        return maxRetries;
    }
    
    @JsonProperty("created_at")
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
    
    @JsonProperty("updated_at")
    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }
    
    @JsonProperty("error_message")
    public String getErrorMessage() {
        return errorMessage;
    }
    
    @JsonProperty("next_retry_at")
    public LocalDateTime getNextRetryAt() {
        return nextRetryAt;
    }
    
    // State modification methods
    public void markAsProcessing() {
        this.state = JobState.PROCESSING;
        this.updatedAt = LocalDateTime.now();
    }
    
    public void markAsCompleted() {
        this.state = JobState.COMPLETED;
        this.updatedAt = LocalDateTime.now();
        this.errorMessage = null;
        this.nextRetryAt = null;
    }
    
    public void markAsFailed(String errorMessage, LocalDateTime nextRetryAt) {
        this.state = JobState.FAILED;
        this.attempts++;
        this.updatedAt = LocalDateTime.now();
        this.errorMessage = errorMessage;
        this.nextRetryAt = nextRetryAt;
    }
    
    public void markAsDead(String errorMessage) {
        this.state = JobState.DEAD;
        this.updatedAt = LocalDateTime.now();
        this.errorMessage = errorMessage;
        this.nextRetryAt = null;
    }
    
    public void resetForRetry() {
        this.state = JobState.PENDING;
        this.updatedAt = LocalDateTime.now();
        this.nextRetryAt = null;
    }
    
    /**
     * Check if the job can be retried
     */
    public boolean canRetry() {
        return attempts < maxRetries && state == JobState.FAILED;
    }
    
    /**
     * Check if the job is ready for retry (time-based)
     */
    public boolean isReadyForRetry() {
        return nextRetryAt == null || LocalDateTime.now().isAfter(nextRetryAt);
    }
    
    /**
     * Create a copy of this job for retry purposes
     */
    public Job copyForRetry() {
        Job copy = new Job(this.id, this.command, this.state, this.attempts, 
                          this.maxRetries, this.createdAt, this.updatedAt, 
                          this.errorMessage, this.nextRetryAt);
        copy.resetForRetry();
        return copy;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Job job = (Job) o;
        return Objects.equals(id, job.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
    
    @Override
    public String toString() {
        return String.format("Job{id='%s', command='%s', state=%s, attempts=%d/%d, createdAt=%s}", 
                           id, command, state, attempts, maxRetries, createdAt);
    }
}
