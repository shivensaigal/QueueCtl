package com.jobqueue.model;

/**
 * Represents the possible states of a job in the queue system.
 */
public enum JobState {
    /**
     * Job is waiting to be picked up by a worker
     */
    PENDING("pending"),
    
    /**
     * Job is currently being executed by a worker
     */
    PROCESSING("processing"),
    
    /**
     * Job has been successfully executed
     */
    COMPLETED("completed"),
    
    /**
     * Job failed but is still retryable
     */
    FAILED("failed"),
    
    /**
     * Job has permanently failed and moved to Dead Letter Queue
     */
    DEAD("dead");
    
    private final String value;
    
    JobState(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
    
    /**
     * Convert string value to JobState enum
     */
    public static JobState fromString(String value) {
        for (JobState state : JobState.values()) {
            if (state.value.equalsIgnoreCase(value)) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown job state: " + value);
    }
    
    @Override
    public String toString() {
        return value;
    }
}
