package com.jobqueue;

import com.jobqueue.config.JobQueueConfig;
import com.jobqueue.model.Job;
import com.jobqueue.model.JobState;
import com.jobqueue.persistence.PersistenceManager;
import com.jobqueue.queue.JobQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic integration tests for the Job Queue System.
 */
class JobQueueSystemTest {
    
    @TempDir
    Path tempDir;
    
    private PersistenceManager persistenceManager;
    private JobQueue jobQueue;
    private JobQueueConfig config;
    
    @BeforeEach
    void setUp() {
        String dataFile = tempDir.resolve("test-jobs.json").toString();
        config = new JobQueueConfig().withDataFile(dataFile);
        persistenceManager = new PersistenceManager(dataFile);
        jobQueue = new JobQueue(persistenceManager, config);
        jobQueue.initialize();
    }
    
    @Test
    void testJobEnqueueAndRetrieve() {
        // Enqueue a job
        String jobId = jobQueue.enqueue("echo 'test'");
        assertNotNull(jobId);
        
        // Retrieve the job
        Optional<Job> jobOpt = jobQueue.getJob(jobId);
        assertTrue(jobOpt.isPresent());
        
        Job job = jobOpt.get();
        assertEquals(jobId, job.getId());
        assertEquals("echo 'test'", job.getCommand());
        assertEquals(JobState.PENDING, job.getState());
        assertEquals(0, job.getAttempts());
        assertEquals(config.getMaxRetries(), job.getMaxRetries());
    }
    
    @Test
    void testJobStateTransitions() {
        // Create and enqueue a job
        String jobId = jobQueue.enqueue("echo 'test'");
        Job job = jobQueue.getJob(jobId).orElseThrow();
        
        // Test state transitions
        assertEquals(JobState.PENDING, job.getState());
        
        job.markAsProcessing();
        assertEquals(JobState.PROCESSING, job.getState());
        
        job.markAsCompleted();
        assertEquals(JobState.COMPLETED, job.getState());
    }
    
    @Test
    void testJobRetryLogic() {
        // Create a job with 2 max retries
        String jobId = jobQueue.enqueue("exit 1", 2);
        Job job = jobQueue.getJob(jobId).orElseThrow();
        
        // Mark as failed first time
        jobQueue.markFailed(job, "Command failed");
        job = jobQueue.getJob(jobId).orElseThrow();
        
        assertEquals(JobState.FAILED, job.getState());
        assertEquals(1, job.getAttempts());
        assertTrue(job.canRetry());
        
        // Mark as failed second time
        jobQueue.markFailed(job, "Command failed again");
        job = jobQueue.getJob(jobId).orElseThrow();
        
        assertEquals(JobState.DEAD, job.getState());
        assertEquals(2, job.getAttempts());
        assertFalse(job.canRetry());
    }
    
    @Test
    void testJobPersistence() {
        // Enqueue jobs
        String jobId1 = jobQueue.enqueue("echo 'job1'");
        String jobId2 = jobQueue.enqueue("echo 'job2'");
        
        // Create new persistence manager with same file
        PersistenceManager newPersistenceManager = new PersistenceManager(config.getDataFile());
        
        // Jobs should be loaded from file
        assertEquals(2, newPersistenceManager.getJobCount());
        assertTrue(newPersistenceManager.getJob(jobId1).isPresent());
        assertTrue(newPersistenceManager.getJob(jobId2).isPresent());
    }
    
    @Test
    void testJobStatistics() {
        // Enqueue jobs in different states
        String pendingJobId = jobQueue.enqueue("echo 'pending'");
        String completedJobId = jobQueue.enqueue("echo 'completed'");
        String failedJobId = jobQueue.enqueue("exit 1");
        
        // Change states
        Job completedJob = jobQueue.getJob(completedJobId).orElseThrow();
        jobQueue.markCompleted(completedJob);
        
        Job failedJob = jobQueue.getJob(failedJobId).orElseThrow();
        jobQueue.markFailed(failedJob, "Test failure");
        
        // Check statistics
        var stats = jobQueue.getStatistics();
        assertEquals(1L, stats.get(JobState.PENDING));
        assertEquals(1L, stats.get(JobState.COMPLETED));
        assertEquals(1L, stats.get(JobState.FAILED));
    }
    
    @Test
    void testJobsByState() {
        // Enqueue jobs
        jobQueue.enqueue("echo 'job1'");
        jobQueue.enqueue("echo 'job2'");
        jobQueue.enqueue("echo 'job3'");
        
        // All should be pending
        List<Job> pendingJobs = jobQueue.getJobsByState(JobState.PENDING);
        assertEquals(3, pendingJobs.size());
        
        List<Job> completedJobs = jobQueue.getJobsByState(JobState.COMPLETED);
        assertEquals(0, completedJobs.size());
    }
}
