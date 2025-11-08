package com.jobqueue.cli;

import com.jobqueue.model.JobState;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * CLI command for showing system status.
 */
@Command(
    name = "status",
    description = "Show job queue system status"
)
public class StatusCommand implements Callable<Integer> {
    
    @ParentCommand
    private QueueCtl parent;
    
    @Override
    public Integer call() {
        try {
            parent.initializeComponents();
            
            // Get job statistics
            Map<JobState, Long> stats = QueueCtl.getJobQueue().getStatistics();
            
            System.out.println("Job Queue System Status");
            System.out.println("=======================");
            
            // Job statistics
            System.out.println("\nJob Statistics:");
            System.out.printf("  Pending:    %d%n", stats.getOrDefault(JobState.PENDING, 0L));
            System.out.printf("  Processing: %d%n", stats.getOrDefault(JobState.PROCESSING, 0L));
            System.out.printf("  Completed:  %d%n", stats.getOrDefault(JobState.COMPLETED, 0L));
            System.out.printf("  Failed:     %d%n", stats.getOrDefault(JobState.FAILED, 0L));
            System.out.printf("  Dead:       %d%n", stats.getOrDefault(JobState.DEAD, 0L));
            
            long totalJobs = stats.values().stream().mapToLong(Long::longValue).sum();
            System.out.printf("  Total:      %d%n", totalJobs);
            
            // Worker status
            System.out.println("\nWorker Status:");
            System.out.printf("  Running:        %s%n", QueueCtl.getWorkerManager().isRunning());
            System.out.printf("  Total Workers:  %d%n", QueueCtl.getWorkerManager().getTotalWorkerCount());
            System.out.printf("  Active Workers: %d%n", QueueCtl.getWorkerManager().getActiveWorkerCount());
            
            // Queue status
            System.out.println("\nQueue Status:");
            System.out.printf("  Pending in Queue: %d%n", QueueCtl.getJobQueue().getPendingCount());
            
            // Configuration
            System.out.println("\nConfiguration:");
            System.out.printf("  Config File:    %s%n", parent.getConfigFile());
            System.out.printf("  Data File:      %s%n", QueueCtl.getConfigManager().getConfig().getDataFile());
            System.out.printf("  Max Retries:    %d%n", QueueCtl.getConfigManager().getConfig().getMaxRetries());
            System.out.printf("  Backoff Base:   %d%n", QueueCtl.getConfigManager().getConfig().getBackoffBase());
            System.out.printf("  Job Timeout:    %d seconds%n", QueueCtl.getConfigManager().getConfig().getJobTimeoutSeconds());
            
            return 0;
            
        } catch (Exception e) {
            System.err.println("Error getting status: " + e.getMessage());
            if (parent.isVerbose()) {
                e.printStackTrace();
            }
            return 1;
        }
    }
}
