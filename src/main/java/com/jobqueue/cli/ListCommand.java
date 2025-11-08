package com.jobqueue.cli;

import com.jobqueue.model.Job;
import com.jobqueue.model.JobState;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * CLI command for listing jobs.
 */
@Command(
    name = "list",
    description = "List jobs by state or criteria"
)
public class ListCommand implements Callable<Integer> {
    
    @ParentCommand
    private QueueCtl parent;
    
    @Option(
        names = {"-s", "--state"},
        description = "Filter by job state: ${COMPLETION-CANDIDATES}"
    )
    private JobState state;
    
    @Option(
        names = {"-l", "--limit"},
        description = "Limit number of results",
        defaultValue = "50"
    )
    private int limit;
    
    @Option(
        names = {"-o", "--offset"},
        description = "Offset for pagination",
        defaultValue = "0"
    )
    private int offset;
    
    @Option(
        names = {"--verbose"},
        description = "Show detailed job information"
    )
    private boolean verbose;
    
    @Override
    public Integer call() {
        try {
            parent.initializeComponents();
            
            List<Job> jobs;
            
            if (state != null) {
                jobs = QueueCtl.getJobQueue().getJobsByState(state);
                System.out.println("Jobs with state: " + state);
            } else {
                jobs = QueueCtl.getJobQueue().getAllJobs();
                System.out.println("All jobs:");
            }
            
            if (jobs.isEmpty()) {
                System.out.println("No jobs found");
                return 0;
            }
            
            // Apply pagination
            int start = Math.min(offset, jobs.size());
            int end = Math.min(start + limit, jobs.size());
            List<Job> paginatedJobs = jobs.subList(start, end);
            
            System.out.printf("Showing %d-%d of %d jobs%n%n", 
                            start + 1, start + paginatedJobs.size(), jobs.size());
            
            // Print header
            if (verbose) {
                System.out.printf("%-36s %-12s %-3s %-50s %-19s %-19s%n",
                                "ID", "STATE", "ATT", "COMMAND", "CREATED", "UPDATED");
                System.out.println("-".repeat(140));
            } else {
                System.out.printf("%-36s %-12s %-3s %-50s%n",
                                "ID", "STATE", "ATT", "COMMAND");
                System.out.println("-".repeat(103));
            }
            
            // Print jobs
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            
            for (Job job : paginatedJobs) {
                String command = job.getCommand();
                if (command.length() > 47) {
                    command = command.substring(0, 44) + "...";
                }
                
                String attempts = String.format("%d/%d", job.getAttempts(), job.getMaxRetries());
                
                if (verbose) {
                    System.out.printf("%-36s %-12s %-3s %-50s %-19s %-19s%n",
                                    job.getId(),
                                    job.getState(),
                                    attempts,
                                    command,
                                    job.getCreatedAt().format(formatter),
                                    job.getUpdatedAt().format(formatter));
                    
                    if (job.getErrorMessage() != null) {
                        System.out.printf("  Error: %s%n", job.getErrorMessage());
                    }
                    
                    if (job.getNextRetryAt() != null) {
                        System.out.printf("  Next Retry: %s%n", job.getNextRetryAt().format(formatter));
                    }
                    
                    System.out.println();
                } else {
                    System.out.printf("%-36s %-12s %-3s %-50s%n",
                                    job.getId(),
                                    job.getState(),
                                    attempts,
                                    command);
                }
            }
            
            // Show pagination info
            if (jobs.size() > limit) {
                System.out.printf("%nShowing page %d of %d (use --offset and --limit for pagination)%n",
                                (offset / limit) + 1, (jobs.size() + limit - 1) / limit);
            }
            
            return 0;
            
        } catch (Exception e) {
            System.err.println("Error listing jobs: " + e.getMessage());
            if (parent.isVerbose()) {
                e.printStackTrace();
            }
            return 1;
        }
    }
}
