package com.jobqueue.cli;

import com.jobqueue.dlq.DLQManager;
import com.jobqueue.model.Job;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * CLI command for managing Dead Letter Queue.
 */
@Command(
    name = "dlq",
    description = "Manage Dead Letter Queue (failed jobs)",
    subcommands = {
        DLQCommand.ListCommand.class,
        DLQCommand.RetryCommand.class,
        DLQCommand.DeleteCommand.class,
        DLQCommand.ClearCommand.class,
        DLQCommand.StatsCommand.class
    }
)
public class DLQCommand implements Callable<Integer> {
    
    @ParentCommand
    private QueueCtl parent;
    
    @Override
    public Integer call() {
        // If no subcommand is specified, show help
        System.out.println("Dead Letter Queue management commands:");
        System.out.println("  list    - List dead jobs");
        System.out.println("  retry   - Retry dead jobs");
        System.out.println("  delete  - Delete dead jobs");
        System.out.println("  clear   - Clear all dead jobs");
        System.out.println("  stats   - Show DLQ statistics");
        return 0;
    }
    
    @Command(name = "list", description = "List jobs in Dead Letter Queue")
    static class ListCommand implements Callable<Integer> {
        
        @ParentCommand
        private DLQCommand parent;
        
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
                parent.parent.initializeComponents();
                
                DLQManager dlqManager = QueueCtl.getDLQManager();
                List<Job> deadJobs = dlqManager.getDeadJobs(offset, limit);
                int totalDeadJobs = dlqManager.getDeadJobCount();
                
                if (deadJobs.isEmpty()) {
                    System.out.println("No jobs in Dead Letter Queue");
                    return 0;
                }
                
                System.out.printf("Dead Letter Queue - Showing %d-%d of %d jobs%n%n",
                                offset + 1, offset + deadJobs.size(), totalDeadJobs);
                
                // Print header
                if (verbose) {
                    System.out.printf("%-36s %-3s %-50s %-19s %-30s%n",
                                    "ID", "ATT", "COMMAND", "FAILED AT", "ERROR");
                    System.out.println("-".repeat(140));
                } else {
                    System.out.printf("%-36s %-3s %-50s %-30s%n",
                                    "ID", "ATT", "COMMAND", "ERROR");
                    System.out.println("-".repeat(120));
                }
                
                // Print jobs
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                
                for (Job job : deadJobs) {
                    String command = job.getCommand();
                    if (command.length() > 47) {
                        command = command.substring(0, 44) + "...";
                    }
                    
                    String error = job.getErrorMessage() != null ? job.getErrorMessage() : "Unknown";
                    if (error.length() > 27) {
                        error = error.substring(0, 24) + "...";
                    }
                    
                    String attempts = String.format("%d/%d", job.getAttempts(), job.getMaxRetries());
                    
                    if (verbose) {
                        System.out.printf("%-36s %-3s %-50s %-19s %-30s%n",
                                        job.getId(),
                                        attempts,
                                        command,
                                        job.getUpdatedAt().format(formatter),
                                        error);
                        
                        if (job.getErrorMessage() != null && job.getErrorMessage().length() > 27) {
                            System.out.printf("  Full Error: %s%n", job.getErrorMessage());
                        }
                        System.out.println();
                    } else {
                        System.out.printf("%-36s %-3s %-50s %-30s%n",
                                        job.getId(),
                                        attempts,
                                        command,
                                        error);
                    }
                }
                
                return 0;
                
            } catch (Exception e) {
                System.err.println("Error listing DLQ: " + e.getMessage());
                if (parent.parent.isVerbose()) {
                    e.printStackTrace();
                }
                return 1;
            }
        }
    }
    
    @Command(name = "retry", description = "Retry jobs from Dead Letter Queue")
    static class RetryCommand implements Callable<Integer> {
        
        @ParentCommand
        private DLQCommand parent;
        
        @Parameters(
            arity = "0..*",
            description = "Job IDs to retry (if none specified, retry all)"
        )
        private List<String> jobIds;
        
        @Option(
            names = {"--all"},
            description = "Retry all dead jobs"
        )
        private boolean retryAll;
        
        @Override
        public Integer call() {
            try {
                parent.parent.initializeComponents();
                
                DLQManager dlqManager = QueueCtl.getDLQManager();
                int retriedCount;
                
                if (retryAll || (jobIds == null || jobIds.isEmpty())) {
                    System.out.println("Retrying all dead jobs...");
                    retriedCount = dlqManager.retryAllDeadJobs();
                } else {
                    System.out.printf("Retrying %d specified jobs...%n", jobIds.size());
                    retriedCount = dlqManager.retryDeadJobs(jobIds);
                }
                
                System.out.printf("Successfully retried %d jobs%n", retriedCount);
                
                return 0;
                
            } catch (Exception e) {
                System.err.println("Error retrying DLQ jobs: " + e.getMessage());
                if (parent.parent.isVerbose()) {
                    e.printStackTrace();
                }
                return 1;
            }
        }
    }
    
    @Command(name = "delete", description = "Delete jobs from Dead Letter Queue")
    static class DeleteCommand implements Callable<Integer> {
        
        @ParentCommand
        private DLQCommand parent;
        
        @Parameters(
            arity = "1..*",
            description = "Job IDs to delete"
        )
        private List<String> jobIds;
        
        @Override
        public Integer call() {
            try {
                parent.parent.initializeComponents();
                
                DLQManager dlqManager = QueueCtl.getDLQManager();
                
                System.out.printf("Deleting %d jobs from DLQ...%n", jobIds.size());
                int deletedCount = dlqManager.deleteDeadJobs(jobIds);
                
                System.out.printf("Successfully deleted %d jobs%n", deletedCount);
                
                return 0;
                
            } catch (Exception e) {
                System.err.println("Error deleting DLQ jobs: " + e.getMessage());
                if (parent.parent.isVerbose()) {
                    e.printStackTrace();
                }
                return 1;
            }
        }
    }
    
    @Command(name = "clear", description = "Clear Dead Letter Queue")
    static class ClearCommand implements Callable<Integer> {
        
        @ParentCommand
        private DLQCommand parent;
        
        @Option(
            names = {"--older-than"},
            description = "Only clear jobs older than specified days"
        )
        private Integer olderThanDays;
        
        @Option(
            names = {"--confirm"},
            description = "Confirm the operation"
        )
        private boolean confirm;
        
        @Override
        public Integer call() {
            try {
                parent.parent.initializeComponents();
                
                DLQManager dlqManager = QueueCtl.getDLQManager();
                
                if (!confirm) {
                    System.out.println("This operation will permanently delete jobs from the DLQ.");
                    System.out.println("Use --confirm to proceed.");
                    return 1;
                }
                
                int deletedCount;
                
                if (olderThanDays != null) {
                    System.out.printf("Clearing dead jobs older than %d days...%n", olderThanDays);
                    deletedCount = dlqManager.clearOldDeadJobs(olderThanDays);
                } else {
                    System.out.println("Clearing all dead jobs...");
                    deletedCount = dlqManager.clearAllDeadJobs();
                }
                
                System.out.printf("Successfully cleared %d jobs from DLQ%n", deletedCount);
                
                return 0;
                
            } catch (Exception e) {
                System.err.println("Error clearing DLQ: " + e.getMessage());
                if (parent.parent.isVerbose()) {
                    e.printStackTrace();
                }
                return 1;
            }
        }
    }
    
    @Command(name = "stats", description = "Show Dead Letter Queue statistics")
    static class StatsCommand implements Callable<Integer> {
        
        @ParentCommand
        private DLQCommand parent;
        
        @Override
        public Integer call() {
            try {
                parent.parent.initializeComponents();
                
                DLQManager dlqManager = QueueCtl.getDLQManager();
                DLQManager.DLQStatistics stats = dlqManager.getStatistics();
                
                System.out.println("Dead Letter Queue Statistics");
                System.out.println("============================");
                System.out.printf("Total Dead Jobs: %d%n", stats.getTotalDeadJobs());
                
                if (stats.getTotalDeadJobs() > 0) {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    
                    if (stats.getOldestJobTime() != null) {
                        System.out.printf("Oldest Job: %s%n", stats.getOldestJobTime().format(formatter));
                    }
                    
                    if (stats.getNewestJobTime() != null) {
                        System.out.printf("Newest Job: %s%n", stats.getNewestJobTime().format(formatter));
                    }
                    
                    System.out.printf("Timeout Errors: %d%n", stats.getTimeoutErrorCount());
                }
                
                return 0;
                
            } catch (Exception e) {
                System.err.println("Error getting DLQ statistics: " + e.getMessage());
                if (parent.parent.isVerbose()) {
                    e.printStackTrace();
                }
                return 1;
            }
        }
    }
}
