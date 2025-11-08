package com.jobqueue.cli;

import com.jobqueue.config.JobQueueConfig;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.util.concurrent.Callable;

/**
 * CLI command for managing configuration.
 */
@Command(
    name = "config",
    description = "Manage system configuration",
    subcommands = {
        ConfigCommand.ShowCommand.class,
        ConfigCommand.SetCommand.class,
        ConfigCommand.ReloadCommand.class
    }
)
public class ConfigCommand implements Callable<Integer> {
    
    @ParentCommand
    private QueueCtl parent;
    
    @Override
    public Integer call() {
        // If no subcommand is specified, show help
        System.out.println("Configuration management commands:");
        System.out.println("  show    - Show current configuration");
        System.out.println("  set     - Set configuration parameter");
        System.out.println("  reload  - Reload configuration from file");
        return 0;
    }
    
    @Command(name = "show", description = "Show current configuration")
    static class ShowCommand implements Callable<Integer> {
        
        @ParentCommand
        private ConfigCommand parent;
        
        @Override
        public Integer call() {
            try {
                parent.parent.initializeComponents();
                
                JobQueueConfig config = QueueCtl.getConfigManager().getConfig();
                
                System.out.println("Current Configuration");
                System.out.println("====================");
                System.out.printf("Max Retries:              %d%n", config.getMaxRetries());
                System.out.printf("Backoff Base:             %d%n", config.getBackoffBase());
                System.out.printf("Worker Count:             %d%n", config.getWorkerCount());
                System.out.printf("Data File:                %s%n", config.getDataFile());
                System.out.printf("Job Timeout (seconds):    %d%n", config.getJobTimeoutSeconds());
                System.out.printf("Retry Check Interval (s): %d%n", config.getRetryCheckIntervalSeconds());
                
                System.out.printf("%nConfiguration File: %s%n", parent.parent.getConfigFile());
                
                return 0;
                
            } catch (Exception e) {
                System.err.println("Error showing configuration: " + e.getMessage());
                if (parent.parent.isVerbose()) {
                    e.printStackTrace();
                }
                return 1;
            }
        }
    }
    
    @Command(name = "set", description = "Set configuration parameter")
    static class SetCommand implements Callable<Integer> {
        
        @ParentCommand
        private ConfigCommand parent;
        
        @Parameters(
            index = "0",
            description = "Configuration parameter: max-retries, backoff-base, worker-count, job-timeout, retry-interval"
        )
        private String parameter;
        
        @Parameters(
            index = "1",
            description = "New value for the parameter"
        )
        private String value;
        
        @Override
        public Integer call() {
            try {
                parent.parent.initializeComponents();
                
                switch (parameter.toLowerCase()) {
                    case "max-retries":
                        int maxRetries = Integer.parseInt(value);
                        if (maxRetries < 0) {
                            System.err.println("Error: max-retries must be non-negative");
                            return 1;
                        }
                        QueueCtl.getConfigManager().updateMaxRetries(maxRetries);
                        System.out.printf("Max retries updated to: %d%n", maxRetries);
                        break;
                        
                    case "backoff-base":
                        int backoffBase = Integer.parseInt(value);
                        if (backoffBase < 1) {
                            System.err.println("Error: backoff-base must be positive");
                            return 1;
                        }
                        QueueCtl.getConfigManager().updateBackoffBase(backoffBase);
                        System.out.printf("Backoff base updated to: %d%n", backoffBase);
                        break;
                        
                    case "worker-count":
                        int workerCount = Integer.parseInt(value);
                        if (workerCount < 1) {
                            System.err.println("Error: worker-count must be positive");
                            return 1;
                        }
                        QueueCtl.getConfigManager().updateWorkerCount(workerCount);
                        System.out.printf("Worker count updated to: %d%n", workerCount);
                        System.out.println("Note: Restart workers for this change to take effect");
                        break;
                        
                    case "job-timeout":
                        long jobTimeout = Long.parseLong(value);
                        if (jobTimeout < 1) {
                            System.err.println("Error: job-timeout must be positive");
                            return 1;
                        }
                        QueueCtl.getConfigManager().updateJobTimeout(jobTimeout);
                        System.out.printf("Job timeout updated to: %d seconds%n", jobTimeout);
                        break;
                        
                    case "retry-interval":
                        long retryInterval = Long.parseLong(value);
                        if (retryInterval < 1) {
                            System.err.println("Error: retry-interval must be positive");
                            return 1;
                        }
                        QueueCtl.getConfigManager().updateRetryCheckInterval(retryInterval);
                        System.out.printf("Retry check interval updated to: %d seconds%n", retryInterval);
                        break;
                        
                    default:
                        System.err.println("Error: Unknown parameter: " + parameter);
                        System.err.println("Valid parameters: max-retries, backoff-base, worker-count, job-timeout, retry-interval");
                        return 1;
                }
                
                return 0;
                
            } catch (NumberFormatException e) {
                System.err.println("Error: Invalid number format for value: " + value);
                return 1;
            } catch (Exception e) {
                System.err.println("Error setting configuration: " + e.getMessage());
                if (parent.parent.isVerbose()) {
                    e.printStackTrace();
                }
                return 1;
            }
        }
    }
    
    @Command(name = "reload", description = "Reload configuration from file")
    static class ReloadCommand implements Callable<Integer> {
        
        @ParentCommand
        private ConfigCommand parent;
        
        @Override
        public Integer call() {
            try {
                parent.parent.initializeComponents();
                
                QueueCtl.getConfigManager().reloadConfig();
                System.out.println("Configuration reloaded successfully");
                
                // Show the reloaded configuration
                JobQueueConfig config = QueueCtl.getConfigManager().getConfig();
                System.out.println("\nReloaded Configuration:");
                System.out.printf("  Max Retries: %d%n", config.getMaxRetries());
                System.out.printf("  Backoff Base: %d%n", config.getBackoffBase());
                System.out.printf("  Worker Count: %d%n", config.getWorkerCount());
                System.out.printf("  Job Timeout: %d seconds%n", config.getJobTimeoutSeconds());
                
                return 0;
                
            } catch (Exception e) {
                System.err.println("Error reloading configuration: " + e.getMessage());
                if (parent.parent.isVerbose()) {
                    e.printStackTrace();
                }
                return 1;
            }
        }
    }
}
