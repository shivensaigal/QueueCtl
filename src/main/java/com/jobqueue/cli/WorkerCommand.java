package com.jobqueue.cli;

import com.jobqueue.worker.WorkerManager;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.util.concurrent.Callable;

/**
 * CLI command for managing workers.
 */
@Command(
    name = "worker",
    description = "Manage worker processes",
    subcommands = {
        WorkerCommand.StartCommand.class,
        WorkerCommand.StopCommand.class,
        WorkerCommand.StatusCommand.class
    }
)
public class WorkerCommand implements Callable<Integer> {
    
    @ParentCommand
    private QueueCtl parent;
    
    @Override
    public Integer call() {
        // If no subcommand is specified, show help
        System.out.println("Worker management commands:");
        System.out.println("  start   - Start worker processes");
        System.out.println("  stop    - Stop worker processes");
        System.out.println("  status  - Show worker status");
        return 0;
    }
    
    @Command(name = "start", description = "Start worker processes")
    static class StartCommand implements Callable<Integer> {
        
        @ParentCommand
        private WorkerCommand parent;
        
        @Option(
            names = {"-c", "--count"},
            description = "Number of workers to start (default: from config)"
        )
        private Integer workerCount;
        
        @Override
        public Integer call() {
            try {
                parent.parent.initializeComponents();
                
                WorkerManager workerManager = QueueCtl.getWorkerManager();
                
                if (workerManager.isRunning()) {
                    System.out.println("Workers are already running");
                    System.out.println("Active workers: " + workerManager.getActiveWorkerCount());
                    return 0;
                }
                
                if (workerCount != null) {
                    workerManager.start(workerCount);
                } else {
                    workerManager.start();
                }
                
                System.out.println("Workers started successfully");
                System.out.println("Total workers: " + workerManager.getTotalWorkerCount());
                System.out.println("Active workers: " + workerManager.getActiveWorkerCount());
                
                // Keep the process running
                System.out.println("Press Ctrl+C to stop workers...");
                
                // Wait for shutdown signal
                try {
                    Thread.currentThread().join();
                } catch (InterruptedException e) {
                    System.out.println("\nShutdown signal received, stopping workers...");
                }
                
                return 0;
                
            } catch (Exception e) {
                System.err.println("Error starting workers: " + e.getMessage());
                if (parent.parent.isVerbose()) {
                    e.printStackTrace();
                }
                return 1;
            }
        }
    }
    
    @Command(name = "stop", description = "Stop worker processes")
    static class StopCommand implements Callable<Integer> {
        
        @ParentCommand
        private WorkerCommand parent;
        
        @Override
        public Integer call() {
            try {
                parent.parent.initializeComponents();
                
                WorkerManager workerManager = QueueCtl.getWorkerManager();
                
                if (!workerManager.isRunning()) {
                    System.out.println("No workers are currently running");
                    return 0;
                }
                
                System.out.println("Stopping workers...");
                workerManager.stop();
                System.out.println("Workers stopped successfully");
                
                return 0;
                
            } catch (Exception e) {
                System.err.println("Error stopping workers: " + e.getMessage());
                if (parent.parent.isVerbose()) {
                    e.printStackTrace();
                }
                return 1;
            }
        }
    }
    
    @Command(name = "status", description = "Show worker status")
    static class StatusCommand implements Callable<Integer> {
        
        @ParentCommand
        private WorkerCommand parent;
        
        @Override
        public Integer call() {
            try {
                parent.parent.initializeComponents();
                
                WorkerManager workerManager = QueueCtl.getWorkerManager();
                
                System.out.println("Worker Status:");
                System.out.println("  Running: " + workerManager.isRunning());
                System.out.println("  Total Workers: " + workerManager.getTotalWorkerCount());
                System.out.println("  Active Workers: " + workerManager.getActiveWorkerCount());
                
                if (workerManager.getTotalWorkerCount() > 0) {
                    System.out.println("\nWorker Details:");
                    workerManager.getWorkerStatus().forEach(status -> {
                        System.out.printf("  %s: %s%s%n", 
                                        status.getWorkerId(),
                                        status.isRunning() ? "RUNNING" : "STOPPED",
                                        status.isShutdownRequested() ? " (shutdown requested)" : "");
                    });
                }
                
                return 0;
                
            } catch (Exception e) {
                System.err.println("Error getting worker status: " + e.getMessage());
                if (parent.parent.isVerbose()) {
                    e.printStackTrace();
                }
                return 1;
            }
        }
    }
}
