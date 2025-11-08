package com.jobqueue.cli;

import com.jobqueue.config.ConfigManager;
import com.jobqueue.config.JobQueueConfig;
import com.jobqueue.dlq.DLQManager;
import com.jobqueue.persistence.PersistenceManager;
import com.jobqueue.queue.JobQueue;
import com.jobqueue.worker.WorkerManager;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

/**
 * Main CLI entry point for the Job Queue System.
 * Provides the queuectl command-line interface.
 */
@Command(
    name = "queuectl",
    description = "Job Queue System Command Line Interface",
    version = "1.0.0",
    mixinStandardHelpOptions = true,
    subcommands = {
        EnqueueCommand.class,
        WorkerCommand.class,
        StatusCommand.class,
        ListCommand.class,
        DLQCommand.class,
        ConfigCommand.class
    }
)
public class QueueCtl implements Callable<Integer> {
    
    @Option(names = {"-c", "--config"}, description = "Configuration file path", defaultValue = "config.json")
    private String configFile;
    
    @Option(names = {"-d", "--data"}, description = "Data file path (overrides config)")
    private String dataFile;
    
    @Option(names = {"-v", "--verbose"}, description = "Enable verbose output")
    private boolean verbose;
    
    // Shared components - will be initialized once and reused
    private static ConfigManager configManager;
    private static PersistenceManager persistenceManager;
    private static JobQueue jobQueue;
    private static WorkerManager workerManager;
    private static DLQManager dlqManager;
    
    public static void main(String[] args) {
        int exitCode = new CommandLine(new QueueCtl()).execute(args);
        System.exit(exitCode);
    }
    
    @Override
    public Integer call() {
        // If no subcommand is specified, show help
        CommandLine.usage(this, System.out);
        return 0;
    }
    
    /**
     * Initialize shared components (lazy initialization)
     */
    public void initializeComponents() {
        if (configManager == null) {
            configManager = new ConfigManager(configFile);
            
            JobQueueConfig config = configManager.getConfig();
            
            // Override data file if specified
            if (dataFile != null) {
                config = config.withDataFile(dataFile);
                configManager.updateConfig(config);
            }
            
            persistenceManager = new PersistenceManager(config.getDataFile());
            jobQueue = new JobQueue(persistenceManager, config);
            workerManager = new WorkerManager(jobQueue, config);
            dlqManager = new DLQManager(persistenceManager, jobQueue);
        }
    }
    
    // Getters for shared components
    public static ConfigManager getConfigManager() {
        return configManager;
    }
    
    public static PersistenceManager getPersistenceManager() {
        return persistenceManager;
    }
    
    public static JobQueue getJobQueue() {
        return jobQueue;
    }
    
    public static WorkerManager getWorkerManager() {
        return workerManager;
    }
    
    public static DLQManager getDLQManager() {
        return dlqManager;
    }
    
    public String getConfigFile() {
        return configFile;
    }
    
    public String getDataFile() {
        return dataFile;
    }
    
    public boolean isVerbose() {
        return verbose;
    }
}
