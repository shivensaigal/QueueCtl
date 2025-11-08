package com.jobqueue.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobqueue.model.Job;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.util.concurrent.Callable;

/**
 * CLI command for enqueuing jobs.
 */
@Command(
    name = "enqueue",
    description = "Add a new job to the queue"
)
public class EnqueueCommand implements Callable<Integer> {
    
    @ParentCommand
    private QueueCtl parent;
    
    @Parameters(
        index = "0",
        description = "Job specification as JSON string or simple command"
    )
    private String jobSpec;
    
    @Option(
        names = {"-r", "--max-retries"},
        description = "Maximum number of retries (overrides config default)"
    )
    private Integer maxRetries;
    
    @Override
    public Integer call() {
        try {
            parent.initializeComponents();
            
            String command;
            int retries = maxRetries != null ? maxRetries : 
                         QueueCtl.getConfigManager().getConfig().getMaxRetries();
            
            // Try to parse as JSON first
            if (jobSpec.trim().startsWith("{")) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jobNode = mapper.readTree(jobSpec);
                
                if (jobNode.has("command")) {
                    command = jobNode.get("command").asText();
                    
                    if (jobNode.has("max_retries")) {
                        retries = jobNode.get("max_retries").asInt();
                    }
                } else {
                    System.err.println("Error: JSON job specification must contain 'command' field");
                    return 1;
                }
            } else {
                // Treat as simple command
                command = jobSpec;
            }
            
            // Validate command
            if (command == null || command.trim().isEmpty()) {
                System.err.println("Error: Command cannot be empty");
                return 1;
            }
            
            // Enqueue the job
            String jobId = QueueCtl.getJobQueue().enqueue(command, retries);
            
            System.out.println("Job enqueued successfully:");
            System.out.println("  Job ID: " + jobId);
            System.out.println("  Command: " + command);
            System.out.println("  Max Retries: " + retries);
            
            return 0;
            
        } catch (Exception e) {
            System.err.println("Error enqueuing job: " + e.getMessage());
            if (parent.isVerbose()) {
                e.printStackTrace();
            }
            return 1;
        }
    }
}
