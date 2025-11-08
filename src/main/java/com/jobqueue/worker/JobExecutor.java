package com.jobqueue.worker;

import com.jobqueue.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

/**
 * Executes shell commands for jobs.
 * Handles process execution, timeout, and result capture.
 */
public class JobExecutor {
    private static final Logger logger = LoggerFactory.getLogger(JobExecutor.class);
    
    private final long timeoutSeconds;
    
    public JobExecutor(long timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }
    
    /**
     * Execute a job's command
     */
    public JobExecutionResult execute(Job job) {
        String command = job.getCommand();
        logger.info("Executing job {}: {}", job.getId(), command);
        
        try {
            ProcessBuilder processBuilder = new ProcessBuilder();
            
            // Handle different operating systems
            if (System.getProperty("os.name").toLowerCase().contains("windows")) {
                processBuilder.command("cmd", "/c", command);
            } else {
                processBuilder.command("sh", "-c", command);
            }
            
            processBuilder.redirectErrorStream(true);
            
            Process process = processBuilder.start();
            
            // Capture output
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }
            
            // Wait for completion with timeout
            boolean finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            
            if (!finished) {
                // Process timed out
                process.destroyForcibly();
                String errorMsg = String.format("Job timed out after %d seconds", timeoutSeconds);
                logger.warn("Job {} timed out: {}", job.getId(), command);
                return new JobExecutionResult(false, errorMsg, output.toString());
            }
            
            int exitCode = process.exitValue();
            boolean success = exitCode == 0;
            
            String resultOutput = output.toString().trim();
            
            if (success) {
                logger.info("Job {} completed successfully: {}", job.getId(), command);
                return new JobExecutionResult(true, null, resultOutput);
            } else {
                String errorMsg = String.format("Command failed with exit code %d", exitCode);
                logger.warn("Job {} failed with exit code {}: {}", job.getId(), exitCode, command);
                return new JobExecutionResult(false, errorMsg, resultOutput);
            }
            
        } catch (IOException e) {
            String errorMsg = "Failed to start process: " + e.getMessage();
            logger.error("Job {} failed to start: {}", job.getId(), command, e);
            return new JobExecutionResult(false, errorMsg, "");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            String errorMsg = "Job execution interrupted: " + e.getMessage();
            logger.warn("Job {} interrupted: {}", job.getId(), command);
            return new JobExecutionResult(false, errorMsg, "");
        } catch (Exception e) {
            String errorMsg = "Unexpected error: " + e.getMessage();
            logger.error("Job {} failed with unexpected error: {}", job.getId(), command, e);
            return new JobExecutionResult(false, errorMsg, "");
        }
    }
    
    /**
     * Result of job execution
     */
    public static class JobExecutionResult {
        private final boolean success;
        private final String errorMessage;
        private final String output;
        
        public JobExecutionResult(boolean success, String errorMessage, String output) {
            this.success = success;
            this.errorMessage = errorMessage;
            this.output = output;
        }
        
        public boolean isSuccess() {
            return success;
        }
        
        public String getErrorMessage() {
            return errorMessage;
        }
        
        public String getOutput() {
            return output;
        }
        
        @Override
        public String toString() {
            return String.format("JobExecutionResult{success=%s, errorMessage='%s', output='%s'}", 
                               success, errorMessage, output);
        }
    }
}
