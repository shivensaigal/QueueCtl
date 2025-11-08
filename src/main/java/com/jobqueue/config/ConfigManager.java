package com.jobqueue.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages configuration for the Job Queue System.
 * Handles loading, saving, and updating configuration with thread safety.
 */
public class ConfigManager {
    private static final Logger logger = LoggerFactory.getLogger(ConfigManager.class);
    private static final String DEFAULT_CONFIG_FILE = "config.json";
    
    private final ObjectMapper objectMapper;
    private final String configFile;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private JobQueueConfig config;
    
    public ConfigManager() {
        this(DEFAULT_CONFIG_FILE);
    }
    
    public ConfigManager(String configFile) {
        this.configFile = configFile;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        
        // Load or create default configuration
        this.config = loadConfig();
    }
    
    /**
     * Get current configuration (thread-safe read)
     */
    public JobQueueConfig getConfig() {
        lock.readLock().lock();
        try {
            return config;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Update configuration and persist to file
     */
    public void updateConfig(JobQueueConfig newConfig) {
        lock.writeLock().lock();
        try {
            this.config = newConfig;
            saveConfig();
            logger.info("Configuration updated: {}", newConfig);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Update a specific configuration parameter
     */
    public void updateMaxRetries(int maxRetries) {
        updateConfig(getConfig().withMaxRetries(maxRetries));
    }
    
    public void updateBackoffBase(int backoffBase) {
        updateConfig(getConfig().withBackoffBase(backoffBase));
    }
    
    public void updateWorkerCount(int workerCount) {
        updateConfig(getConfig().withWorkerCount(workerCount));
    }
    
    public void updateDataFile(String dataFile) {
        updateConfig(getConfig().withDataFile(dataFile));
    }
    
    public void updateJobTimeout(long jobTimeoutSeconds) {
        updateConfig(getConfig().withJobTimeout(jobTimeoutSeconds));
    }
    
    public void updateRetryCheckInterval(long retryCheckIntervalSeconds) {
        updateConfig(getConfig().withRetryCheckInterval(retryCheckIntervalSeconds));
    }
    
    /**
     * Load configuration from file or create default
     */
    private JobQueueConfig loadConfig() {
        Path configPath = Paths.get(configFile);
        
        if (!Files.exists(configPath)) {
            logger.info("Configuration file {} not found, creating default configuration", configFile);
            JobQueueConfig defaultConfig = new JobQueueConfig();
            saveConfigInternal(defaultConfig);
            return defaultConfig;
        }
        
        try {
            String content = Files.readString(configPath);
            JobQueueConfig loadedConfig = objectMapper.readValue(content, JobQueueConfig.class);
            logger.info("Configuration loaded from {}: {}", configFile, loadedConfig);
            return loadedConfig;
        } catch (IOException e) {
            logger.error("Failed to load configuration from {}, using defaults", configFile, e);
            return new JobQueueConfig();
        }
    }
    
    /**
     * Save current configuration to file
     */
    private void saveConfig() {
        saveConfigInternal(this.config);
    }
    
    /**
     * Save specific configuration to file
     */
    private void saveConfigInternal(JobQueueConfig config) {
        try {
            // Ensure parent directory exists
            Path configPath = Paths.get(configFile);
            Path parentDir = configPath.getParent();
            if (parentDir != null && !Files.exists(parentDir)) {
                Files.createDirectories(parentDir);
            }
            
            String json = objectMapper.writerWithDefaultPrettyPrinter()
                                    .writeValueAsString(config);
            Files.writeString(configPath, json);
            logger.debug("Configuration saved to {}", configFile);
        } catch (IOException e) {
            logger.error("Failed to save configuration to {}", configFile, e);
            throw new RuntimeException("Failed to save configuration", e);
        }
    }
    
    /**
     * Reload configuration from file
     */
    public void reloadConfig() {
        lock.writeLock().lock();
        try {
            this.config = loadConfig();
            logger.info("Configuration reloaded from {}", configFile);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Get configuration file path
     */
    public String getConfigFile() {
        return configFile;
    }
}
