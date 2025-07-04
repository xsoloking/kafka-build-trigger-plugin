package io.jenkins.plugins.kafkabuildtrigger.service;

import io.jenkins.plugins.kafkabuildtrigger.exception.KafkaConnectionException;

/**
 * Interface for managing Kafka connections.
 * 
 * @author Optimized by AI Assistant
 */
public interface ConnectionManager {
    
    /**
     * Updates the connection with new configuration.
     * 
     * @throws KafkaConnectionException if connection update fails
     */
    void update() throws KafkaConnectionException;
    
    /**
     * Shuts down the connection gracefully.
     * 
     * @throws InterruptedException if shutdown is interrupted
     */
    void shutdown() throws InterruptedException;
    
    /**
     * Checks if the connection is currently open.
     * 
     * @return true if connection is open
     */
    boolean isOpen();
    
    /**
     * Gets connection status information.
     * 
     * @return connection status as string
     */
    String getConnectionStatus();
}
