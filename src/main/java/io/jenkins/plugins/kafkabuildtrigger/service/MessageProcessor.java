package io.jenkins.plugins.kafkabuildtrigger.service;

import io.jenkins.plugins.kafkabuildtrigger.model.BuildMessage;

/**
 * Interface for processing build messages from Kafka.
 * 
 * @author Optimized by AI Assistant
 */
public interface MessageProcessor {
    
    /**
     * Processes a build message and triggers appropriate builds.
     * 
     * @param message the build message to process
     * @param topicName the topic name from which the message was received
     */
    void processMessage(BuildMessage message, String topicName);
    
    /**
     * Checks if the processor can handle the given message.
     * 
     * @param message the build message to check
     * @return true if the processor can handle the message
     */
    boolean canProcess(BuildMessage message);
}
