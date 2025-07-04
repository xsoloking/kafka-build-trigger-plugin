package io.jenkins.plugins.kafkabuildtrigger.exception;

/**
 * Exception thrown when configuration validation fails.
 * 
 * @author Optimized by AI Assistant
 */
public class InvalidConfigurationException extends KafkaBuildTriggerException {
    
    private static final long serialVersionUID = 1L;
    
    public InvalidConfigurationException(String message) {
        super(message);
    }
    
    public InvalidConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
