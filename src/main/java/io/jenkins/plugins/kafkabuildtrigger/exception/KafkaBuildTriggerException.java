package io.jenkins.plugins.kafkabuildtrigger.exception;

/**
 * Base exception for Kafka Build Trigger Plugin.
 * 
 * @author Optimized by AI Assistant
 */
public class KafkaBuildTriggerException extends Exception {
    
    private static final long serialVersionUID = 1L;
    
    public KafkaBuildTriggerException(String message) {
        super(message);
    }
    
    public KafkaBuildTriggerException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public KafkaBuildTriggerException(Throwable cause) {
        super(cause);
    }
}
