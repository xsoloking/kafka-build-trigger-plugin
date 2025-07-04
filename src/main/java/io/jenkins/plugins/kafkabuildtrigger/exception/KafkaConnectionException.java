package io.jenkins.plugins.kafkabuildtrigger.exception;

/**
 * Exception thrown when Kafka connection issues occur.
 * 
 * @author Optimized by AI Assistant
 */
public class KafkaConnectionException extends KafkaBuildTriggerException {
    
    private static final long serialVersionUID = 1L;
    
    public KafkaConnectionException(String message) {
        super(message);
    }
    
    public KafkaConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public KafkaConnectionException(Throwable cause) {
        super(cause);
    }
}
