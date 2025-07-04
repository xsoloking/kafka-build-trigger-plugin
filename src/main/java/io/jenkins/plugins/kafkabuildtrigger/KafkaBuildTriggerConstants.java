package io.jenkins.plugins.kafkabuildtrigger;

/**
 * Constants for Kafka Build Trigger Plugin.
 * 
 * @author Optimized by AI Assistant
 */
public final class KafkaBuildTriggerConstants {
    
    // Plugin Information
    public static final String PLUGIN_NAME = "Kafka Build Trigger";
    public static final String PLUGIN_APPID = "remote-build";
    
    // Configuration
    public static final String CONTENT_NONE = "-";
    public static final String DEFAULT_GROUP_ID = "jenkins-kafka-consumer";
    
    // Message Processing
    public static final String KEY_PARAM_NAME = "name";
    public static final String KEY_PARAM_VALUE = "value";
    
    // Timeouts and Retry Configuration
    public static final long TIMEOUT_CLOSE_MS = 5000L;
    public static final int MAX_RETRY_TIMES = 3;
    public static final int SLEEP_SECONDS = 10;
    public static final int KAFKA_POLL_TIMEOUT_MS = 100;
    
    // Kafka Consumer Configuration
    public static final String AUTO_OFFSET_RESET_LATEST = "latest";
    public static final String KEY_DESERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String VALUE_DESERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringDeserializer";
    
    // Security
    public static final String SASL_MECHANISM_PLAIN = "PLAIN";
    public static final String SECURITY_PROTOCOL_SASL_PLAINTEXT = "SASL_PLAINTEXT";
    
    // Private constructor to prevent instantiation
    private KafkaBuildTriggerConstants() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
}
