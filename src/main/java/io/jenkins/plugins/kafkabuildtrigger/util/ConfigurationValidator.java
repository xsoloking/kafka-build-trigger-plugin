package io.jenkins.plugins.kafkabuildtrigger.util;

import io.jenkins.plugins.kafkabuildtrigger.GlobalKafkaBuildTriggerConfig;
import io.jenkins.plugins.kafkabuildtrigger.exception.InvalidConfigurationException;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Utility class for validating Kafka Build Trigger configuration.
 * 
 * @author Optimized by AI Assistant
 */
public final class ConfigurationValidator {
    
    private static final Pattern BROKER_PATTERN = Pattern.compile("^[a-zA-Z0-9.-]+:[0-9]+$");
    private static final Pattern TOPIC_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]+$");
    private static final Pattern GROUP_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]+$");
    
    private ConfigurationValidator() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
    
    /**
     * Validates the global configuration.
     * 
     * @param config the configuration to validate
     * @throws InvalidConfigurationException if validation fails
     */
    public static void validate(GlobalKafkaBuildTriggerConfig config) throws InvalidConfigurationException {
        if (config == null) {
            throw new InvalidConfigurationException("Configuration cannot be null");
        }
        
        List<String> errors = new ArrayList<>();
        
        // Validate brokers
        if (config.isEnableConsumer()) {
            validateBrokers(config.getBrokers(), errors);
            validateTopic(config.getTopic(), errors);
            validateGroupId(config.getGroupId(), errors);
            validateCredentials(config.getUsername(), errors);
        }
        
        if (!errors.isEmpty()) {
            throw new InvalidConfigurationException("Configuration validation failed: " + String.join(", ", errors));
        }
    }
    
    private static void validateBrokers(String brokers, List<String> errors) {
        if (StringUtils.isBlank(brokers)) {
            errors.add("Brokers cannot be empty when consumer is enabled");
            return;
        }
        
        String[] brokerList = brokers.split(",");
        for (String broker : brokerList) {
            String trimmedBroker = broker.trim();
            if (!BROKER_PATTERN.matcher(trimmedBroker).matches()) {
                errors.add("Invalid broker format: " + trimmedBroker + ". Expected format: host:port");
            }
        }
    }
    
    private static void validateTopic(String topic, List<String> errors) {
        if (StringUtils.isBlank(topic)) {
            errors.add("Topic cannot be empty when consumer is enabled");
            return;
        }
        
        if (!TOPIC_PATTERN.matcher(topic).matches()) {
            errors.add("Invalid topic name: " + topic + ". Topic names can only contain letters, numbers, dots, underscores, and hyphens");
        }
    }
    
    private static void validateGroupId(String groupId, List<String> errors) {
        if (StringUtils.isNotBlank(groupId) && !GROUP_ID_PATTERN.matcher(groupId).matches()) {
            errors.add("Invalid group ID: " + groupId + ". Group IDs can only contain letters, numbers, dots, underscores, and hyphens");
        }
    }
    
    private static void validateCredentials(String username, List<String> errors) {
        if (StringUtils.isBlank(username)) {
            errors.add("Username cannot be empty when consumer is enabled");
        }
    }
}
