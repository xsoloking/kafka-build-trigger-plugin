package io.jenkins.plugins.kafkabuildtrigger.util;

import io.jenkins.plugins.kafkabuildtrigger.GlobalKafkaBuildTriggerConfig;
import io.jenkins.plugins.kafkabuildtrigger.exception.InvalidConfigurationException;
import hudson.util.Secret;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

/**
 * Unit tests for ConfigurationValidator.
 * 
 * @author Optimized by AI Assistant
 */
public class ConfigurationValidatorTest {
    
    private GlobalKafkaBuildTriggerConfig config;
    
    @Before
    public void setUp() {
        config = new GlobalKafkaBuildTriggerConfig();
        config.setEnableConsumer(true);
    }
    
    @Test
    public void testValidConfiguration() throws InvalidConfigurationException {
        config.setBrokers("localhost:9092");
        config.setTopic("test-topic");
        config.setGroupId("test-group");
        config.setUsername("testuser");
        config.setPassword(Secret.fromString("testpass"));
        
        // Should not throw exception
        ConfigurationValidator.validate(config);
    }
    
    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidBrokerFormat() throws InvalidConfigurationException {
        config.setBrokers("invalid-broker");
        config.setTopic("test-topic");
        config.setGroupId("test-group");
        config.setUsername("testuser");
        config.setPassword(Secret.fromString("testpass"));
        
        ConfigurationValidator.validate(config);
    }
    
    @Test(expected = InvalidConfigurationException.class)
    public void testEmptyTopic() throws InvalidConfigurationException {
        config.setBrokers("localhost:9092");
        config.setTopic("");
        config.setGroupId("test-group");
        config.setUsername("testuser");
        config.setPassword(Secret.fromString("testpass"));
        
        ConfigurationValidator.validate(config);
    }
    
    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidTopicName() throws InvalidConfigurationException {
        config.setBrokers("localhost:9092");
        config.setTopic("invalid topic with spaces");
        config.setGroupId("test-group");
        config.setUsername("testuser");
        config.setPassword(Secret.fromString("testpass"));
        
        ConfigurationValidator.validate(config);
    }
    
    @Test(expected = InvalidConfigurationException.class)
    public void testEmptyUsername() throws InvalidConfigurationException {
        config.setBrokers("localhost:9092");
        config.setTopic("test-topic");
        config.setGroupId("test-group");
        config.setUsername("");
        config.setPassword(Secret.fromString("testpass"));
        
        ConfigurationValidator.validate(config);
    }
    
    @Test(expected = InvalidConfigurationException.class)
    public void testNullPassword() throws InvalidConfigurationException {
        config.setBrokers("localhost:9092");
        config.setTopic("test-topic");
        config.setGroupId("test-group");
        config.setUsername("testuser");
        config.setPassword((Secret) null);
        
        ConfigurationValidator.validate(config);
    }
    
    @Test
    public void testValidBrokerFormats() throws InvalidConfigurationException {
        String[] validBrokers = {
            "localhost:9092",
            "broker1:9092,broker2:9092",
            "192.168.1.100:9092",
            "kafka.example.com:9092"
        };
        
        for (String broker : validBrokers) {
            config.setBrokers(broker);
            config.setTopic("test-topic");
            config.setGroupId("test-group");
            config.setUsername("testuser");
            config.setPassword(Secret.fromString("testpass"));
            
            // Should not throw exception
            ConfigurationValidator.validate(config);
        }
    }
    
    @Test
    public void testValidTopicNames() throws InvalidConfigurationException {
        String[] validTopics = {
            "test-topic",
            "test_topic",
            "test.topic",
            "TestTopic123"
        };
        
        for (String topic : validTopics) {
            config.setBrokers("localhost:9092");
            config.setTopic(topic);
            config.setGroupId("test-group");
            config.setUsername("testuser");
            config.setPassword(Secret.fromString("testpass"));
            
            // Should not throw exception
            ConfigurationValidator.validate(config);
        }
    }
    
    @Test
    public void testDisabledConsumerSkipsValidation() throws InvalidConfigurationException {
        config.setEnableConsumer(false);
        // Set invalid configuration
        config.setBrokers("invalid");
        config.setTopic("");
        
        // Should not throw exception when consumer is disabled
        ConfigurationValidator.validate(config);
    }
}
