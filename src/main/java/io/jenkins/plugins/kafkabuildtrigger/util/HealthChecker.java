package io.jenkins.plugins.kafkabuildtrigger.util;

import io.jenkins.plugins.kafkabuildtrigger.GlobalKafkaBuildTriggerConfig;
import io.jenkins.plugins.kafkabuildtrigger.KafkaManager;
import io.jenkins.plugins.kafkabuildtrigger.TriggerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for performing health checks on the Kafka Build Trigger plugin.
 * 
 * @author Optimized by AI Assistant
 */
public final class HealthChecker {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(HealthChecker.class);
    
    private HealthChecker() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
    
    /**
     * Performs a comprehensive health check of the plugin.
     * 
     * @return health check results
     */
    public static HealthCheckResult performHealthCheck() {
        HealthCheckResult result = new HealthCheckResult();
        
        try {
            // Check configuration
            checkConfiguration(result);
            
            // Check Kafka connection
            checkKafkaConnection(result);
            
            // Check trigger manager
            checkTriggerManager(result);
            
            // Check metrics
            checkMetrics(result);
            
        } catch (Exception e) {
            LOGGER.error("Error during health check", e);
            result.addError("Health check failed: " + e.getMessage());
        }
        
        return result;
    }
    
    private static void checkConfiguration(HealthCheckResult result) {
        try {
            GlobalKafkaBuildTriggerConfig config = GlobalKafkaBuildTriggerConfig.get();
            if (config == null) {
                result.addError("Configuration is null");
                return;
            }
            
            if (config.isEnableConsumer()) {
                if (config.getBrokers() == null || config.getBrokers().trim().isEmpty()) {
                    result.addError("Brokers configuration is empty");
                }
                
                if (config.getTopic() == null || config.getTopic().trim().isEmpty()) {
                    result.addError("Topic configuration is empty");
                }
                
                if (config.getUsername() == null || config.getUsername().trim().isEmpty()) {
                    result.addError("Username configuration is empty");
                }
                
                if (config.getPassword() == null) {
                    result.addError("Password configuration is empty");
                }
            }
            
            result.addInfo("Configuration check completed");
            
        } catch (Exception e) {
            result.addError("Configuration check failed: " + e.getMessage());
        }
    }
    
    private static void checkKafkaConnection(HealthCheckResult result) {
        try {
            KafkaManager kafkaManager = KafkaManager.getInstance();
            if (kafkaManager == null) {
                result.addError("KafkaManager instance is null");
                return;
            }
            
            if (kafkaManager.isOpen()) {
                result.addInfo("Kafka connection is open");
            } else {
                result.addWarning("Kafka connection is not open");
            }
            
        } catch (Exception e) {
            result.addError("Kafka connection check failed: " + e.getMessage());
        }
    }
    
    private static void checkTriggerManager(HealthCheckResult result) {
        try {
            TriggerManager triggerManager = TriggerManager.getInstance();
            if (triggerManager == null) {
                result.addError("TriggerManager instance is null");
                return;
            }
            
            int triggerCount = triggerManager.getTriggers().size();
            result.addInfo("Active triggers: " + triggerCount);
            
            if (triggerCount == 0) {
                result.addWarning("No active triggers found");
            }
            
        } catch (Exception e) {
            result.addError("Trigger manager check failed: " + e.getMessage());
        }
    }
    
    private static void checkMetrics(HealthCheckResult result) {
        try {
            MetricsCollector metrics = MetricsCollector.getInstance();
            if (metrics == null) {
                result.addError("MetricsCollector instance is null");
                return;
            }
            
            result.addInfo("Messages received: " + metrics.getMessagesReceived());
            result.addInfo("Messages processed: " + metrics.getMessagesProcessed());
            result.addInfo("Messages rejected: " + metrics.getMessagesRejected());
            result.addInfo("Builds triggered: " + metrics.getBuildsTriggered());
            result.addInfo("Connection errors: " + metrics.getConnectionErrors());
            
        } catch (Exception e) {
            result.addError("Metrics check failed: " + e.getMessage());
        }
    }
    
    /**
     * Health check result container.
     */
    public static class HealthCheckResult {
        private final Map<String, String> errors = new HashMap<>();
        private final Map<String, String> warnings = new HashMap<>();
        private final Map<String, String> info = new HashMap<>();
        
        public void addError(String message) {
            errors.put("error_" + errors.size(), message);
        }
        
        public void addWarning(String message) {
            warnings.put("warning_" + warnings.size(), message);
        }
        
        public void addInfo(String message) {
            info.put("info_" + info.size(), message);
        }
        
        public boolean isHealthy() {
            return errors.isEmpty();
        }
        
        public Map<String, String> getErrors() {
            return new HashMap<>(errors);
        }
        
        public Map<String, String> getWarnings() {
            return new HashMap<>(warnings);
        }
        
        public Map<String, String> getInfo() {
            return new HashMap<>(info);
        }
        
        public String getSummary() {
            StringBuilder sb = new StringBuilder();
            sb.append("Health Check Summary:\n");
            sb.append("Status: ").append(isHealthy() ? "HEALTHY" : "UNHEALTHY").append("\n");
            
            if (!errors.isEmpty()) {
                sb.append("Errors:\n");
                errors.values().forEach(error -> sb.append("  - ").append(error).append("\n"));
            }
            
            if (!warnings.isEmpty()) {
                sb.append("Warnings:\n");
                warnings.values().forEach(warning -> sb.append("  - ").append(warning).append("\n"));
            }
            
            if (!info.isEmpty()) {
                sb.append("Information:\n");
                info.values().forEach(infoMsg -> sb.append("  - ").append(infoMsg).append("\n"));
            }
            
            return sb.toString();
        }
    }
}
