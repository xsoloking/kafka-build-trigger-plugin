package io.jenkins.plugins.kafkabuildtrigger.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Utility class for collecting metrics about Kafka Build Trigger operations.
 * 
 * @author Optimized by AI Assistant
 */
public final class MetricsCollector {
    
    private static final MetricsCollector INSTANCE = new MetricsCollector();
    
    private final AtomicLong messagesReceived = new AtomicLong(0);
    private final AtomicLong messagesProcessed = new AtomicLong(0);
    private final AtomicLong messagesRejected = new AtomicLong(0);
    private final AtomicLong buildsTriggered = new AtomicLong(0);
    private final AtomicLong connectionErrors = new AtomicLong(0);
    
    private final Map<String, AtomicLong> projectMetrics = new ConcurrentHashMap<>();
    
    private MetricsCollector() {
        // Private constructor for singleton
    }
    
    public static MetricsCollector getInstance() {
        return INSTANCE;
    }
    
    /**
     * Increments the count of messages received.
     */
    public void incrementMessagesReceived() {
        messagesReceived.incrementAndGet();
    }
    
    /**
     * Increments the count of messages processed successfully.
     */
    public void incrementMessagesProcessed() {
        messagesProcessed.incrementAndGet();
    }
    
    /**
     * Increments the count of messages rejected due to validation or other issues.
     */
    public void incrementMessagesRejected() {
        messagesRejected.incrementAndGet();
    }
    
    /**
     * Increments the count of builds triggered.
     * 
     * @param projectName the name of the project for which build was triggered
     */
    public void incrementBuildsTriggered(String projectName) {
        buildsTriggered.incrementAndGet();
        projectMetrics.computeIfAbsent(projectName, k -> new AtomicLong(0)).incrementAndGet();
    }
    
    /**
     * Increments the count of connection errors.
     */
    public void incrementConnectionErrors() {
        connectionErrors.incrementAndGet();
    }
    
    /**
     * Gets the total number of messages received.
     * 
     * @return messages received count
     */
    public long getMessagesReceived() {
        return messagesReceived.get();
    }
    
    /**
     * Gets the total number of messages processed.
     * 
     * @return messages processed count
     */
    public long getMessagesProcessed() {
        return messagesProcessed.get();
    }
    
    /**
     * Gets the total number of messages rejected.
     * 
     * @return messages rejected count
     */
    public long getMessagesRejected() {
        return messagesRejected.get();
    }
    
    /**
     * Gets the total number of builds triggered.
     * 
     * @return builds triggered count
     */
    public long getBuildsTriggered() {
        return buildsTriggered.get();
    }
    
    /**
     * Gets the total number of connection errors.
     * 
     * @return connection errors count
     */
    public long getConnectionErrors() {
        return connectionErrors.get();
    }
    
    /**
     * Gets build count for a specific project.
     * 
     * @param projectName the project name
     * @return build count for the project
     */
    public long getProjectBuildCount(String projectName) {
        AtomicLong count = projectMetrics.get(projectName);
        return count != null ? count.get() : 0;
    }
    
    /**
     * Gets all project metrics.
     * 
     * @return map of project names to build counts
     */
    public Map<String, Long> getAllProjectMetrics() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        projectMetrics.forEach((project, count) -> result.put(project, count.get()));
        return result;
    }
    
    /**
     * Resets all metrics to zero.
     */
    public void reset() {
        messagesReceived.set(0);
        messagesProcessed.set(0);
        messagesRejected.set(0);
        buildsTriggered.set(0);
        connectionErrors.set(0);
        projectMetrics.clear();
    }
    
    /**
     * Gets a summary of all metrics as a formatted string.
     * 
     * @return metrics summary
     */
    public String getMetricsSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("Kafka Build Trigger Metrics:\n");
        sb.append("Messages Received: ").append(getMessagesReceived()).append("\n");
        sb.append("Messages Processed: ").append(getMessagesProcessed()).append("\n");
        sb.append("Messages Rejected: ").append(getMessagesRejected()).append("\n");
        sb.append("Builds Triggered: ").append(getBuildsTriggered()).append("\n");
        sb.append("Connection Errors: ").append(getConnectionErrors()).append("\n");
        
        if (!projectMetrics.isEmpty()) {
            sb.append("Project Build Counts:\n");
            projectMetrics.forEach((project, count) -> 
                sb.append("  ").append(project).append(": ").append(count.get()).append("\n"));
        }
        
        return sb.toString();
    }
}
