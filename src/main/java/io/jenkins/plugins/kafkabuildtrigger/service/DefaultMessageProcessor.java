package io.jenkins.plugins.kafkabuildtrigger.service;

import io.jenkins.plugins.kafkabuildtrigger.RemoteBuildTrigger;
import io.jenkins.plugins.kafkabuildtrigger.TriggerManager;
import io.jenkins.plugins.kafkabuildtrigger.model.BuildMessage;
import io.jenkins.plugins.kafkabuildtrigger.util.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of MessageProcessor.
 * 
 * @author Optimized by AI Assistant
 */
public class DefaultMessageProcessor implements MessageProcessor {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMessageProcessor.class);

    private final TriggerManager triggerManager;
    private final MetricsCollector metricsCollector;
    
    public DefaultMessageProcessor(TriggerManager triggerManager) {
        this.triggerManager = triggerManager;
        this.metricsCollector = MetricsCollector.getInstance();
    }
    
    @Override
    public void processMessage(BuildMessage message, String topicName) {
        metricsCollector.incrementMessagesReceived();

        if (!canProcess(message)) {
            LOGGER.warn("Cannot process invalid message: {}", message);
            metricsCollector.incrementMessagesRejected();
            return;
        }

        LOGGER.info("Processing build message for project: {}", message.getProject());

        boolean messageProcessed = false;

        for (RemoteBuildTrigger trigger : triggerManager.getTriggers()) {
            if (shouldTriggerBuild(trigger, message)) {
                LOGGER.info("Triggering build for project: {} with token: {}",
                           trigger.getProjectName(), trigger.getRemoteBuildToken());

                trigger.scheduleBuild(topicName, message.getParameter());
                metricsCollector.incrementBuildsTriggered(trigger.getProjectName());
                messageProcessed = true;
            }
        }

        if (messageProcessed) {
            metricsCollector.incrementMessagesProcessed();
        } else {
            LOGGER.warn("No matching trigger found for project: {} with token: {}",
                       message.getProject(), message.getToken());
            metricsCollector.incrementMessagesRejected();
        }
    }
    
    @Override
    public boolean canProcess(BuildMessage message) {
        return message != null && message.isValid();
    }
    
    /**
     * Determines if a trigger should be activated for the given message.
     * 
     * @param trigger the trigger to check
     * @param message the build message
     * @return true if the trigger should be activated
     */
    private boolean shouldTriggerBuild(RemoteBuildTrigger trigger, BuildMessage message) {
        if (trigger.getRemoteBuildToken() == null) {
            LOGGER.warn("Ignoring trigger for project {}: no token set", trigger.getProjectName());
            return false;
        }
        
        return trigger.getProjectName().equals(message.getProject()) &&
               trigger.getRemoteBuildToken().equals(message.getToken());
    }
}
