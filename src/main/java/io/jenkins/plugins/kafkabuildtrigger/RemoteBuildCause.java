package io.jenkins.plugins.kafkabuildtrigger;

import hudson.model.Cause;
import org.kohsuke.stapler.export.Exported;

/**
 * Cause class for remote build.
 */
 public class RemoteBuildCause extends Cause {

    private final String topicName;

    /**
     * Creates instance with specified parameter.
     *
     * @param topicName
     *            the queue name.
     */
    public RemoteBuildCause(String topicName) {
        this.topicName = topicName;
    }

    @Override
    @Exported(visibility = 3)
    public String getShortDescription() {
        return "Triggered by remote build message from Kafka topic: " + topicName;
    }

}