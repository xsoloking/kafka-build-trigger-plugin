package io.jenkins.plugins.kafkabuildtrigger;

import hudson.Extension;
import hudson.XmlFile;
import hudson.model.Saveable;
import hudson.model.listeners.SaveableListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Extension
public class SaveableListenerImpl extends SaveableListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaveableListenerImpl.class);

    @Override
    public final void onChange(Saveable o, XmlFile file) {
        if (o instanceof GlobalKafkaBuildTriggerConfig) {
            LOGGER.info("RabbitMQ configuration is updated, so update connection...");
            KafkaManager.getInstance().update();
        }
        super.onChange(o, file);
    }

    /**
     * Gets instance of this extension.
     *
     * @return the instance of this extension.
     */
    public static SaveableListenerImpl get() {
        return SaveableListener.all().get(SaveableListenerImpl.class);
    }
}