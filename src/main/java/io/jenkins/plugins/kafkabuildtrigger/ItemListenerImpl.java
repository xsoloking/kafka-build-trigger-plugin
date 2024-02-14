package io.jenkins.plugins.kafkabuildtrigger;

import groovy.util.logging.Slf4j;
import hudson.Extension;
import hudson.model.listeners.ItemListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Extension
public class ItemListenerImpl extends ItemListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ItemListenerImpl.class);
    private final KafkaManager kafkaManager;

    public ItemListenerImpl() {
        super();
        this.kafkaManager = KafkaManager.getInstance();
    }

    @Override
    public final void onLoaded() {
        LOGGER.info("Start bootup process.");
        kafkaManager.update();
        super.onLoaded();
    }

    @Override
    public final void onBeforeShutdown() {
        kafkaManager.shutdown();
        super.onBeforeShutdown();
    }

    /**
     * Gets this extension's instance.
     *
     * @return the instance of this extension.
     */
    public static ItemListenerImpl get() {
        return ItemListener.all().get(ItemListenerImpl.class);
    }
}
