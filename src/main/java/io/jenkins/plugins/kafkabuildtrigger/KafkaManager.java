package io.jenkins.plugins.kafkabuildtrigger;

import hudson.util.Secret;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class KafkaManager {

    /**
     * Intance holder class for {@link KafkaManager}.
     *
     * @author Radhika Gupta
     */
    private static class InstanceHolder {
        private static final KafkaManager INSTANCE = new KafkaManager();
    }

    private static final long TIMEOUT_CLOSE = 300000;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaManager.class);

    private KafkaConsumerHandler kafkaConnection;
    private volatile boolean statusOpen = false;
    private CountDownLatch closeLatch = null;

    /**
     * Gets instance.
     *
     * @return the instance.
     */
    public static KafkaManager getInstance() {
        return InstanceHolder.INSTANCE;
    }

    /**
     * Updates underlying kafka connection.
     */
    public void update() {
        LOGGER.info("Start to update connections...");

        GlobalKafkaBuildTriggerConfig conf = GlobalKafkaBuildTriggerConfig.get();
        LOGGER.info("brokers: {} enableConsumer: {} topicName: {}", conf.getBrokers(),
                conf.isEnableConsumer(), conf.getTopic());
        String brokers = conf.getBrokers();
        String topicName = conf.getTopic();
        boolean enableConsumer = conf.isEnableConsumer();

        try {
            if (!enableConsumer || brokers == null) {
                if (kafkaConnection != null) {
                    shutdownWithWait();
                    kafkaConnection = null;
                }
            }

            if (kafkaConnection != null &&
                    !brokers.equals(kafkaConnection.getBrokers()) &&
                    !topicName.equals(kafkaConnection.getTopicName())) {
                if (kafkaConnection != null) {
                    shutdownWithWait();
                    kafkaConnection = null;
                }
            }

            if (enableConsumer) {
                if (kafkaConnection == null) {
                    kafkaConnection = new KafkaConsumerHandler(conf);
                    try {
                        kafkaConnection.enableConsumerThread();
                        statusOpen = true;
                    } catch (Exception e) {
                        LOGGER.warn("Cannot open connection!", e);
                        kafkaConnection = null;
                    }
                } else {
                    kafkaConnection.updateConf(conf);
                    // TODO: reconnect
                }
            }
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted when waiting to close connection.");
        }
    }

    /**
     * Shutdown connection.
     */
    public void shutdown() {
        if (kafkaConnection != null && kafkaConnection.isConsumerThreadEnabled()) {
            try {
                statusOpen = false;
                kafkaConnection.disableConsumerThread();
            } catch(Exception ex) {
                onCloseCompleted(kafkaConnection);
            }
        }
    }

    /**
     * Shutdown connection then wait to close connection.
     *
     * @throws InterruptedException
     *             throw if wait process is interrupted.
     */
    public synchronized void shutdownWithWait() throws InterruptedException {
        if (kafkaConnection != null && kafkaConnection.isConsumerThreadEnabled()) {
            try {
                closeLatch = new CountDownLatch(1);
                shutdown();
                if (!closeLatch.await(TIMEOUT_CLOSE, TimeUnit.MILLISECONDS)) {
                    onCloseCompleted(kafkaConnection);
                    throw new InterruptedException("Wait timeout");
                }
            } finally {
                closeLatch = null;
            }
        }
    }

    /**
     * Gets whether connection is established or not.
     *
     * @return true if connection is already established.
     */
    public boolean isOpen() {
        return statusOpen;
    }


    /**
     * @param kafkaConnection
     *            the connection.
     */
    public void onCloseCompleted(KafkaConsumerHandler kafkaConnection) {
        if (this.kafkaConnection != null && this.kafkaConnection.equals(kafkaConnection)) {
            this.kafkaConnection = null;
            LOGGER.info("Closed RabbitMQ connection: {}",kafkaConnection.getBrokers());
            kafkaConnection.disableConsumerThread();
            kafkaConnection = null;
            //kafkaConnection.removeRMQConnectionListener(this);
            //ServerOperator.fireOnCloseCompleted(kafkaConnection);
            statusOpen = false;
            if (closeLatch != null) {
                closeLatch.countDown();
            }
        }
    }

    /**
     * Creates instance.
     */
    private KafkaManager() {
    }
}