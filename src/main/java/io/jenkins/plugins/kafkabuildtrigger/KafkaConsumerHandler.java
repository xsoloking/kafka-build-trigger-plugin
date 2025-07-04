package io.jenkins.plugins.kafkabuildtrigger;

import com.fasterxml.jackson.databind.ObjectMapper;
import hudson.util.Secret;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.jenkins.plugins.kafkabuildtrigger.model.BuildMessage;
import io.jenkins.plugins.kafkabuildtrigger.service.DefaultMessageProcessor;
import io.jenkins.plugins.kafkabuildtrigger.service.MessageProcessor;
import io.jenkins.plugins.kafkabuildtrigger.util.MetricsCollector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Properties;

import static io.jenkins.plugins.kafkabuildtrigger.KafkaBuildTriggerConstants.*;



public class KafkaConsumerHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerHandler.class);
    private String brokers;
    private String username;
    private String password;
    private String topicName;
    private String groupId;
    private MessageProcessor messageProcessor;
    private ConsumerThread consumerThread;

    public String getBrokers(){
        return brokers;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getGroupId() {
        return groupId;
    }

    public KafkaConsumerHandler(GlobalKafkaBuildTriggerConfig config) {
        this.brokers = config.getBrokers();
        this.topicName = config.getTopic();
        this.groupId = (config.getGroupId() != null && !config.getGroupId().isEmpty())
                       ? config.getGroupId()
                       : DEFAULT_GROUP_ID;
        this.username = config.getUsername();
        this.password = Secret.toString(config.getPassword());
        this.messageProcessor = new DefaultMessageProcessor(TriggerManager.getInstance());
    }

    public boolean isConsumerThreadEnabled(){
        if (consumerThread != null && consumerThread.isAlive()) {
            return true;
        }
        return false;
    }

    public void enableConsumerThread() {
        if (!isConsumerThreadEnabled()) {
            LOGGER.info("Enabling consumer, broker: {}, groupId: {}", brokers, groupId);
            consumerThread = new ConsumerThread(brokers, topicName, groupId, username, password);
            consumerThread.start();
        }

    }

    public void disableConsumerThread() {
        if (isConsumerThreadEnabled()) {
            consumerThread.getKafkaConsumer().close();
            LOGGER.info("Stopping consumer .....");
            try {
                consumerThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        consumerThread = null;
    }

    public void updateConf(GlobalKafkaBuildTriggerConfig config) {
        this.brokers = config.getBrokers();
        this.topicName = config.getTopic();
        if (config.getGroupId() != null && ! config.getGroupId().isEmpty()) {
            this.groupId = config.getGroupId();
        }
        this.username = config.getUsername();
        this.password = Secret.toString(config.getPassword());
    }


    private class ConsumerThread extends Thread{

        private String broker;
        private String username;
        private String password;
        private String topicName;
        private String groupId;
        private KafkaConsumer<String,LinkedHashMap> kafkaConsumer;

        public ConsumerThread(String broker, String topicName, String groupId, String username, String password) {
            this.broker = broker;
            this.username = username;
            this.password = password;
            this.topicName = topicName;
            this.groupId = groupId;
        }

        public void run() throws WakeupException {

            Properties configProperties = new Properties();
            String jaasConfig = String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";", username, password);
            configProperties.setProperty("security.protocol", SECURITY_PROTOCOL_SASL_PLAINTEXT);
            configProperties.setProperty("sasl.mechanism", "SCRAM-SHA-512");
            configProperties.setProperty("sasl.jaas.config", jaasConfig);
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_LATEST);

            //Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, LinkedHashMap>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));

            ObjectMapper mapper = new ObjectMapper();
            //Start processing messages
            try {
                while (true) {
                    ConsumerRecords<String, LinkedHashMap> records = kafkaConsumer.poll(KAFKA_POLL_TIMEOUT_MS);
                    for (ConsumerRecord<String, LinkedHashMap> record : records) {
                        try {
                            BuildMessage buildMsg = mapper.convertValue(record.value(), BuildMessage.class);
                            messageProcessor.processMessage(buildMsg, this.topicName);
                        } catch (Exception e) {
                            LOGGER.error("Error processing message from topic {}: {}", this.topicName, e.getMessage(), e);
                        }
                    }
                }
            } catch (WakeupException e) {
                LOGGER.info("Consumer thread interrupted");
            } finally{
                kafkaConsumer.close();
                LOGGER.info("After closing KafkaConsumer");
            }
        }

        /**
         * Stops the consumer gracefully.
         */
        public void shutdown() {
            if (kafkaConsumer != null) {
                kafkaConsumer.wakeup();
            }
        }

        public KafkaConsumer<String, LinkedHashMap> getKafkaConsumer(){
            return this.kafkaConsumer;
        }

    }



}

