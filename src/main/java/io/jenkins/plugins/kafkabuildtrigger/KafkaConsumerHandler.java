package io.jenkins.plugins.kafkabuildtrigger;

import com.fasterxml.jackson.databind.ObjectMapper;
import hudson.util.Secret;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import net.sf.json.JSONArray;
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

class BuildMessage {
    private String project;
    private String token;
    private JSONArray parameter;

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public JSONArray getParameter() {
        return parameter;
    }

    public void setParameter(JSONArray parameter) {
        this.parameter = parameter;
    }
}

public class KafkaConsumerHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerHandler.class);
    private String brokers;
    private String username;
    private String password;
    private String topicName;
    private String groupId;
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
        if (config.getGroupId() != null && ! config.getGroupId().isEmpty()) {
            this.groupId = config.getGroupId();
        }
        this.username = config.getUsername();
        this.password = Secret.toString(config.getPassword());
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


    private static class ConsumerThread extends Thread{

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
            configProperties.setProperty("security.protocol", "SASL_PLAINTEXT");
            configProperties.setProperty("sasl.mechanism", "SCRAM-SHA-512");
            configProperties.setProperty("sasl.jaas.config", jaasConfig);
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

            //Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, LinkedHashMap>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));

            ObjectMapper mapper = new ObjectMapper();
            //Start processing messages
            try {
                while (true) {
                    ConsumerRecords<String, LinkedHashMap> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, LinkedHashMap> record : records) {
                        BuildMessage buildMsg = mapper.convertValue(record.value(), BuildMessage.class);
                        handleBuildMessage(buildMsg);
                    }
                }
            } finally{
                kafkaConsumer.close();
                LOGGER.info("After closing KafkaConsumer");
            }
        }

        /**
         * Finds matched projects using given project name and token then schedule
         * build.
         */
        public void handleBuildMessage(BuildMessage buildMsg) {

            for (RemoteBuildTrigger t : TriggerManager.getInstance().getTriggers()) {

                if (t.getRemoteBuildToken() == null) {
                    LOGGER.warn("Ignoring kafka trigger for project {}: no token set", t.getProjectName());
                    continue;
                }

                if (t.getProjectName().equals(buildMsg.getProject())
                        && t.getRemoteBuildToken().equals(buildMsg.getToken())) {

                    t.scheduleBuild(this.topicName, buildMsg.getParameter());
                }
            }

        }

        public KafkaConsumer<String, LinkedHashMap> getKafkaConsumer(){
            return this.kafkaConsumer;
        }

    }



}

