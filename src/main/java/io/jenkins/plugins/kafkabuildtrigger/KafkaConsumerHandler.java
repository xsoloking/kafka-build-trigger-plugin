package io.jenkins.plugins.kafkabuildtrigger;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import net.sf.json.JSONArray;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
    private String topicName;
    private String groupId;
    private ConsumerThread consumerThread;

    public String getBrokers(){
        return brokers;
    }

    public String getTopicName() {
        return topicName;
    }

    public KafkaConsumerHandler(String brokers, String topicName, String groupId) {
        this.brokers = brokers;
        this.topicName = topicName;
        this.groupId = groupId;
        if (groupId == null || groupId.isEmpty()) {
            this.groupId = topicName+"_jenkins_consumer_"+System.currentTimeMillis();
        }
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
            consumerThread = new ConsumerThread(brokers, topicName, groupId);
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

    public void updateConf(String brokers, String topicName, String groupId) {
        this.brokers = brokers;
        this.topicName = topicName;
        if (groupId != null && ! groupId.isEmpty()) {
            this.groupId = groupId;
        }
    }


    private static class ConsumerThread extends Thread{

        private String broker;
        private String topicName;
        private String groupId;
        private KafkaConsumer<String,LinkedHashMap> kafkaConsumer;

        public ConsumerThread(String broker, String topicName, String groupId){
            this.broker = broker;
            this.topicName = topicName;
            this.groupId = groupId;
        }

        public void run() throws WakeupException {

            Properties configProperties = new Properties();
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

