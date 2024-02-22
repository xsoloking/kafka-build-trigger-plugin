package io.jenkins.plugins.kafkabuildtrigger;

import hudson.Extension;
import hudson.util.Secret;
import jenkins.model.GlobalConfiguration;
import net.sf.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.StaplerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Extension
public class GlobalKafkaBuildTriggerConfig extends GlobalConfiguration {

    private static final String PLUGIN_NAME = "Kafka Build Trigger";
    /**
     * The string in global configuration that indicates content is empty.
     */
    public static final String CONTENT_NONE = "-";

    @SuppressWarnings("unused")
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalKafkaBuildTriggerConfig.class);

    private boolean enableConsumer;
    private String brokers;
    private String topic;
    private String username;

    private Secret password;
    private String groupId;


    @DataBoundConstructor
    public GlobalKafkaBuildTriggerConfig(boolean enableConsumer, String brokers,
                                         String topic, String groupId, String username, Secret password) {
        this.enableConsumer = enableConsumer;
        this.brokers = StringUtils.strip(StringUtils.stripToNull(brokers), "/");
        this.topic = topic;
        this.groupId = groupId;
        this.username = username;
        this.password = password;
    }

    /**
     * Create GlobalRabbitmqConfiguration from disk.
     */
    public GlobalKafkaBuildTriggerConfig() {
        load();
    }

    @Override
    public String getDisplayName() {
        return PLUGIN_NAME;
    }

    @Override
    public boolean configure(StaplerRequest req, JSONObject json) throws FormException {
        req.bindJSON(this, json);
        save();
        return true;
    }

    /**
     * Gets whether this plugin is enabled or not.
     *
     * @return true if this plugin is enabled.
     */
    public boolean isEnableConsumer() {
        return enableConsumer;
    }

    /**
     * Sets flag whether this plugin is enabled or not.
     *
     * @param enableConsumer true if this plugin is enabled.
     */
    public void setEnableConsumer(boolean enableConsumer) {
        this.enableConsumer = enableConsumer;
    }

    public String getBrokers() {
        return brokers;
    }

    public void setBrokers(final String serviceUri) {
        this.brokers = StringUtils.strip(StringUtils.stripToNull(serviceUri), "/");
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Secret getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = Secret.fromString(password);
    }

    public void setPassword(Secret password) {
        this.password = password;
    }


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }


    /**
     * Gets this extension's instance.
     *
     * @return the instance of this extension.
     */
    public static GlobalKafkaBuildTriggerConfig get() {
        return GlobalConfiguration.all().get(GlobalKafkaBuildTriggerConfig.class);
    }
}