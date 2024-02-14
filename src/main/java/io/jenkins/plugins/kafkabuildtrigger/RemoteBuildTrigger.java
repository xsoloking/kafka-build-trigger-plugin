package io.jenkins.plugins.kafkabuildtrigger;

import groovy.util.logging.Slf4j;
import hudson.Extension;
import hudson.model.*;
import hudson.model.listeners.ItemListener;
import hudson.triggers.Trigger;
import hudson.triggers.TriggerDescriptor;
import jenkins.model.Jenkins;
import jenkins.model.ParameterizedJobMixIn;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.stapler.DataBoundConstructor;

import java.util.*;
import java.util.logging.Logger;

/**
 * The extension trigger builds by application message.
 *
 * @author Radhika Gupta
 */
public class RemoteBuildTrigger <T extends Job<?, ?> & ParameterizedJobMixIn.ParameterizedJob> extends Trigger<T> {

    public static final String PLUGIN_APPID = "remote-build";

    private static final String PLUGIN_NAME = "Kafka Build Trigger";

    private static final String KEY_PARAM_NAME = "name";
    private static final String KEY_PARAM_VALUE = "value";

    private String remoteBuildToken;

    /**
     * Creates instance with specified parameters.
     *
     * @param remoteBuildToken
     *            the token for remote build.
     */
    @DataBoundConstructor
    public RemoteBuildTrigger(String remoteBuildToken) {
        super();
        this.remoteBuildToken = StringUtils.stripToNull(remoteBuildToken);
    }

    @Override
    public void start(T project, boolean newInstance) {
        TriggerManager listener = TriggerManager.getInstance();

        if (listener != null) {
            listener.addTrigger(this);
        }
        removeDuplicatedTrigger(listener.getTriggers());
        super.start(project, newInstance);
    }

    @Override
    public void stop() {
        TriggerManager.getInstance().removeTrigger(this);
        super.stop();
    }

    /**
     * Remove the duplicated trigger from the triggers.
     *
     * @param triggers
     *          the set of current trigger instances which have already been loaded in the memory
     */
    public void removeDuplicatedTrigger(Set<RemoteBuildTrigger> triggers){
        Map<String,RemoteBuildTrigger>  tempHashMap= new HashMap<String,RemoteBuildTrigger>();
        for(RemoteBuildTrigger trigger:triggers){
            tempHashMap.put(trigger.getProjectName(), trigger);
        }
        triggers.clear();
        triggers.addAll(tempHashMap.values());
    }

    /**
     * Gets token.
     *
     * @return the token.
     */
    public String getRemoteBuildToken() {
        return remoteBuildToken;
    }

    /**
     * Sets token.
     *
     * @param remoteBuildToken the token.
     */
    public void setRemoteBuildToken(String remoteBuildToken) {
        this.remoteBuildToken = remoteBuildToken;
    }

    /**
     * Gets project name.
     *
     * @return the project name.
     */
    public String getProjectName() {
        if(job!=null){
            return job.getName();
        }
        return "";
    }

    /**
     * Schedules build for triggered job using application message.
     *
     * @param queueName
     *            the queue name.
     * @param jsonArray
     *            the content of application message.
     */
    public void scheduleBuild(String queueName, JSONArray jsonArray) {
        if (job != null) {
            if (jsonArray != null) {
                List<ParameterValue> parameters = getUpdatedParameters(jsonArray, getDefinitionParameters(job));
                ParameterizedJobMixIn.scheduleBuild2(job, 0, new CauseAction(new RemoteBuildCause(queueName)), new ParametersAction(parameters));
            } else {
                ParameterizedJobMixIn.scheduleBuild2(job, 0, new CauseAction(new RemoteBuildCause(queueName)));
            }
        }
    }

    /**
     * Gets updated parameters in job.
     *
     * @param jsonParameters
     *            the array of JSONObjects.
     * @param definedParameters
     *            the list of defined paramters.
     * @return the list of parameter values.
     */
    private List<ParameterValue> getUpdatedParameters(JSONArray jsonParameters, List<ParameterValue> definedParameters) {
        List<ParameterValue> newParams = new ArrayList<ParameterValue>();
        for (ParameterValue defParam : definedParameters) {

            for (int i = 0; i < jsonParameters.size(); i++) {
                JSONObject jsonParam = jsonParameters.getJSONObject(i);

                if (defParam.getName().toUpperCase().equals(jsonParam.getString(KEY_PARAM_NAME).toUpperCase())) {
                    newParams.add(new StringParameterValue(defParam.getName(), jsonParam.getString(KEY_PARAM_VALUE)));
                }
            }
        }
        return newParams;
    }

    /**
     * Gets definition parameters.
     *
     * @param project
     *            the project.
     * @return the list of parameter values.
     */
    private List<ParameterValue> getDefinitionParameters(Job<?, ?> project) {
        List<ParameterValue> parameters = new ArrayList<ParameterValue>();
        ParametersDefinitionProperty properties = project
                .getProperty(ParametersDefinitionProperty.class);

        if (properties != null) {
            for (ParameterDefinition paramDef : properties.getParameterDefinitions()) {
                ParameterValue param = paramDef.getDefaultParameterValue();
                if (param != null) {
                    parameters.add(param);
                }
            }
        }

        return parameters;
    }

    @Override
    public DescriptorImpl getDescriptor() {
        return (DescriptorImpl) super.getDescriptor();
    }

    /**
     * The descriptor for this trigger.
     *
     * @author rinrinne a.k.a. rin_ne
     */
    @Extension
    public static class DescriptorImpl extends TriggerDescriptor {

        @Override
        public boolean isApplicable(Item item) {
            return true;
        }

        @Override
        public String getDisplayName() {
            return PLUGIN_NAME;
        }

        /**
         * ItemListener implementation class.
         *
         * @author Radhika Gupta
         */
        @Extension
        public static class ItemListenerImpl extends ItemListener {

            @Override
            public void onLoaded() {
                TriggerManager triggerMgr = TriggerManager.getInstance();
                for (Project<?, ?> p : Jenkins.getInstance().getAllItems(Project.class)) {
                    RemoteBuildTrigger t = p.getTrigger(RemoteBuildTrigger.class);
                    if (t != null) {
                        triggerMgr.addTrigger(t);
                    }
                }
            }
        }
    }
}