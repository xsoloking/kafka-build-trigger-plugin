package io.jenkins.plugins.kafkabuildtrigger;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class TriggerManager {

    private static class InstanceHolder {
        private static final TriggerManager INSTANCE = new TriggerManager();
    }

    /**
     * Gets instance.
     *
     * @return the instance.
     */
    public static TriggerManager getInstance() {
        return InstanceHolder.INSTANCE;
    }


    private final Set<RemoteBuildTrigger> triggers = new CopyOnWriteArraySet<RemoteBuildTrigger>();


    /**
     * Get triggers.
     *
     *       the triggers.
     */
    public  Set<RemoteBuildTrigger> getTriggers(){
        return this.triggers;
    }

    /**
     * Adds trigger.
     *
     * @param trigger
     *            the trigger.
     */
    public void addTrigger(RemoteBuildTrigger trigger) {
        triggers.add(trigger);
    }

    /**
     * Removes trigger.
     *
     * @param trigger
     *            the trigger.
     */
    public void removeTrigger(RemoteBuildTrigger trigger) {
        triggers.remove(trigger);
    }

    /**
     * Creates instance.
     */
    private TriggerManager() {
    }


}
