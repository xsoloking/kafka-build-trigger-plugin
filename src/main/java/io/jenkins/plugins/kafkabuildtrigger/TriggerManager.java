package io.jenkins.plugins.kafkabuildtrigger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Manages RemoteBuildTrigger instances in a thread-safe manner.
 *
 * @author Optimized by AI Assistant
 */
public class TriggerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TriggerManager.class);

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

    private final Set<RemoteBuildTrigger> triggers = new CopyOnWriteArraySet<>();


    /**
     * Gets all registered triggers.
     *
     * @return immutable set of triggers
     */
    public Set<RemoteBuildTrigger> getTriggers() {
        return Set.copyOf(triggers);
    }

    /**
     * Adds a trigger to the manager.
     *
     * @param trigger the trigger to add
     * @return true if the trigger was added, false if it already existed
     */
    public boolean addTrigger(RemoteBuildTrigger trigger) {
        if (trigger == null) {
            LOGGER.warn("Attempted to add null trigger");
            return false;
        }

        boolean added = triggers.add(trigger);
        if (added) {
            LOGGER.info("Added trigger for project: {}", trigger.getProjectName());
        } else {
            LOGGER.debug("Trigger for project {} already exists", trigger.getProjectName());
        }
        return added;
    }

    /**
     * Removes a trigger from the manager.
     *
     * @param trigger the trigger to remove
     * @return true if the trigger was removed, false if it didn't exist
     */
    public boolean removeTrigger(RemoteBuildTrigger trigger) {
        if (trigger == null) {
            LOGGER.warn("Attempted to remove null trigger");
            return false;
        }

        boolean removed = triggers.remove(trigger);
        if (removed) {
            LOGGER.info("Removed trigger for project: {}", trigger.getProjectName());
        } else {
            LOGGER.debug("Trigger for project {} was not found", trigger.getProjectName());
        }
        return removed;
    }

    /**
     * Gets the number of registered triggers.
     *
     * @return trigger count
     */
    public int getTriggerCount() {
        return triggers.size();
    }

    /**
     * Clears all triggers.
     */
    public void clearTriggers() {
        int count = triggers.size();
        triggers.clear();
        LOGGER.info("Cleared {} triggers", count);
    }

    /**
     * Creates instance.
     */
    private TriggerManager() {
    }


}
