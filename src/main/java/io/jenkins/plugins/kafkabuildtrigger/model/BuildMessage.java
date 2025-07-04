package io.jenkins.plugins.kafkabuildtrigger.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import net.sf.json.JSONArray;

import java.util.Objects;

/**
 * Represents a build message received from Kafka.
 * 
 * @author Optimized by AI Assistant
 */
public class BuildMessage {
    
    @JsonProperty("project")
    private String project;
    
    @JsonProperty("token")
    private String token;
    
    @JsonProperty("parameter")
    private JSONArray parameter;
    
    public BuildMessage() {
        // Default constructor for JSON deserialization
    }
    
    public BuildMessage(String project, String token, JSONArray parameter) {
        this.project = project;
        this.token = token;
        this.parameter = parameter;
    }
    
    public String getProject() {
        return project;
    }
    
    public void setProject(String project) {
        this.project = project;
    }
    
    public String getToken() {
        return token;
    }
    
    public void setToken(String token) {
        this.token = token;
    }
    
    public JSONArray getParameter() {
        return parameter != null ? parameter : new JSONArray();
    }
    
    public void setParameter(JSONArray parameter) {
        this.parameter = parameter;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BuildMessage that = (BuildMessage) o;
        return Objects.equals(project, that.project) &&
               Objects.equals(token, that.token) &&
               Objects.equals(parameter, that.parameter);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(project, token, parameter);
    }
    
    @Override
    public String toString() {
        return "BuildMessage{" +
               "project='" + project + '\'' +
               ", token='" + token + '\'' +
               ", parameter=" + parameter +
               '}';
    }
    
    /**
     * Validates if the build message has all required fields.
     *
     * @return true if the message is valid
     */
    @com.fasterxml.jackson.annotation.JsonIgnore
    public boolean isValid() {
        return project != null && !project.trim().isEmpty() &&
               token != null && !token.trim().isEmpty();
    }
}
