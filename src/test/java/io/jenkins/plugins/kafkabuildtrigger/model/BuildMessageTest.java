package io.jenkins.plugins.kafkabuildtrigger.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for BuildMessage.
 * 
 * @author Optimized by AI Assistant
 */
public class BuildMessageTest {
    
    private ObjectMapper objectMapper;
    
    @Before
    public void setUp() {
        objectMapper = new ObjectMapper();
    }
    
    @Test
    public void testValidBuildMessage() {
        BuildMessage message = new BuildMessage();
        message.setProject("test-project");
        message.setToken("test-token");
        
        JSONArray params = new JSONArray();
        JSONObject param1 = new JSONObject();
        param1.put("name", "param1");
        param1.put("value", "value1");
        JSONObject param2 = new JSONObject();
        param2.put("name", "param2");
        param2.put("value", "value2");
        params.add(param1);
        params.add(param2);
        message.setParameter(params);
        
        assertTrue("Valid message should pass validation", message.isValid());
    }
    
    @Test
    public void testInvalidBuildMessageNullProject() {
        BuildMessage message = new BuildMessage();
        message.setProject(null);
        message.setToken("test-token");
        
        assertFalse("Message with null project should be invalid", message.isValid());
    }
    
    @Test
    public void testInvalidBuildMessageEmptyProject() {
        BuildMessage message = new BuildMessage();
        message.setProject("");
        message.setToken("test-token");
        
        assertFalse("Message with empty project should be invalid", message.isValid());
    }
    
    @Test
    public void testInvalidBuildMessageNullToken() {
        BuildMessage message = new BuildMessage();
        message.setProject("test-project");
        message.setToken(null);
        
        assertFalse("Message with null token should be invalid", message.isValid());
    }
    
    @Test
    public void testInvalidBuildMessageEmptyToken() {
        BuildMessage message = new BuildMessage();
        message.setProject("test-project");
        message.setToken("");
        
        assertFalse("Message with empty token should be invalid", message.isValid());
    }
    
    @Test
    public void testBuildMessageWithNullParameters() {
        BuildMessage message = new BuildMessage();
        message.setProject("test-project");
        message.setToken("test-token");
        message.setParameter(null);
        
        assertTrue("Message with null parameters should be valid", message.isValid());
        assertNotNull("Parameters should be initialized to empty map", message.getParameter());
        assertTrue("Parameters should be empty", message.getParameter().isEmpty());
    }
    
    @Test
    public void testBuildMessageSerialization() throws Exception {
        BuildMessage message = new BuildMessage();
        message.setProject("test-project");
        message.setToken("test-token");
        
        JSONArray params = new JSONArray();
        JSONObject param1 = new JSONObject();
        param1.put("name", "param1");
        param1.put("value", "value1");
        params.add(param1);
        message.setParameter(params);
        
        // Serialize to JSON
        String json = objectMapper.writeValueAsString(message);
        assertNotNull("JSON should not be null", json);
        assertTrue("JSON should contain project", json.contains("test-project"));
        assertTrue("JSON should contain token", json.contains("test-token"));
        assertTrue("JSON should contain parameter", json.contains("param1"));
        
        // Deserialize from JSON
        BuildMessage deserializedMessage = objectMapper.readValue(json, BuildMessage.class);
        assertEquals("Project should match", message.getProject(), deserializedMessage.getProject());
        assertEquals("Token should match", message.getToken(), deserializedMessage.getToken());
        assertEquals("Parameters should match", message.getParameter(), deserializedMessage.getParameter());
    }
    
    @Test
    public void testBuildMessageEqualsAndHashCode() {
        BuildMessage message1 = new BuildMessage();
        message1.setProject("test-project");
        message1.setToken("test-token");
        
        JSONArray params = new JSONArray();
        JSONObject param1 = new JSONObject();
        param1.put("name", "param1");
        param1.put("value", "value1");
        params.add(param1);
        message1.setParameter(params);

        BuildMessage message2 = new BuildMessage();
        message2.setProject("test-project");
        message2.setToken("test-token");
        JSONArray params2 = new JSONArray();
        JSONObject param1Copy = new JSONObject();
        param1Copy.put("name", "param1");
        param1Copy.put("value", "value1");
        params2.add(param1Copy);
        message2.setParameter(params2);
        
        assertEquals("Messages with same content should be equal", message1, message2);
        assertEquals("Hash codes should be equal", message1.hashCode(), message2.hashCode());
        
        // Test inequality
        message2.setProject("different-project");
        assertNotEquals("Messages with different content should not be equal", message1, message2);
    }
    
    @Test
    public void testBuildMessageToString() {
        BuildMessage message = new BuildMessage();
        message.setProject("test-project");
        message.setToken("test-token");
        
        JSONArray params = new JSONArray();
        JSONObject param1 = new JSONObject();
        param1.put("name", "param1");
        param1.put("value", "value1");
        params.add(param1);
        message.setParameter(params);
        
        String toString = message.toString();
        assertNotNull("toString should not be null", toString);
        assertTrue("toString should contain project", toString.contains("test-project"));
        assertTrue("toString should contain token", toString.contains("test-token"));
    }
    
    @Test
    public void testBuildMessageFromJson() throws Exception {
        String json = "{\"project\":\"test-project\",\"token\":\"test-token\",\"parameter\":[{\"name\":\"param1\",\"value\":\"value1\"}]}";

        BuildMessage message = objectMapper.readValue(json, BuildMessage.class);

        assertEquals("Project should be parsed correctly", "test-project", message.getProject());
        assertEquals("Token should be parsed correctly", "test-token", message.getToken());
        assertNotNull("Parameters should not be null", message.getParameter());
        assertEquals("Parameter array should have one element", 1, message.getParameter().size());
        JSONObject param = (JSONObject) message.getParameter().get(0);
        assertEquals("Parameter should be parsed correctly", "value1", param.get("value"));
        assertTrue("Parsed message should be valid", message.isValid());
    }
}
